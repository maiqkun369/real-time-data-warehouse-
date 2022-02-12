package com.mqk.gmall.app.dwm.analysis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.bean.OrderWide;
import com.mqk.gmall.bean.PaymentInfo;
import com.mqk.gmall.bean.PaymentWide;
import com.mqk.gmall.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {
	public static void main(String[] args) throws Exception {
		// TODO: 2022/1/18 执行环境
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();
		String groupId = "payment_wide_group";
		String paymentInfoSourceTopic = "dwd_payment_info";
		String orderWideSourceTopic = "dwm_order_wide";
		String paymentWideSinkTopic = "dwm_payment_wide";


		// TODO: 2022/1/19  读取kafka创建流，转javabean,提取watermark
		final SingleOutputStreamOperator<OrderWide> orderWideDS = environment
				.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
				.map(line -> JSON.parseObject(line, OrderWide.class))
				.assignTimestampsAndWatermarks(
						WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
								.withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
									@Override
									public long extractTimestamp(OrderWide orderWide, long l) {
										final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
										try {
											return sdf.parse(orderWide.getCreate_time()).getTime();
										} catch (ParseException e) {
											e.printStackTrace();
											return l;
										}
									}
								})
				);

		final SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = environment
				.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
				.map(line -> JSON.parseObject(line, PaymentInfo.class))
				.assignTimestampsAndWatermarks(
						WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
								.withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
									@Override
									public long extractTimestamp(PaymentInfo paymentInfo, long l) {
										final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
										try {
											return sdf.parse(paymentInfo.getCreate_time()).getTime();
										} catch (ParseException e) {
											e.printStackTrace();
											return l;
										}
									}
								})
				);

		// TODO: 2022/1/19 双流join
		final SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS
				.keyBy(PaymentInfo::getOrder_id)
				.intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
				//支付是主流，支付事件先到所以要往回15分钟left join 订单数据
				//如果订单是主流，表示该时刻有一订单,因此要根据此时刻向前15分钟left join支付事件
				.between(Time.minutes(-15), Time.seconds(5))
				.process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
					@Override
					public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
						collector.collect(new PaymentWide(paymentInfo, orderWide));
					}
				});

		// TODO: 2022/1/19  数据写入kafka

		paymentWideDS.print("paymentWideDS");
		paymentWideDS
				.map(JSONObject::toJSONString)
				.addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));
		environment.execute("PaymentWideApp");
	}
}
