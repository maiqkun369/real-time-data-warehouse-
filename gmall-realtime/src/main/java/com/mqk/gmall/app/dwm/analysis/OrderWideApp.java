package com.mqk.gmall.app.dwm.analysis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.app.dwm.selfprocessFunction.DimAsyncFunction;
import com.mqk.gmall.app.dwm.selfprocessFunction.MyProcessJoinFunctionn;
import com.mqk.gmall.bean.OrderDetail;
import com.mqk.gmall.bean.OrderInfo;
import com.mqk.gmall.bean.OrderWide;
import com.mqk.gmall.utils.MyKafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * 数据流：web/app -> nginx -> springboot -> mysql -> flinkApp -> kafka(ods)
 *              -> flinkApp -> kafka/phoenix(dwd-dim) -> flinkApp9(redis) -> kafka(dwm)
 */
public class OrderWideApp {
	public static void main(String[] args) throws Exception {
		// TODO: 2022/1/18 执行环境
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();

		// TODO: 2022/1/18 读取kafka数据
		String orderInfoSourceTopic = "dwd_order_info";
		String orderDetailSourceTopic = "dwd_order_detail";
		String orderWideSinkTopic = "dwm_order_wide";
		String groupId = "order_wide_group_new";

		final SingleOutputStreamOperator<OrderInfo> orderInfoDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
				.map(
						line -> {
							final OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
							final String create_time = orderInfo.getCreate_time();
							final String[] dateTimeArr = create_time.split(" ");
							orderInfo.setCreate_date(dateTimeArr[0]);
							orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
							final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							orderInfo.setCreate_ts(format.parse(create_time).getTime());
							return orderInfo;
						}
				).assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<OrderInfo>forMonotonousTimestamps()
								.withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
									@Override
									public long extractTimestamp(OrderInfo orderInfo, long l) {
										return orderInfo.getCreate_ts();
									}
								})
				);


		final SingleOutputStreamOperator<OrderDetail> orderDetailDS = environment
				.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
				.map(
						line -> {
							final OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
							final String create_time = orderDetail.getCreate_time();
							final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							orderDetail.setCreate_ts(format.parse(create_time).getTime());
							return orderDetail;
						}
				).assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<OrderDetail>forMonotonousTimestamps()
								.withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
									@Override
									public long extractTimestamp(OrderDetail orderDet, long l) {
										return orderDet.getCreate_ts();
									}
								})
				);


		// TODO: 2022/1/18 双流join intervalJoin相当于主流left join 辅流

		final SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDS = orderInfoDS
				.keyBy(OrderInfo::getId)
				.intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
				.between(Time.seconds(-5), Time.seconds(5)) //生产环境中为最大延迟时间
				.process(new MyProcessJoinFunctionn());


		// TODO: 2022/1/18 打印测试
		orderWideWithNoDimDS.print("orderWideWithNoDimDS");


		// TODO: 2022/1/18 关联维度信息 多个AsyncDataStream同时执行

		// TODO: 2022/1/19 异步方式关联用户维度
		final SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
				orderWideWithNoDimDS,
				new DimAsyncFunction<OrderWide>("DIM_USER_INFO"){
					@Override
					public String getKey(OrderWide input) {
						return input.getUser_id().toString();
					}

					@SneakyThrows
					@Override
					public void join(OrderWide orderWide, JSONObject dimInfo) {
						final String gender = dimInfo.getString("GENDER");
						orderWide.setUser_gender(gender);
						final String birthday = dimInfo.getString("BIRTHDAY");
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

						final long currentTs = System.currentTimeMillis();
						final long time = sdf.parse(birthday).getTime();
						Long age = (currentTs - time) / (1000 * 60 * 60 * 24 * 365L);
						orderWide.setUser_age(age.intValue());
					}
				},
				60,
				TimeUnit.SECONDS
		);
		// TODO: 2022/1/19 关联省份信息
		final SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
				orderWideWithUserDS, //一层一层的关联
				new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
					@Override
					public String getKey(OrderWide orderWide) {
						return orderWide.getProvince_id().toString();
					}

					@Override
					public void join(OrderWide orderWide, JSONObject dimInfo) {
						orderWide.setProvince_name(dimInfo.getString("NAME"));
						orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
						orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
						orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
					}
				},
				60, TimeUnit.SECONDS);


		// TODO: 2022/1/19 异步方式关联SKU的维度
		SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS =
				AsyncDataStream.unorderedWait(
						orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
							@Override
							public void join(OrderWide orderWide, JSONObject jsonObject) {
								orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
								orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
								orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
								orderWide.setTm_id(jsonObject.getLong("TM_ID"));
							}
							@Override
							public String getKey(OrderWide orderWide) {
								return String.valueOf(orderWide.getSku_id());
							}
						}, 60, TimeUnit.SECONDS);


		// TODO: 2022/1/19 关联SPU维度
		SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS =
				AsyncDataStream.unorderedWait(
						orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
							@Override
							public void join(OrderWide orderWide, JSONObject jsonObject) {
								orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
							}
							@Override
							public String getKey(OrderWide orderWide) {
								return String.valueOf(orderWide.getSpu_id());
							}
						}, 60, TimeUnit.SECONDS);


		// TODO: 2022/1/19 关联TM维度
		SingleOutputStreamOperator<OrderWide> orderWideWithTmDS =
				AsyncDataStream.unorderedWait(
						orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK")
						{
							@Override
							public void join(OrderWide orderWide, JSONObject jsonObject) {
								orderWide.setTm_name(jsonObject.getString("TM_NAME"));
							}
							@Override
							public String getKey(OrderWide orderWide) {
								return String.valueOf(orderWide.getTm_id());
							}
						}, 60, TimeUnit.SECONDS);

		// TODO: 2022/1/19 关联category维度
		SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS =
				AsyncDataStream.unorderedWait(
						orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3")
						{
							@Override
							public void join(OrderWide orderWide, JSONObject jsonObject) {
								orderWide.setCategory3_name(jsonObject.getString("NAME"));
							}
							@Override
							public String getKey(OrderWide orderWide) {
								return String.valueOf(orderWide.getCategory3_id());
							}
						}, 60, TimeUnit.SECONDS);


		//全部维度关联后的结果
		orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>");



		// TODO: 2022/1/18 写回kafka

		orderWideWithCategory3DS
				.map(data -> JSONObject.toJSONString(data))
				.addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));



		environment.execute("OrderWideApp");
	}
}
