package com.mqk.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.app.dwm.selfprocessFunction.DimAsyncFunction;
import com.mqk.gmall.bean.OrderWide;
import com.mqk.gmall.bean.PaymentWide;
import com.mqk.gmall.bean.ProductStats;
import com.mqk.gmall.common.GmallConstant;
import com.mqk.gmall.utils.ClickHouseUtil;
import com.mqk.gmall.utils.DateTimeUtil;
import com.mqk.gmall.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 数据流：
 * app/web -> nginx -> springboot -> kafka(ods) -flinkApp -> kafka(dwd) -> FlinkApp -> clickHouse
 * app/web -> nginx -> springboot -> mysql -> flinkApp -> kafka (ods) -> FlinkApp -> kafka/phoenix ->
 *      flinkApp -> kafka(dwm) -> flinkApp -> clickhouse
 */
public class ProductStatsApp {
	public static void main(String[] args) throws Exception {

		// TODO: 2022/1/20 1.获取执行环境
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();


		// TODO: 2022/1/20 2.创建七个kafka主题的数据流
		String groupId = "product_stats_app";
		String pageViewSourceTopic = "dwd_page_log";
		String orderWideSourceTopic = "dwm_order_wide";
		String paymentWideSourceTopic = "dwm_payment_wide";
		String cartInfoSourceTopic = "dwd_cart_info";
		String favorInfoSourceTopic = "dwd_favor_info";
		String refundInfoSourceTopic = "dwd_order_refund_info";
		String commentInfoSourceTopic = "dwd_comment_info";
		final DataStreamSource<String> pvDS  = environment.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
		final DataStreamSource<String> favoDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
		final DataStreamSource<String> cartDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
		final DataStreamSource<String> orderDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
		final DataStreamSource<String> payDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
		final DataStreamSource<String> refundDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
		final DataStreamSource<String> commentDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));


		// TODO: 2022/1/20 3.将七个流统一数据格式
		final SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pvDS.flatMap(new FlatMapFunction<String, ProductStats>() {
			@Override
			public void flatMap(String value, Collector<ProductStats> collector) throws Exception {
				final JSONObject jsonObject = JSON.parseObject(value);

				//取出Page
				final JSONObject page = jsonObject.getJSONObject("page");
				final String pageId = page.getString("page_id");
				final Long ts = jsonObject.getLong("ts");

				if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
					collector.collect(
							ProductStats
									.builder()
									.sku_id(page.getLong("item"))
									.click_ct(1L)
									.ts(ts)
									.build());
				}
				//尝试取出曝光数据
				final JSONArray displays = jsonObject.getJSONArray("displays");
				if (null != displays && displays.size() > 0) {
					for (int i = 0; i < displays.size(); i++) {
						//取出曝光数据
						final JSONObject display = displays.getJSONObject(i);
						if ("sku_id".equals(display.getString("item_type"))) {
							collector.collect(
									ProductStats.builder()
											.sku_id(display.getLong("item"))
											.display_ct(1L)
											.ts(ts)
											.build());
						}
					}
				}
			}
		});

		final SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favoDS.map(
				line -> {
					final JSONObject jsonObject = JSON.parseObject(line);


					return ProductStats.builder()
							.sku_id(jsonObject.getLong("sku_id"))
							.favor_ct(1L)
							.ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
							.build();
				}
		);

		final SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(
				line -> {
					final JSONObject jsonObject = JSON.parseObject(line);


					return ProductStats.builder()
							.sku_id(jsonObject.getLong("sku_id"))
							.cart_ct(1L)
							.ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
							.build();
				}

		);

		final SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(
				line -> {
					final OrderWide orderWide = JSON.parseObject(line, OrderWide.class);
					Set<Long> orderIds = new HashSet();
					orderIds.add(orderWide.getOrder_id());

					return ProductStats.builder()
							.sku_id(orderWide.getSku_id())
							.order_sku_num(orderWide.getSku_num())
							.order_amount(orderWide.getOrder_price())
							.orderIdSet(orderIds)
							.ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
							.build();
				}
		);

		final SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = payDS.map(
				line -> {
					final PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);
					Set<Long> orderIds = new HashSet();
					orderIds.add(paymentWide.getOrder_id());

					return ProductStats.builder()
							.sku_id(paymentWide.getSku_id())
							.payment_amount(paymentWide.getOrder_price())
							.ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
							.paidOrderIdSet(orderIds)
							.build();
				}
		);


		final SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(
				line -> {
					final JSONObject jsonObject = JSON.parseObject(line);
					Set<Long> orderIds = new HashSet();
					orderIds.add(jsonObject.getLong("order_id"));
					return ProductStats.builder()
							.sku_id(jsonObject.getLong("sku_id"))
							.refund_amount(jsonObject.getBigDecimal("refund_amount'"))
							.refundOrderIdSet(orderIds)
							.ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
							.build();
				}
		);


		final SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(
				line -> {
					final JSONObject jsonObject = JSON.parseObject(line);

					final String appraise = jsonObject.getString("appraise");
					long goodCt = 0L;
					if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
						goodCt = 1L;
					}
					return ProductStats.builder()
							.sku_id(jsonObject.getLong("sku_id"))
							.comment_ct(1L)
							.good_comment_ct(goodCt)
							.ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
							.build();
				}
		);


		// TODO: 2022/1/20 4.union 7个流
		final DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
				productStatsWithFavorDS,
				productStatsWithCartDS,
				productStatsWithOrderDS,
				productStatsWithPaymentDS,
				productStatsWithRefundDS,
				productStatsWithCommentDS);


		// TODO: 2022/1/20 5.提取时间戳生成wm
		final SingleOutputStreamOperator<ProductStats> productStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(
				WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
						.withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
							@Override
							public long extractTimestamp(ProductStats productStats, long l) {
								return productStats.getTs();
							}
						})
		);


		// TODO: 2022/1/20 6.分组、开窗、聚合
		final SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWmDS
				.keyBy(ProductStats::getSku_id)
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.reduce(
						new ReduceFunction<ProductStats>() {
							@Override
							public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
								stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
								stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
								stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
								stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());

								stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
								stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
//								stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
								stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

								stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));
								stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
//								stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);

								stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));
								stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
//								stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
								stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
								stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
								return stats1;
							}
						},
						new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
							@Override
							public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {
								//取出数据
								final ProductStats productStats = iterable.iterator().next();

								//设置窗口时间
								productStats.setStt(DateTimeUtil.toYMDhms(new Date(timeWindow.getStart())));
								productStats.setEdt(DateTimeUtil.toYMDhms(new Date(timeWindow.getEnd())));

								//设置订单数量
								productStats.setOrder_ct((long) productStats.getPaidOrderIdSet().size());
								productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
								productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

								collector.collect(productStats);
							}
						}
				);

		// TODO: 2022/1/20 7.关联维度信息
		// TODO: 2022/1/20 7.1 关联SKU信息
		final SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
				reduceDS,
				new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
					@Override
					public String getKey(ProductStats productStats) {
						return productStats.getSku_id().toString();
					}

					@Override
					public void join(ProductStats productStats, JSONObject jsonObject) {
						productStats.setSku_name(jsonObject.getString("SKU_NAME"));
						productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
						productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
						productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
						productStats.setTm_id(jsonObject.getLong("TM_ID"));

					}
				},
				60, TimeUnit.SECONDS);


		// TODO: 2022/1/20 7.2 关联SPU信息
		SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
				AsyncDataStream.unorderedWait(productStatsWithSkuDS,
						new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
							@Override
							public void join(ProductStats productStats, JSONObject jsonObject){
								productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
							}
							@Override
							public String getKey(ProductStats productStats) {
								return String.valueOf(productStats.getSpu_id());
							}
						}, 60, TimeUnit.SECONDS);

		// TODO: 2022/1/20 7.3 关联category信息
		SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
				AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
						new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
							@Override
							public void join(ProductStats productStats, JSONObject jsonObject) {
								productStats.setCategory3_name(jsonObject.getString("NAME"));
							}
							@Override
							public String getKey(ProductStats productStats) {
								return String.valueOf(productStats.getCategory3_id());
							}
						}, 60, TimeUnit.SECONDS);

		// TODO: 2022/1/20 7.4 关联TM信息
		SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
				AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
						new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
							@Override
							public void join(ProductStats productStats, JSONObject jsonObject){
								productStats.setTm_name(jsonObject.getString("TM_NAME"));
							}
							@Override
							public String getKey(ProductStats productStats) {
								return String.valueOf(productStats.getTm_id());
							}
						}, 60, TimeUnit.SECONDS);


		// TODO: 2022/1/20 8.数据写入clickhouse
		productStatsWithTmDstream.addSink(
				ClickHouseUtil
				.<ProductStats>getSink("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

		environment.execute("ProductStatsApp");
	}
}
