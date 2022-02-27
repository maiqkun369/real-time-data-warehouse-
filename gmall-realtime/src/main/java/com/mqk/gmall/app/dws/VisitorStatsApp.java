package com.mqk.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.bean.VisitorStats;
import com.mqk.gmall.utils.ClickHouseUtil;
import com.mqk.gmall.utils.DateTimeUtil;
import com.mqk.gmall.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * 数据流：web/app -> Nginx -> Springboot -> kafka(ods) -> FlinkApp -> kafka(dwd) -> flinkApp -> kafka(dwm) -> flinkApp -> clickHourse
 *        mockLog -> Nginx -> Logger.sh -> kafka(zk) -> baseLogApp -> kafka
 */
public class VisitorStatsApp {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();

		// TODO: 2022/1/19 读取数据创建流
		String groupId = "visitor_stats_app";
		String pageViewSourceTopic = "dwd_page_log";
		String uniqueVisitSourceTopic = "dwm_unique_visit";
		String userJumpDetailSourceTopic = "dwm_user_jump_detail";

		final DataStreamSource<String> uvDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
		final DataStreamSource<String> ujDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
		final DataStreamSource<String> pvDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

		// TODO: 2022/1/19 将每个流处理成相同的数据类型

		// 处理uv数据
		final SingleOutputStreamOperator<VisitorStats> visitorStateWithUvDS = uvDS.map(
				line -> {
					final JSONObject jsonObject = JSON.parseObject(line);
					final JSONObject common = jsonObject.getJSONObject("common");

					return new VisitorStats("", "",
							common.getString("vc"),
							common.getString("ch"),
							common.getString("ar"),
							common.getString("is_new"),
							1L, 0L, 0L, 0L, 0L,
							jsonObject.getLong("ts"));
				}
		);

		//处理uj数据
		final SingleOutputStreamOperator<VisitorStats> visitorStateWithUjDS = ujDS.map(
				line -> {
					final JSONObject jsonObject = JSON.parseObject(line);
					final JSONObject common = jsonObject.getJSONObject("common");

					return new VisitorStats("", "",
							common.getString("vc"),
							common.getString("ch"),
							common.getString("ar"),
							common.getString("is_new"),
							0L, 0L, 0L, 1L, 0L,
							jsonObject.getLong("ts"));
				}
		);

		//处理pv数据
		final SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDs = pvDS.map(
				line -> {
					final JSONObject jsonObject = JSON.parseObject(line);
					//公共字段
					final JSONObject common = jsonObject.getJSONObject("common");
					final JSONObject page = jsonObject.getJSONObject("page");
					final String lastPageId = page.getString("last_page_id");

					long sv = 0L;
					if (null == lastPageId || lastPageId.length() <= 0) {
						sv = 1L;
					}
					return new VisitorStats("", "",
							common.getString("vc"),
							common.getString("ch"),
							common.getString("ar"),
							common.getString("is_new"),
							0L, 1L, sv, 0L, page.getLong("during_time"),
							jsonObject.getLong("ts"));
				}
		);

		// TODO: 2022/1/19 Union几个流
		final DataStream<VisitorStats> unionDS = visitorStateWithUvDS.union(visitorStateWithUjDS, visitorStatsWithPvDs);

		// TODO: 2022/1/19 提取时间戳生成wk
		final SingleOutputStreamOperator<VisitorStats> visitorStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(
				WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
						.withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
							@Override
							public long extractTimestamp(VisitorStats visitorStats, long l) {
								return visitorStats.getTs();
							}
						})
		);

		// TODO: 2022/1/19 按照维度信息分组
		final KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
			@Override
			public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
				return new Tuple4<>(
						visitorStats.getAr(),
						visitorStats.getCh(),
						visitorStats.getIs_new(),
						visitorStats.getVc()
				);
			}
		});

		// TODO: 2022/1/19 开窗聚合 10s的滚动窗口
		final SingleOutputStreamOperator<VisitorStats> res = keyedStream
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.reduce(
						/**
						 * 增量聚合, 窗口内的数据一条一条的进行计算
						 */
						new ReduceFunction<VisitorStats>() {
							@Override
							public VisitorStats reduce(VisitorStats visitorStats1, VisitorStats visitorStats2) throws Exception {
								visitorStats1.setUv_ct(visitorStats1.getUv_ct() + visitorStats2.getUv_ct());
								visitorStats1.setPv_ct(visitorStats1.getPv_ct() + visitorStats2.getPv_ct());
								visitorStats1.setSv_ct(visitorStats1.getSv_ct() + visitorStats2.getSv_ct());
								visitorStats1.setUj_ct(visitorStats1.getUj_ct() + visitorStats2.getUj_ct());
								visitorStats1.setDur_sum(visitorStats1.getDur_sum() + visitorStats2.getDur_sum());
								return visitorStats1;
							}
						},
						/**
						 * 全窗口聚合，对整个窗口内的数据进行计算，例如TOPN等
						 */
						new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
							@Override
							public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<VisitorStats> input, Collector<VisitorStats> collector) throws Exception {
								final long start = timeWindow.getStart();
								final long end = timeWindow.getEnd();

								final VisitorStats visitorStats = input.iterator().next();

								//补充窗口信息
								visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
								visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

								collector.collect(visitorStats);

							}
						}
				);
		res.print(">>>>>>>>>>>>>>>>>>>");

		// TODO: 2022/1/19 写入Clickhouse
		res.addSink(ClickHouseUtil.
				getSink("insert into visitor_stats values (?,?,?,?,?,?,?,?,?,?,?,?)"));






		environment.execute("VisitorStatsApp");
	}
}
