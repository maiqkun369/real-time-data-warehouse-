package com.mqk.gmall.app.dwm.analysis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();
		// TODO: 2022/1/17  读取kafka数据创建主流
		String sourceTopic = "dwd_page_log";
		String groupId = "userJumpDetailApp";
		String sinkTopic = "dwm_user_jump_detail";
		final DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
		// TODO: 2022/1/17  将每行数据转换为json对象并提取时间戳生成watermark CEP的within是可以利用事件时间的
		final SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS
				.map(JSON::parseObject)
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
								.withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
									@Override
									public long extractTimestamp(JSONObject jsonObject, long l) {
										return jsonObject.getLong("ts");
									}
								})
				);
		// TODO: 2022/1/17 定义模式序列
//		final Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//			@Override
//			public boolean filter(JSONObject value) throws Exception {
//				final String lastPageId = value.getJSONObject("page").getString("last_page_id");
//				return lastPageId == null || lastPageId.length() <= 0;
//			}
//		}).next("next").where(new SimpleCondition<JSONObject>() {
//			@Override
//			public boolean filter(JSONObject value) throws Exception {
//				final String lastPageId = value.getJSONObject("page").getString("last_page_id");
//				return lastPageId == null || lastPageId.length() <= 0;
//			}
//		}).within(Time.seconds(10));

		//使用循环模式定义模式序列
		final Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
			@Override
			public boolean filter(JSONObject value) throws Exception {
				final String lastPageId = value.getJSONObject("page").getString("last_page_id");
				return lastPageId == null || lastPageId.length() <= 0;
			}
		}).times(2).consecutive().within(Time.seconds(10));


		// TODO: 2022/1/17 应用到流上
		final PatternStream<JSONObject> patternStream =
				CEP.pattern(jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid")), pattern);

		// TODO: 2022/1/17 提取匹配上的和超时事件
		final OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeOut") {
		};

		//匹配上的
		final SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(
				timeoutTag,
				new PatternTimeoutFunction<JSONObject, JSONObject>() {
					@Override
					public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
						return map.get("start").get(0);
					}
				},
				new PatternSelectFunction<JSONObject, JSONObject>() {
					@Override
					public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
						return map.get("start").get(0);
					}
				}
		);
		//超时事件
		final DataStream<JSONObject> timeoutDS = selectDS.getSideOutput(timeoutTag);

		// TODO: 2022/1/17 union两种事件
		final DataStream<JSONObject> union = selectDS.union(timeoutDS);

		// TODO: 2022/1/17 写入kafka
		union.print();
		union
				.map(JSONAware::toJSONString)
				.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

		environment.execute("UserJumpDetailApp");

	}
}
