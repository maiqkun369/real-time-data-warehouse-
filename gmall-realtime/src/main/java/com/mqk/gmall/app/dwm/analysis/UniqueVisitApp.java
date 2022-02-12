package com.mqk.gmall.app.dwm.analysis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.app.dwm.selfprocessFunction.MidFilter;
import com.mqk.gmall.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 *   数据流：web/app -> Nginx -> Springboot -> kafka(ods) -> FlinkApp -> kafka(dwd) -> FlinkApp -> kafka(dwm)
 *          mockLog -> Nginx -> Logger.sh -> kafka(zk) -> BaseLogApp -> kafka -> UniqueVisitApp -> kafka
 */
public class UniqueVisitApp {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();
		// TODO: 2022/1/17 读取 dwd_page_log主题的数据
		final DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getKafkaConsumer("dwd_page_log", "unique_visit_app"));

		// TODO: 2022/1/17 将每行数据转化为json对象
		final SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

		// TODO: 2022/1/17 过滤数据 保留每个mid每天第一次登录的数据
		final KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(
				jsonObject -> {
					return jsonObject.getJSONObject("common").getString("mid");
				}
		);
		final SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new MidFilter());


		// TODO: 2022/1/17 将数据写入kafka
		uvDS
				.map(JSONAware::toJSONString)
				.addSink(MyKafkaUtil.getKafkaProducer("dwm_unique_visit"));
		uvDS.print("res");

		environment.execute("UniqueVisitApp");
	}
}
