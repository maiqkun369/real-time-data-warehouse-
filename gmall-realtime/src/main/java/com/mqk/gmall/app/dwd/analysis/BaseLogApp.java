package com.mqk.gmall.app.dwd.analysis;

import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.common.BaseLogAppConstant;
import com.mqk.gmall.app.dwd.selfprocessFunction.process.JsonElemDs2NewFlagDs;
import com.mqk.gmall.app.dwd.selfprocessFunction.process.KafkaDsElem2JsonDsElem;
import com.mqk.gmall.app.dwd.selfprocessFunction.process.NewFlagDs2PageDs;
import com.mqk.gmall.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据流：web/app -> Nginx -> Springboot -> kafka(ods) -> FlinkApp -> kafka(dwd)
 *        mockLog -> Nginx -> Logger.sh -> kafka(zk) -> baseLogApp -> kafka
 */
public class BaseLogApp {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();

		// TODO: 2022/1/13  消费ods_base_log主题数据创建流
		final DataStreamSource<String> kafkaDs =
				environment.addSource(MyKafkaUtil.getKafkaConsumer("ods_base_log", "base_log_app"));

		// TODO: 2022/1/13  将每行数据转换为json对象
		final SingleOutputStreamOperator<JSONObject> jsonObjDs =
				kafkaDs.process(new KafkaDsElem2JsonDsElem());

		// TODO: 2022/1/13  新老用户校验，状态编程
		final SingleOutputStreamOperator<JSONObject> newFlagDs = jsonObjDs.keyBy(
				jsonObject -> jsonObject.getJSONObject("common").getString("mid")
		).map(new JsonElemDs2NewFlagDs());

		// TODO: 2022/1/13  测输出流，页面：主流，启动：测输出流，曝光：测输出流
		final SingleOutputStreamOperator<String> pageDs =
				newFlagDs.process(new NewFlagDs2PageDs());

		// TODO: 2022/1/13  提取测输出流
		final DataStream<String> startDs = pageDs.getSideOutput(BaseLogAppConstant.startOutput);
		final DataStream<String> displayDs = pageDs.getSideOutput(BaseLogAppConstant.displayOutput);

		// TODO: 2022/1/13  将三个流进行打印并输出到kafka中
		startDs.print("Start >>>>>>>>>>>>>>>");
		pageDs.print("Page >>>>>>>>>>>>>>>");
		displayDs.print("Display >>>>>>>>>>>>>>>");
		//打印脏数据
		jsonObjDs.getSideOutput(BaseLogAppConstant.dirtyOutput).print("Dirty >>>>>>>>>>>>>>>");

		// TODO: 2022/1/13 sink to kafka
		startDs.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
		pageDs.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
		displayDs.addSink( MyKafkaUtil.getKafkaProducer("dwd_display_log"));

		//启动任务
		environment.execute("BaseLogApp");
	}
}
