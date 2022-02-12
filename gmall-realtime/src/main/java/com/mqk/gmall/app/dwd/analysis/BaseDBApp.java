package com.mqk.gmall.app.dwd.analysis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.common.BaseLogAppConstant;
import com.mqk.gmall.app.dwd.selfprocessFunction.process.MyBroadcastProcess;
import com.mqk.gmall.app.dwd.selfprocessFunction.sink.DimSinkFunction;
import com.mqk.gmall.app.ods.function.FlinkCDC_DeserializationSchema;
import com.mqk.gmall.bean.TableProcess;
import com.mqk.gmall.utils.MyKafkaUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * ODS：原始数据，日志和业务数据
 * DWD：根据数据对象为单位进行分流，比如订单、页面访问等等
 * DIM：维度数据
 * DWM：对于部分数据对象进行进一步加工，比如独立访问、跳出行为，也可以和维度进行关联，形成宽表，依旧是明细数据。
 * DWS：根据某个主题将多个事实数据轻度聚合，形成主题宽表。
 * ADS：把ClickHouse中的数据根据可视化需进行筛选聚合
 */
public class BaseDBApp {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();

		// TODO: 2022/1/13 消费kafka ods_base_db主题数据创建流
		final DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getKafkaConsumer("ods_base_db", "base_db_app"));

		// TODO: 2022/1/13 将每行数据转换为JSON对象并过滤(delete)  -> 主流
		final SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS
				.map(JSON::parseObject)
				.filter(
						value -> {
							final String type = value.getString("type");
							return !"delete".equals(type);
						}
				);

		// TODO: 2022/1/13 使用cdc消费配置表 -> 广播流
		final DebeziumSourceFunction<String> sourceFunction = MySQLSource
				.<String>builder()
				.hostname("hadoop102")
				.port(3306)
				.username("root")
				.password("000000")
				.databaseList("gmall-realtime") //监听的数据库
				.tableList("gmall-realtime.table_process")
				.startupOptions(StartupOptions.initial())
				.deserializer(new FlinkCDC_DeserializationSchema())// 自定义反序列化器
				.build();



		final DataStreamSource<String> tableProcessDs = environment.addSource(sourceFunction);
		//形成广播流
		final MapStateDescriptor<String, TableProcess> mapStateDes =
				new MapStateDescriptor<>("map-state", String.class, TableProcess.class);

		final BroadcastStream<String> broadcastStream = tableProcessDs.broadcast(mapStateDes);
		// TODO: 2022/1/13 连接主流和广播流
		final BroadcastConnectedStream<JSONObject, String> connectStream = jsonObjDS.connect(broadcastStream);

		// TODO: 2022/1/13 处理数据
		final SingleOutputStreamOperator<JSONObject> kafka =
				connectStream.process(new MyBroadcastProcess(BaseLogAppConstant.hbaseOutput, mapStateDes));


		// TODO: 2022/1/13 提取kafka流数据和Hbase流数据
		final DataStream<JSONObject> hbase = kafka.getSideOutput(BaseLogAppConstant.hbaseOutput);


		// TODO: 2022/1/13 将kafka数据写入kafka主题，将hbase数据写入Phoenix表
		kafka.print("Kafka >>>>>>>>>>>>>>");
		hbase.print("Hbase >>>>>>>>>>>>>>");


		//数据写入到hbase
		hbase.addSink(new DimSinkFunction());
		kafka.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
					@Override
					public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long aLong) {
						return new ProducerRecord<>(
								element.getString("sinkTable"),
								element.getString("after").getBytes()
						);
					}
				})
		);
		// TODO: 2022/1/13 启动任务
		environment.execute("BaseDBApp");
	}

}
