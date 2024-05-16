package com.mqk.gmall.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.app.ods.function.FlinkCDC_DeserializationSchema;
import com.mqk.gmall.enums.FlinkStartupOptionsEnum;
import com.mqk.gmall.utils.MyKafkaUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FlinkCDC {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();
		//设置批处理
//		environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
		//cdc 构建数据源
		final DataStreamSource<String> dataStream = environment.addSource(listenMysql(FlinkStartupOptionsEnum.LATEST));

		//sink 数据到kafka
		dataStream.addSink(MyKafkaUtil.getKafkaProducer("ods_base_db"));

		dataStream.print("cdc");

		environment.execute("FlinkCDCWithCustomerDeserialization");
	}

	public static DebeziumSourceFunction listenMysql(FlinkStartupOptionsEnum startupOptions){
		final MySQLSource.Builder<String> sourceBuilder = MySQLSource
				.<String>builder()
				.hostname("hadoop102")
				.port(3306)
				.username("root")
				.password("000000")
				.databaseList("gmall-flink") //监听的数据库
				.deserializer(new FlinkCDC_DeserializationSchema());// 自定义反序列化器
		switch (startupOptions){
			case LATEST: return sourceBuilder.startupOptions(StartupOptions.latest()).build();
			case INITIAL: return sourceBuilder.startupOptions(StartupOptions.initial()).build();
			default: return null;
		}
	}
}
