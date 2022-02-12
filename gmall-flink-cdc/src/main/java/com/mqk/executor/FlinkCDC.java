package com.mqk.executor;

import com.mqk.serializer.FlinkCDC_DeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setParallelism(1);

		//开启chk 5s做一次chk
		environment.enableCheckpointing(5000L);

		//超时时间，根据状态的保存时间来判断
		environment.getCheckpointConfig().setCheckpointTimeout(10000L);
		environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		environment.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
		//两次chk之间最小的暂停时间
		environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);

//		//指定状态后端
//		environment.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/chk"));

		//cdc 构建数据源
		final DataStreamSource<String> dataStream = environment.addSource(listenWithinitial());
		dataStream.print("cdc");

		environment.execute("cdc job");
	}

	public static DebeziumSourceFunction listenWithinitial(){
		return MySqlSource
				.<String>builder()
				.hostname("hadoop102")
				.port(3306)
				.username("root")
				.password("000000")
				.databaseList("gmall-flink") //监听的数据库
				.tableList("gmall-flink.base_trademark")
				.deserializer(new FlinkCDC_DeserializationSchema()) // 自定义反序列化器
				.startupOptions(StartupOptions.initial()) //该方式先读取一次全表快照
				.build();
	}

}
