package com.mqk.gmall.app;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基础任务环境配置
 */
public class BaseEnviroment {

	public static StreamExecutionEnvironment getEnvironment(){
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		//防止访问hdfs报错
		System.setProperty("HADOOP_USER_NAME","root");
		//执行环境
		//设置并行度
		environment.setParallelism(1); //生产环境与kafka的分区数数目保持一致
		//指定状态后端
		environment.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/chk"));
		//开启chk 5s做一次chk
		environment.enableCheckpointing(5000L);
		//超时时间，根据状态的保存时间来判断
		environment.getCheckpointConfig().setCheckpointTimeout(10000L);
		environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		environment.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
		//两次chk之间最小的暂停时间
		environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
		return environment;
	}



}
