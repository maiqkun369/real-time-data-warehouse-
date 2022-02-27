package com.mqk.gmall.app.dws;

import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.app.dws.function.SplitFunction;
import com.mqk.gmall.bean.KeywordStats;
import com.mqk.gmall.common.GmallConstant;
import com.mqk.gmall.utils.ClickHouseUtil;
import com.mqk.gmall.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
	public static void main(String[] args) throws Exception {
		// TODO: 2022/2/27 执行环境
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();
		final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);


		// TODO: 2022/2/27 DDL方式读取kafka数据
		String groupId = "keyword_stats_app";
		String pageViewSourceTopic ="dwd_page_log";

		tableEnv.executeSql("CREATE TABLE page_view " +
				"(common MAP<STRING,STRING>, " +
				"page MAP<STRING,STRING>," +
				"ts BIGINT, " +
				"rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
				"WATERMARK FOR rowtime AS rowtime - INTERVAL '' SECOND) " +
				"WITH ("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId)+")");


		// TODO: 2022/2/27 过滤数据上一跳为“search” and 搜索词 is not null
		Table fullwordView = tableEnv.sqlQuery("select page['item'] full_word ," +
				"rowtime from page_view " +
				"where page['last_page_id']='search' " +
				"and page['item'] IS NOT NULL ");


		// TODO: 2022/2/27 注册udtf,进行分词处理
		tableEnv.createTemporarySystemFunction("split_words", SplitFunction.class);
		Table keywordView = tableEnv.sqlQuery("select word,rowtime from " + fullwordView +
				" ,LATERAL TABLE(split_words(full_word))");

		// TODO: 2022/2/27 分组开窗聚合
		Table keywordStatsSearch = tableEnv.sqlQuery(
				"SELECT " +
						GmallConstant.KEYWORD_SEARCH + "' source ," +
						"word as keyword,count(*) ct,"+
						"DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
						"DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
						"UNIX_TIMESTAMP()*1000 ts " +
					"FROM "+ keywordView
				 + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ), keyword");

		// TODO: 2022/2/27 将动态表转回为流
		final DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);
		// TODO: 2022/2/27 数据写入clickhouse
		keywordStatsDataStream.print("keywordStatsDataStream");
		keywordStatsDataStream.addSink(
				ClickHouseUtil.<KeywordStats>getSink(
						"insert into keyword_stats(keyword,ct,source,stt,edt,ts) " +
								" values(?,?,?,?,?,?)"));
		
				environment.execute("KeywordStatsApp");


	}
}
