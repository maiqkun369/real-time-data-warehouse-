package com.mqk.gmall.app.dws;

import com.mqk.gmall.app.BaseEnviroment;
import com.mqk.gmall.bean.ProvinceStats;
import com.mqk.gmall.utils.ClickHouseUtil;
import com.mqk.gmall.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 数据流：web/app -> nginx -> springboot -> mysql -> flinkApp -> kafka(ods)
 *              -> flinkApp -> kafka/phoenix(dwd-dim) -> flinkApp9(redis) -> kafka(dwm) -> flinkApp -> clickhouse
 */
public class ProvinceStatsSqlApp {
	public static void main(String[] args) throws Exception {

		// TODO: 2022/2/27 执行环境
		final StreamExecutionEnvironment environment = BaseEnviroment.getEnvironment();
		final StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

		// TODO: 2022/2/27 DDL创建表并提取时间戳生成wk
		String groupId = "province_stats";
		String orderWideTopic = "dwm_order_wide";

		tableEnvironment.executeSql("CREATE TABLE ORDER_WIDE (" +
						"province_id BIGINT, " +
						"province_name STRING," +
						"province_area_code STRING," +
						"province_iso_code STRING," +
						"province_3166_2_code STRING," +
						"order_id STRING, " +
						"total_amount DOUBLE," +
						"create_time STRING," +
						"rt AS TO_TIMESTAMP(create_time)," +
						"WATERMARK FOR rt AS rt - INTERVAL '1' SECOND )" +
						" WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

		// TODO: 2022/2/27 查询数据，分组开窗聚合
		Table provinceStateTable = tableEnvironment.sqlQuery("select " +
						"DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
						"DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt, " +
						"province_id,province_name,province_area_code," +
						"province_iso_code,province_3166_2_code," +
						"COUNT( DISTINCT order_id) order_count, sum(total_amount) order_amount," +
						"UNIX_TIMESTAMP()*1000 ts "+
						" from " +
							"ORDER_WIDE " +
						" group by " +
							"TUMBLE(rt, INTERVAL '10' SECOND ), province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");



		// TODO: 2022/2/27 将动态表转换成流
		final DataStream<ProvinceStats> provinceStatsDataStream = tableEnvironment.toAppendStream(provinceStateTable, ProvinceStats.class);
		provinceStatsDataStream.print();


		// TODO: 2022/2/27 写入clickhouse中
		provinceStatsDataStream.addSink(ClickHouseUtil.
				<ProvinceStats>getSink("insert into province_stats_2021 values(?,?,?,?,?,?,?,?,?,?)"));




		environment.execute("provinceStatsDataStream");
	}
}
