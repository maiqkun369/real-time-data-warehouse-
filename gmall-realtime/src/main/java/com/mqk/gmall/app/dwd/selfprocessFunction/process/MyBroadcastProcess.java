package com.mqk.gmall.app.dwd.selfprocessFunction.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.common.GmallConfig;
import com.mqk.gmall.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class MyBroadcastProcess extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

	private Connection connection;
	private OutputTag<JSONObject> hbaseOutput;
	private MapStateDescriptor<String, TableProcess> mapStateDes;

	public MyBroadcastProcess(OutputTag<JSONObject> hbaseOutput, MapStateDescriptor<String, TableProcess> mapStateDes) {
		this.hbaseOutput = hbaseOutput;
		this.mapStateDes = mapStateDes;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		Class.forName(GmallConfig.PHOENIX_DRIVER);
		connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
	}

	/**
	 * 处理广播流
	 *
	 * @param value
	 * @param context
	 * @param collector
	 * @throws Exception
	 */
	@Override
	public void processBroadcastElement(String value, Context context, Collector<JSONObject> collector) throws Exception {

		//  1.解析数据 => TableProcess
		final JSONObject jsonObject = JSON.parseObject(value);
		final String data = jsonObject.getString("after");
		final TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

		//  2.检查Hbase表是否存在并创建表
		if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
			checkTable(
					tableProcess.getSinkTable(),
					tableProcess.getSinkColumns(),
					tableProcess.getSinkPk(),
					tableProcess.getSinkExtend()
			);
		}

		//  3.写入状态，广播出去
		final BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDes);
		String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
		broadcastState.put(key, tableProcess);


	}

	/**
	 * 建表语句：
	 *  create table if not exists db.tn(
	 *      id varchar primary key,
	 *      tm_name varchar
	 *  ) xxx;
	 * @param sinkTable
	 * @param sinkColumns
	 * @param sinkPk
	 * @param sinkExtend
	 */
	private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
		PreparedStatement preparedStatement = null;
		try {
			if(null == sinkPk){
				sinkPk = "id";
			}
			if(null == sinkExtend){
				sinkExtend = "";
			}

			final StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
					.append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable)
					.append(" (");

			final String[] fields = sinkColumns.split(",");
			for (int i = 0; i < fields.length; i++) {
				String field = fields[i];
				//判断主键
				if(sinkPk.equals(field)){
					createTableSQL.append(field).append(" varchar primary key ");
				}else {
					createTableSQL.append(field).append(" varchar ");
				}
				//判断是否为最后一个字段
				if(i < fields.length - 1){
					createTableSQL.append(",");
				}
			}
			createTableSQL.append(")").append(sinkExtend);

			System.out.println(createTableSQL);
			//预编译sql
			preparedStatement = connection.prepareStatement(createTableSQL.toString());

			preparedStatement.execute();
		} catch (SQLException e) {
			throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
		}finally {
			if(null != preparedStatement){
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 处理主流
	 * value -> {"database":"gmall-flink","before":{},
	 *  "after":{"consignee_tel":"158144392939","id":26449,"consignee":"张三","total_amount":21966.00},
	 *  "type":"insert","tableName":"order_info"}
	 *  1.读取状态
	 *  2.过滤数据
	 *  3.分流
	 * @param value
	 * @param readOnlyContext
	 * @param collector 用来输出到主流
	 * @throws Exception
	 */
	@Override
	public void processElement(JSONObject value, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
		//读取状态
		final ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDes);

		String key = value.getString("tableName") + "-" + value.getString("type");
		final TableProcess tableProcess = broadcastState.get(key);

		if(null != tableProcess){
			//过滤字段
			final JSONObject data = value.getJSONObject("after");
			filterColumn(data, tableProcess.getSinkColumns());

			//分流
			//将输出表/主题信息写入到value
			value.put("sinkTable", tableProcess.getSinkTable());
			if(TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())){
				//kafka数据,写入主流
				collector.collect(value);
			}else if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
				//hbase数据写入侧流
				readOnlyContext.output(hbaseOutput, value);
			}
		}else {
			// TODO: 2022/1/19 此处可以利用jdbc的方式查询table_process判断key到底存不存在
			System.out.println("该组合key: " + key + "不存在!");
		}




	}

	/**
	 *
	 * @param data {"id":"11","tm_name":"xxxx","logo_url":"aaa"}
	 * @param sinkColumns id,tm_name
	 *                   处理后  {"id":"11","tm_name":"xxxx"}
	 */
	private static void filterColumn(JSONObject data, String sinkColumns) {
		final String[] split = sinkColumns.split(",");
		final List<String> colums = Arrays.asList(split);

		data.entrySet().removeIf(next -> !colums.contains(next.getKey()));

//		final Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
//		while (iterator.hasNext()){
//			final Map.Entry<String, Object> next = iterator.next();
//			if(!colums.contains(next.getKey())){
//				iterator.remove();
//			}
//		}
	}

}
