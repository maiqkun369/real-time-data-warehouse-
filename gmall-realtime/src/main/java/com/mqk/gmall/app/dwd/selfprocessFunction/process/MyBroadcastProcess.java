package com.mqk.gmall.app.dwd.selfprocessFunction.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.common.GmallConfig;
import com.mqk.gmall.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
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

/**
 * 这里有一些 broadcast state 的重要注意事项，在使用它时需要时刻清楚：
 * 1.没有跨 task 通讯：如上所述，这就是为什么只有在 (Keyed)-BroadcastProcessFunction 中处理广播流元素的方法里可以更改 broadcast state 的内容。
 * 同时，用户需要保证所有 task 对于 broadcast state 的处理方式是一致的，否则会造成不同 task 读取 broadcast state 时内容不一致的情况，最终导致结果不一致。
 *
 * 2.broadcast state 在不同的 task 的事件顺序可能是不同的：虽然广播流中元素的过程能够保证所有的下游 task 全部能够收到，但在不同 task 中元素的到达顺序可能不同。
 * 所以 broadcast state 的更新不能依赖于流中元素到达的顺序。
 *
 * 3.所有的 task 均会对 broadcast state 进行 checkpoint：虽然所有 task 中的 broadcast state 是一致的，但当 checkpoint 来临时所有 task 均会对 broadcast state 做 checkpoint。
 * 这个设计是为了防止在作业恢复后读文件造成的文件热点。当然这种方式会造成 checkpoint 一定程度的写放大，放大倍数为 p（=并行度）。
 * Flink 会保证在恢复状态/改变并发的时候数据没有重复且没有缺失。 在作业恢复时，如果与之前具有相同或更小的并发度，所有的 task 读取之前已经 checkpoint 过的 state。在增大并发的情况下，
 * task 会读取本身的 state，多出来的并发（p_new - p_old）会使用轮询调度算法读取之前 task 的 state。
 *
 * 4.不使用 RocksDB state backend: broadcast state 在运行时保存在内存中，需要保证内存充足。这一特性同样适用于所有其他 Operator State。
 * 形参：
 *   1.非广播流中的元素类型
 *   2.广播流中的元素类型
 *   3.结果的类型，在这里是 JSONObject
 */
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
	 * 处理广播流，在处理广播流元素这端，对broadcast state 是具有读写权限的，而对于处理非广播流元素这端是只读的。
	 * 这样做的原因是，Flink 中是不存在跨 task 通讯的
	 * ctx:
	 * 		得到广播流的存储状态：ctx.getBroadcastState(MapStateDescriptor<K, V> stateDescriptor)
	 * 		查询元素的时间戳：ctx.timestamp()
	 * 		查询目前的Watermark：ctx.currentWatermark()
	 * 		目前的处理时间(processing time)：ctx.currentProcessingTime()
	 * 		产生旁路输出：ctx.output(OutputTag<X> outputTag, X value)
	 *
	 * @param value
	 * @param ctx
	 * @param collector
	 * @throws Exception
	 */
	@Override
	public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> collector) throws Exception {

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
		final BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDes);
		String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
		broadcastState.put(key, tableProcess);


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
		final MapState<String, TableProcess> mapState = getRuntimeContext().getMapState(mapStateDes);
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
