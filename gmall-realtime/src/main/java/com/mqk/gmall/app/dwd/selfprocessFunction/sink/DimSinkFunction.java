package com.mqk.gmall.app.dwd.selfprocessFunction.sink;

import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.common.GmallConfig;
import com.mqk.gmall.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;


public class DimSinkFunction extends RichSinkFunction<JSONObject> {
	private Connection connection;
	@Override
	public void open(Configuration parameters) throws Exception {
		Class.forName(GmallConfig.PHOENIX_DRIVER);
		connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
		connection.setAutoCommit(true);
	}

	/**
	 *
	 * @param value {
	 * 	  "sinkTable":"dim_base_trademark",
	 * 	  "database":"gmall-flink",
	 * 	  "before":{},
	 * 	  "after":{"tm_name":"sz","id":12},
	 * 	  "type":"insert","tableName":"base_trademark"}
	 * @param context
	 * @throws Exception
	 */
	@Override
	public void invoke(JSONObject value, Context context) throws Exception {
		PreparedStatement preparedStatement = null;

		try {
			final String sinkTable = value.getString("sinkTable");
			final JSONObject after = value.getJSONObject("after");
			String upsertSQL = genericSQL(
					sinkTable,
					after
			);
			System.out.println(upsertSQL);
			preparedStatement = connection.prepareStatement(upsertSQL);

			//判断当前数据为update，则删除redis中的数据
			if("update".equals(value.getString("type"))){
				DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
			}

			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(null != preparedStatement){
				preparedStatement.close();
			}
		}

	}

	private String genericSQL(String sinkTable, JSONObject data) {
		StringBuffer sb = new StringBuffer("upsert into ");
		sb.append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");

		final Set<String> keySet = data.keySet();
		final Collection<Object> values = data.values();

		sb.append(StringUtils.join(keySet,",")).append(") values('")
				.append(StringUtils.join(values,"','")).append("')");

		return sb.toString();
	}
}
