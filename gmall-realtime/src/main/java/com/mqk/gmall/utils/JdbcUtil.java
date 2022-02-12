package com.mqk.gmall.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.mqk.gmall.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {


	/**
	 *
	 * @param connection
	 * @param querySql
	 * @param <T>
	 * @return
	 */
	public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception{
		//创建集合用于存放查询结果数据
		List<T> resList = new ArrayList<>();

		final PreparedStatement preparedStatement = connection.prepareStatement(querySql);

		//执行查询
		final ResultSet resultSet = preparedStatement.executeQuery();


		final ResultSetMetaData metaData = resultSet.getMetaData();
		final int columnCount = metaData.getColumnCount();
		while (resultSet.next()){
			//创建泛型对象
			final T t = clz.newInstance();

			//泛型对象进行赋值
			for (int i = 1; i < columnCount + 1; i++) {
				//获取列名
				String columnName = metaData.getColumnName(i);
				//判断是否需要转换为驼峰命名
				if(underScoreToCamel){
					//小写的下滑线命名转换成小驼峰
					columnName = CaseFormat.LOWER_UNDERSCORE
							.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
				}
				//获取列值
				final Object columnValue = resultSet.getObject(i);

				//给泛型对象赋值
				BeanUtils.setProperty(t, columnName, columnValue);
			}
			resList.add(t);

		}
		preparedStatement.close();
		resultSet.close();

		return  resList;
	}

	public static void main(String[] args) throws Exception {
		Class.forName(GmallConfig.PHOENIX_DRIVER);
		final Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
		final List<JSONObject> jsonObjects = queryList(
				connection,
				"select * from GMALL2021_REALTIME.DIM_USER_INFO",
				JSONObject.class,
				true);

		for (JSONObject jsonObject : jsonObjects) {
			System.out.println(jsonObject);
		}
		connection.close();


	}
}
