package com.mqk.gmall.utils;

import com.mqk.gmall.common.GmallConfig;
import com.mqk.gmall.bean.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import  org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

	public static <T> SinkFunction<T> getSink(String sql){

		return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
			@Override
			public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
				try {
					//获取所有的属性信息
					final Field[] fields = t.getClass().getDeclaredFields();

					int offset = 0;
					for (int i = 0; i < fields.length; i++) {
						final Field field = fields[i];

						//私有属性可访问
						field.setAccessible(true);

						//获取注解字段
						final TransientSink annotation = field.getAnnotation(TransientSink.class);
						if(null != annotation){
							//该字段被注解标记了
							offset ++;
							continue;
						}

						//根据反射获取值
						final Object value = field.get(t);

						//获取值
						preparedStatement.setObject(i + 1 - offset, value);
					}
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		},
				new JdbcExecutionOptions
						.Builder()
						.withBatchSize(5)
						.build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
						.withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
						.withUrl(GmallConfig.CLICKHOUSE_URL)
						.build()
		);



	}

}
