package com.mqk.gmall.app.dwm.selfprocessFunction;

import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.common.GmallConfig;
import com.mqk.gmall.utils.DimUtil;
import com.mqk.gmall.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * 抽象方法加泛型实现通用适配
 * @param <T>
 */
public abstract class DimAsyncFunction<T>
		extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T> {
	private Connection connection;
	private ThreadPoolExecutor threadPoolExecutor;

	private String tableName;

	public DimAsyncFunction(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		Class.forName(GmallConfig.PHOENIX_DRIVER);
		connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
		threadPoolExecutor = ThreadPoolUtil.getThreadPool();
	}

	@Override
	public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
		threadPoolExecutor.submit(
				() -> {
					try {
						String id = getKey(input);
						//查询维度信息
						final JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
						//补充维度信息
						if(null != dimInfo){
							join(input, dimInfo);
						}
						//数据输出
						resultFuture.complete(Collections.singletonList(input));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
		);
	}

	@Override
	public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
		System.out.println("Timeout:" + input);
	}
}
