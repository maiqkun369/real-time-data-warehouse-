package com.mqk.gmall.utils;

import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.common.GmallConfig;
import redis.clients.jedis.Jedis;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class DimUtil {


	public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
		//加入缓存
		final Jedis jedis = RedisUtil.getJedis();
		String redisKey = "DIM:" + tableName + ":" + id;

		final String dimInfoJsonStr = jedis.get(redisKey);
		if(null != dimInfoJsonStr){
			//归还连接
			//重置过期时间
			jedis.expire(redisKey, 24 * 60 * 60);
			jedis.close();
			return JSONObject.parseObject(dimInfoJsonStr);
		}


		//拼接查询语句
		StringBuffer querySQL = new StringBuffer();
		querySQL.append("select * from ").append(GmallConfig.HBASE_SCHEMA).append(".").append(tableName)
				.append(" where id=").append("'").append(id).append("'");

		//查询phoenix
		final List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, querySQL.toString(), JSONObject.class, false);


		final JSONObject jsonObject = jsonObjects.get(0);
		//放入缓存
		jedis.set(redisKey, jsonObject.toJSONString());
		//重置过期时间
		jedis.expire(redisKey, 24 * 60 * 60);
		jedis.close();
		return jsonObject;
	}

	public static  void delRedisDimInfo(String tableName, String id){
		final Jedis jedis = RedisUtil.getJedis();
		String redisKey = "DIM:" + tableName + ":" + id;
		jedis.del(redisKey);
		jedis.close();
	}

	public static void main(String[] args) throws Exception {
		Class.forName(GmallConfig.PHOENIX_DRIVER);
		final Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
		System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "13"));

	}
}
