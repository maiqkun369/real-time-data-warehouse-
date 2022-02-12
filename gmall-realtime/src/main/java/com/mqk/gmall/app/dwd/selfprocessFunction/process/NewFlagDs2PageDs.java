package com.mqk.gmall.app.dwd.selfprocessFunction.process;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.common.BaseLogAppConstant;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 数据流： web/app -> Nginx -> springboot -> kafka(ods) -> flinkApp -> kafka(dwd)
 *
 */
public class NewFlagDs2PageDs extends ProcessFunction<JSONObject, String> {
	@Override
	public void processElement(JSONObject value, Context context, Collector<String> collector) throws Exception {
		//启动日志字段
		final String start = value.getString("start");
		if (null != start && 0 < start.length()) {
			//数据写入启动日志测输出流
			context.output(BaseLogAppConstant.startOutput, value.toJSONString());
		} else {
			//将数据写入页面日志主流
			collector.collect(value.toJSONString());
			//取出数据中的曝光数据
			final JSONArray displays = value.getJSONArray("displays");
			if (null != displays && 0 < displays.size()) {
				//获取页面id
				final String pageId = value.getJSONObject("page").getString("page_id");

				for (int i = 0; i < displays.size(); i++) {
					final JSONObject dispaly = displays.getJSONObject(i);
					//添加页面id
					dispaly.put("page_id", pageId);
					//写出到曝光测输出流
					context.output(BaseLogAppConstant.displayOutput, displays.toJSONString());
				}
			}
		}
	}
}
