package com.mqk.gmall.app.dwd.selfprocessFunction.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mqk.gmall.common.BaseLogAppConstant;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class KafkaDsElem2JsonDsElem extends ProcessFunction<String, JSONObject> {
	@Override
	public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
		try {
			final JSONObject jsonObject = JSON.parseObject(value);
			collector.collect(jsonObject);
		} catch (Exception e) {
			//发生异常，数据写入测输出流
			context.output(BaseLogAppConstant.dirtyOutput, value);
		}
	}
}
