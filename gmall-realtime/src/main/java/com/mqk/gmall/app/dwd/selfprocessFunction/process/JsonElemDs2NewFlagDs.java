package com.mqk.gmall.app.dwd.selfprocessFunction.process;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class JsonElemDs2NewFlagDs extends RichMapFunction<JSONObject, JSONObject> {
	private ValueState<String> valueState;

	@Override
	public void open(Configuration parameters) throws Exception {
		valueState = this.getRuntimeContext().getState(new ValueStateDescriptor<>("value-start", String.class));
	}

	@Override
	public void close() throws Exception {
		if (null != valueState) {
			valueState.clear();
		}
	}

	@Override
	public JSONObject map(JSONObject jsonObject) throws Exception {

		//获取数据的isNew标记
		final String isNew = jsonObject.getJSONObject("common").getString("is_new");
		if ("1".equals(isNew)) {
			//根据状态来判断是否是新用户
			final String state = this.valueState.value();
			if (null != state) {
				//修改isNew标记
				jsonObject.getJSONObject("common").put("is_new", "0");
			} else {
				valueState.update("1");
			}
		}
		return jsonObject;
	}
}
