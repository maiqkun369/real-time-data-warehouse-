package com.mqk.gmall.app.dwm.selfprocessFunction;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;

public class MidFilter extends RichFilterFunction<JSONObject> {

	private ValueState<String> dateState;
	private SimpleDateFormat simpleDateFormat;


	@Override
	public void open(Configuration parameters) throws Exception {
		final ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visit-state", String.class);
		// TODO: 2022/1/17 设置状态TTL
		final StateTtlConfig build = StateTtlConfig
				.newBuilder(Time.days(1))
				//todo 设置如何重置状态的ttl. 创建或者写时进行状态TTL的重置
				.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
				.build();
		valueStateDescriptor.enableTimeToLive(build);

		this.dateState = this.getRuntimeContext().getState(valueStateDescriptor);
		simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

	}

	@Override
	public boolean filter(JSONObject value) throws Exception {
		//取出上一条页面信息
		final String lastPageId = value.getJSONObject("page").getString("last_page_id");

		if(null == lastPageId || lastPageId.length() <= 0){
			final String lastDate = dateState.value();
			final String currentDate = simpleDateFormat.format(value.getLong("ts"));
			if(!currentDate.equals(lastDate)){
				dateState.update(currentDate);
				return true;
			}else {
				return false;
			}
		}else {
			return false;
		}
	}
}
