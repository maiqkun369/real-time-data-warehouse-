package com.mqk.gmall.app.dwm.selfprocessFunction;

import com.alibaba.fastjson.JSONObject;

public interface DimAsyncJoinFunction<T> {
	String getKey(T input);

	void join(T input, JSONObject dimInfo);

}
