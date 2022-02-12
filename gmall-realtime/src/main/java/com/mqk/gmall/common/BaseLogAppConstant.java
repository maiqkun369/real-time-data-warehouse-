package com.mqk.gmall.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.util.OutputTag;

public class BaseLogAppConstant {
	public static final OutputTag<String> dirtyOutput = new OutputTag<String>("dirty"){};
	public static final OutputTag<String> startOutput = new OutputTag<String>("start"){};
	public static final OutputTag<String> displayOutput = new OutputTag<String>("display"){};
	public static final OutputTag<JSONObject> hbaseOutput = new OutputTag<JSONObject>("hbase-tag"){};
}
