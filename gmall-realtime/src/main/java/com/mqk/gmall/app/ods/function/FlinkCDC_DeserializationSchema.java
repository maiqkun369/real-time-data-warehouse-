package com.mqk.gmall.app.ods.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class FlinkCDC_DeserializationSchema implements DebeziumDeserializationSchema<String> {
	/**
	 * 自定义反序列化方式
	 * @param sourceRecord
	 * @param collector
	 * @throws Exception
	 */
	@Override
	public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
		final JSONObject res = new JSONObject();

		//获取库名和表名
		final String topic = sourceRecord.topic();
		final String[] fields= topic.split("\\.");
		res.put("database",fields[1]);
		res.put("tableName",fields[2]);

		//获取before数据
		final Struct value = (Struct) sourceRecord.value();
		final Struct before = value.getStruct("before");
		final JSONObject beforeJson = new JSONObject();

		if(null != before){
			final Schema schema = before.schema();
			//列名
			final List<Field> fieldsList = schema.fields();
			for (Field field : fieldsList) {
				beforeJson.put(field.name(), before.get(field));
			}
		}
		res.put("before", beforeJson);

		//获取after数据
		final Struct after = value.getStruct("after");
		final JSONObject afterJson = new JSONObject();

		if(null != after){
			final Schema schema = after.schema();
			//列名
			final List<Field> fieldsList = schema.fields();
			for (Field field : fieldsList) {
				afterJson.put(field.name(), after.get(field));
			}
		}
		res.put("after", afterJson);

		//获取操作类型
		final Envelope.Operation operation = Envelope.operationFor(sourceRecord);
		String type = operation.toString().toLowerCase();
		if("read".equals(type) || "create".equals(type)){
			type = "insert";
		}
		res.put("type", type);

		collector.collect(res.toJSONString());
	}


	@Override
	public TypeInformation<String> getProducedType() {
		return BasicTypeInfo.STRING_TYPE_INFO;
	}
}
