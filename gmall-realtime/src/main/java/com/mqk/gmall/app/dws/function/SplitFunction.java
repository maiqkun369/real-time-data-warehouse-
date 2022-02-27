package com.mqk.gmall.app.dws.function;

import com.mqk.gmall.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {


	public void eval(String str){
		//分词
		final List<String> words = KeywordUtil.analyze(str);
		//遍历并且写出
		for (String word : words) {
			this.collect(Row.of(word));
		}

	}

}
