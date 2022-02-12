package com.mqk.gmall.app.dwm.selfprocessFunction;

import com.mqk.gmall.bean.OrderDetail;
import com.mqk.gmall.bean.OrderInfo;
import com.mqk.gmall.bean.OrderWide;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class MyProcessJoinFunctionn extends ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide> {
	@Override
	public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
		collector.collect(new OrderWide(orderInfo, orderDetail));
	}
}
