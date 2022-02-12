package com.mqk.gmall.bean;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
public class OrderDetail {
	 Long id;
	 Long order_id;
	 Long sku_id;
	 BigDecimal order_price;
	 Long sku_num;
	 String sku_name;
	 String create_time;
	 BigDecimal split_total_amount;
	 BigDecimal split_activity_amount;
	 BigDecimal split_coupon_amount;
	 Long create_ts;
}
