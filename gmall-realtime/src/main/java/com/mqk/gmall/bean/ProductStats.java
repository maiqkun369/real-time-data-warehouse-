package com.mqk.gmall.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

@Data
@Builder
public class ProductStats {

	String stt;//窗口起始时间
	String edt; //窗口结束时间
	Long sku_id; //sku 编号
	String sku_name;//sku 名称
	BigDecimal sku_price; //sku 单价

	Long spu_id; //spu 编号
	String spu_name;//spu 名称
	Long tm_id; //品牌编号
	String tm_name;//品牌名称
	Long category3_id;//品类编号
	String category3_name;//品类名称

	@Builder.Default //设置默认值
	Long display_ct = 0L; //曝光数
	@Builder.Default
	Long click_ct = 0L; //点击数
	@Builder.Default
	Long favor_ct = 0L; //收藏数
	@Builder.Default
	Long cart_ct = 0L; //添加购物车数
	@Builder.Default
	Long order_sku_num = 0L; //下单商品个数
	@Builder.Default
	BigDecimal order_amount = BigDecimal.ZERO; //下单商品金额
	@Builder.Default
	Long order_ct = 0L; //订单数
	@Builder.Default
	BigDecimal payment_amount = BigDecimal.ZERO;//支付金额
	@Builder.Default
	Long paid_order_ct = 0L; //支付订单数
	@Builder.Default
	Long refund_order_ct = 0L; //退款订单数
	@Builder.Default
	BigDecimal refund_amount = BigDecimal.ZERO;
	@Builder.Default
	Long comment_ct = 0L;//评论订单数
	@Builder.Default
	Long good_comment_ct = 0L; //好评订单数
	@Builder.Default
	@TransientSink
	Set orderIdSet = new HashSet(); //用于统计订单数
	@Builder.Default
	@TransientSink
	Set paidOrderIdSet = new HashSet(); //用于统计支付订单数
	@Builder.Default
	@TransientSink
	Set refundOrderIdSet = new HashSet();//用于退款支付订单数

	Long ts; //统计时间戳
}