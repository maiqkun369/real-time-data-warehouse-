package com.mqk.gmall.bean;

import com.google.common.base.MoreObjects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Desc:地区统计宽表实体类
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ProvinceStats {
	private String stt;
	private String edt;
	private Long province_id;
	private String province_name;
	private String province_area_code;
	private String province_iso_code;
	private String province_3166_2_code;
	private BigDecimal order_amount;
	private Long order_count;
	private Long ts;

	public String getStt() {
		return stt;
	}

	public void setStt(String stt) {
		this.stt = stt;
	}

	public String getEdt() {
		return edt;
	}

	public void setEdt(String edt) {
		this.edt = edt;
	}

	public Long getProvince_id() {
		return province_id;
	}

	public void setProvince_id(Long province_id) {
		this.province_id = province_id;
	}

	public String getProvince_name() {
		return province_name;
	}

	public void setProvince_name(String province_name) {
		this.province_name = province_name;
	}

	public String getProvince_area_code() {
		return province_area_code;
	}

	public void setProvince_area_code(String province_area_code) {
		this.province_area_code = province_area_code;
	}

	public String getProvince_iso_code() {
		return province_iso_code;
	}

	public void setProvince_iso_code(String province_iso_code) {
		this.province_iso_code = province_iso_code;
	}

	public String getProvince_3166_2_code() {
		return province_3166_2_code;
	}

	public void setProvince_3166_2_code(String province_3166_2_code) {
		this.province_3166_2_code = province_3166_2_code;
	}

	public BigDecimal getOrder_amount() {
		return order_amount;
	}

	public void setOrder_amount(BigDecimal order_amount) {
		this.order_amount = order_amount;
	}

	public Long getOrder_count() {
		return order_count;
	}

	public void setOrder_count(Long order_count) {
		this.order_count = order_count;
	}

	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("stt", stt)
				.add("edt", edt)
				.add("province_id", province_id)
				.add("province_name", province_name)
				.add("province_area_code", province_area_code)
				.add("province_iso_code", province_iso_code)
				.add("province_3166_2_code", province_3166_2_code)
				.add("order_amount", order_amount)
				.add("order_count", order_count)
				.add("ts", ts)
				.toString();
	}
}
