package com.mqk.gmall.enums;

public enum FlinkStartupOptionsEnum {
	INITIAL("initial"),
	LATEST("latest");

	private String method;

	FlinkStartupOptionsEnum(String method) {
		this.method = method;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}
}
