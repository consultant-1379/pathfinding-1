package com.ericsson.eniq.bean;

public enum LicenseStatus {

	VALID("VALID"), NOT_FOUND("NOT FOUND"), VALID_IN_FUTURE("VALID IN FUTURE"), EXPIRED("EXPIRED");

	private String value;

	public String getValue() {
		return value;
	}

	private LicenseStatus(String value) {
	  this.value = value;
	 }
}
