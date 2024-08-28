package com.ericsson.eniq.bean;

public class License {

	private String keyId;
	private LicenseType type;

	public String getKeyId() {
		return keyId;
	}

	public void setKeyId(String keyId) {
		this.keyId = keyId;
	}

	public LicenseType getType() {
		return type;
	}

	public void setType(LicenseType type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "License [keyId=" + keyId + ", type=" + type + "]";
	}
	
	

}
