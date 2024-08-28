package com.ericsson.eniq.config;

public class License {

	private String keyId;
	private String type;

	public String getKeyId() {
		return keyId;
	}

	public void setKeyId(String keyId) {
		this.keyId = keyId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "License [keyId=" + keyId + ", type=" + type + "]";
	}
	
	

}
