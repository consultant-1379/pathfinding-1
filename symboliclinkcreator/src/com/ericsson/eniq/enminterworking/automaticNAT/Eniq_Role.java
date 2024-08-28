package com.ericsson.eniq.enminterworking.automaticNAT;


public class Eniq_Role {

	private String eniq_identifier = null;

	private String ip_address = null;

	private String role = null;

	public Eniq_Role(String eniq_identifier, String ip_address, String role) {
		this.eniq_identifier = eniq_identifier;
		this.ip_address = ip_address;
		this.role = role;

	}

	public String getEniq_identifier() {
		return eniq_identifier;
	}

	public String getIp_address() {
		return ip_address;
	}

	public String getRole() {
		return role;
	}
		
	public String toString() {
		return eniq_identifier;
	}
}
