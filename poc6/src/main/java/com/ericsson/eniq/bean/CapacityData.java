package com.ericsson.eniq.bean;

public class CapacityData {

	private Boolean isLimited;
	private Integer licensedCapacity;

	public Boolean getIsLimited() {
		return isLimited;
	}

	public void setIsLimited(Boolean isLimited) {
		this.isLimited = isLimited;
	}

	public Integer getLicensedCapacity() {
		return licensedCapacity;
	}

	public void setLicensedCapacity(Integer licensedCapacity) {
		this.licensedCapacity = licensedCapacity;
	}

	@Override
	public String toString() {
		return "CapacityData [isLimited=" + isLimited + ", licensedCapacity=" + licensedCapacity + "]";
	}

	
	
}
