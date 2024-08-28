package com.ericsson.eniq.bean;

public class LicenseInfo {

	private License license;
	private String licenseStatus;
	private CapacityData capacityInfo;

	public License getLicense() {
		return license;
	}

	public void setLicense(License license) {
		this.license = license;
	}

	public String getLicenseStatus() {
		return licenseStatus;
	}

	public void setLicenseStatus(String licenseStatus) {
		this.licenseStatus = licenseStatus;
	}

	public CapacityData getCapacityInfo() {
		return capacityInfo;
	}

	public void setCapacityInfo(CapacityData capacityInfo) {
		this.capacityInfo = capacityInfo;
	}

	@Override
	public String toString() {
		return "LicenseInfo [license=" + license + ", licenseStatus=" + licenseStatus + ", capacityInfo=" + capacityInfo
				+ "]";
	}
	
	

}
