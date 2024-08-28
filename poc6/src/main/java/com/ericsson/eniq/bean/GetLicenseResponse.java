package com.ericsson.eniq.bean;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties("licenses")
public class GetLicenseResponse {

	private OperationalStatus operationalStatusInfo;
	private List<LicenseInfo> licensesInfo;

	public OperationalStatus getOperationalStatusInfo() {
		return operationalStatusInfo;
	}

	public void setOperationalStatusInfo(OperationalStatus operationalStatusInfo) {
		this.operationalStatusInfo = operationalStatusInfo;
	}

	public List<LicenseInfo> getLicensesInfo() {
		return licensesInfo;
	}

	public void setLicensesInfo(List<LicenseInfo> licensesInfo) {
		this.licensesInfo = licensesInfo;
	}

	@Override
	public String toString() {
		return "GetLicenseResponse [operationalStatusInfo=" + operationalStatusInfo + ", licensesInfo=" + licensesInfo
				+ "]";
	}
	

}
