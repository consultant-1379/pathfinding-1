//package com.ericsson.eniq.config;
//
//import java.util.List;
//
//import org.springframework.boot.context.properties.ConfigurationProperties;
//import org.springframework.boot.context.properties.EnableConfigurationProperties;
//import org.springframework.context.annotation.Configuration;
//
//@Configuration
//@EnableConfigurationProperties
//@ConfigurationProperties("licenses")
//public class LicenseDetails {
//	
//	    private Object licenses;
//	    private Object operationalStatusInfo;
//	    private String operationalMode;
//	    private long autonomousModeDuration;
//	    private List<LicensesInfo> licensesInfo;
//	    private String keyID;
//	    private String type;
//	    private String licenseStatus;
//	    private CapacityData capacityInfo;
//	    private boolean isLimited;
//	    private long licensedCapacity;
//
//	    public Object getLicenses() { return licenses; }
//	    public void setLicenses(Object value) { this.licenses = value; }
//
//	    public Object getOperationalStatusInfo() { return operationalStatusInfo; }
//	    public void setOperationalStatusInfo(Object value) { this.operationalStatusInfo = value; }
//
//	    public String getOperationalMode() { return operationalMode; }
//	    public void setOperationalMode(String value) { this.operationalMode = value; }
//
//	    public long getAutonomousModeDuration() { return autonomousModeDuration; }
//	    public void setAutonomousModeDuration(long value) { this.autonomousModeDuration = value; }
//
//	    public List<LicensesInfo> getLicensesInfo() { return licensesInfo; }
//	    public void setLicensesInfo(List<LicensesInfo> value) { this.licensesInfo = value; }
//
//	    public String getKeyID() { return keyID; }
//	    public void setKeyID(String value) { this.keyID = value; }
//
//	    public String getType() { return type; }
//	    public void setType(String value) { this.type = value; }
//
//	    public String getLicenseStatus() { return licenseStatus; }
//	    public void setLicenseStatus(String value) { this.licenseStatus = value; }
//
//	    public CapacityData getCapacityInfo() { return capacityInfo; }
//	    public void setCapacityInfo(CapacityData value) { this.capacityInfo = value; }
//
//	    public boolean getIsLimited() { return isLimited; }
//	    public void setIsLimited(boolean value) { this.isLimited = value; }
//
//	    public long getLicensedCapacity() { return licensedCapacity; }
//	    public void setLicensedCapacity(long value) { this.licensedCapacity = value; }
//	}
//
//
//
//
