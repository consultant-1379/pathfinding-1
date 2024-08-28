package com.ericsson.eniq.bean;

import java.sql.Timestamp;

public class FeatureActivationRequest {

	private Long fId;
	private Integer licenseNo;
	private String featureDesc;
	private String techpacks;
	private Timestamp createdOn;
	private Timestamp updatedOn;
	private String status;
	
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Long getfId() {
		return fId;
	}

	public void setfId(Long fId) {
		this.fId = fId;
	}

	public Integer getLicenseNo() {
		return licenseNo;
	}

	public void setLicenseNo(Integer licenseNo) {
		this.licenseNo = licenseNo;
	}

	public String getFeatureDesc() {
		return featureDesc;
	}

	public void setFeatureDesc(String featureDesc) {
		this.featureDesc = featureDesc;
	}

	public String getTechpacks() {
		return techpacks;
	}

	public void setTechpacks(String techpacks) {
		this.techpacks = techpacks;
	}

	public Timestamp getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(Timestamp createdOn) {
		this.createdOn = createdOn;
	}

	public Timestamp getUpdatedOn() {
		return updatedOn;
	}

	public void setUpdatedOn(Timestamp updatedOn) {
		this.updatedOn = updatedOn;
	}

}
