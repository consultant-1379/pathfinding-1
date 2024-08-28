package com.ericsson.eniq.model;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "feature_activation")
public class FeatureActivation {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long f_id;

	@Column(name = "license_no")
	private Integer licenseno;

	@Column(name = "feature_desc")
	private String featureDescription;

	@Column(name = "techpacks")
	private String techpacks;

	@Column(name = "status")
	private String status;

	@Column(name = "created_on")
	private Timestamp createdOn;

	@Column(name = "updated_on")
	private Timestamp updatedOn;

	public FeatureActivation() {

	}

	public FeatureActivation(Integer licenseno, String featureDescription, String techpacks, String status,
			Timestamp createdOn, Timestamp updatedOn) {
		super();
		this.licenseno = licenseno;
		this.featureDescription = featureDescription;
		this.techpacks = techpacks;
		this.status = status;
		this.createdOn = createdOn;
		this.updatedOn = updatedOn;
	}

	public Long getF_id() {
		return f_id;
	}

	public void setF_id(Long f_id) {
		this.f_id = f_id;
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

	public Integer getLicenseno() {
		return licenseno;
	}

	public void setLicenseno(Integer licenseno) {
		this.licenseno = licenseno;
	}

	public String getFeatureDescription() {
		return featureDescription;
	}

	public void setFeatureDescription(String featureDescription) {
		this.featureDescription = featureDescription;
	}

	public String getTechpacks() {
		return techpacks;
	}

	public void setTechpacks(String techpacks) {
		this.techpacks = techpacks;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public String toString() {
		return "FeatureActivation [licenseno=" + licenseno + ", featureDescription=" + featureDescription
				+ ", techpacks=" + techpacks + ", status=" + status + "]";
	}

}
