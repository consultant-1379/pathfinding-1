package com.ericsson.eniq.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "tech_pack_activation")
public class TechpackActivation {
	
	// INSERT INTO tech_pack_activation (id, tp_name, tp_version, tp_status)
	
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;
	
	@Column(name = "tp_name")
	private String tpName;
	
	@Column(name = "tp_version")
	private String tpVersion;

	@Column(name = "tp_status")
	private String tpStatus;

	public TechpackActivation() {

	}

	public TechpackActivation(Long id, String tpName, String tpVersion, String tpStatus) {
		super();
		this.id = id;
		this.tpName = tpName;
		this.tpVersion = tpVersion;
		this.tpStatus = tpStatus;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getTpName() {
		return tpName;
	}

	public void setTpName(String tpName) {
		this.tpName = tpName;
	}

	public String getTpVersion() {
		return tpVersion;
	}

	public void setTpVersion(String tpVersion) {
		this.tpVersion = tpVersion;
	}

	public String getTpStatus() {
		return tpStatus;
	}

	public void setTpStatus(String tpStatus) {
		this.tpStatus = tpStatus;
	}

	@Override
	public String toString() {
		return "TechpackActivation [id=" + id + ", tpName=" + tpName + ", tpVersion=" + tpVersion + ", tpStatus="
				+ tpStatus + "]";
	}
	
	

}
