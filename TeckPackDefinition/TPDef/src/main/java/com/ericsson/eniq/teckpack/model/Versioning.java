package com.ericsson.eniq.teckpack.model;

import java.sql.Timestamp;
public class Versioning {
	private  String VERSIONID;
	private String DESCRIPTION;
	private Long STATUS;
	private String TECHPACK_NAME;
	private String TECHPACK_VERSION;
	private String TECHPACK_TYPE;
	private String PRODUCT_NUMBER;
	private String LOCKEDBY;
	private Timestamp LOCKDATE;
	private String BASEDEFINITION;
	private String BASEVERSION;
	private String INSTALLDESCRIPTION;
	private String UNIVERSENAME;
	private String UNIVERSEEXTENSION;
	private String ENIQ_LEVEL;
	private String LICENSENAME;

	public Versioning() {
		 this.VERSIONID = null;
         this.DESCRIPTION = null;
         this.STATUS = null;
         this.TECHPACK_NAME = null;
         this.TECHPACK_VERSION = null;
         this.TECHPACK_TYPE = null;
         this.PRODUCT_NUMBER = null;
         this.LOCKEDBY = null;
         this.LOCKDATE = null;
         this.BASEDEFINITION = null;
         this.BASEVERSION = null;
         this.INSTALLDESCRIPTION = null;
         this.UNIVERSENAME = null;
         this.UNIVERSEEXTENSION = null;
         this.ENIQ_LEVEL = null;
         this.LICENSENAME = null;

	}

	public String getVERSIONID() {
		return VERSIONID;
	}

	public void setVERSIONID(String VERSIONID) {
		this.VERSIONID = VERSIONID;
	}

	public String getDESCRIPTION() {
		return DESCRIPTION;
	}

	public void setDESCRIPTION(String DESCRIPTION) {
		this.DESCRIPTION = DESCRIPTION;
	}

	public Long getSTATUS() {
		return STATUS;
	}

	public void setSTATUS(Long STATUS) {
		this.STATUS = STATUS;
	}

	public String getTECHPACK_NAME() {
		return TECHPACK_NAME;
	}

	public void setTECHPACK_NAME(String TECHPACK_NAME) {
		this.TECHPACK_NAME = TECHPACK_NAME;
	}

	public String getTECHPACK_VERSION() {
		return TECHPACK_VERSION;
	}

	public void setTECHPACK_VERSION(String TECHPACK_VERSION) {
		this.TECHPACK_VERSION = TECHPACK_VERSION;
	}

	public String getTECHPACK_TYPE() {
		return TECHPACK_TYPE;
	}

	public void setTECHPACK_TYPE(String TECHPACK_TYPE) {
		this.TECHPACK_TYPE = TECHPACK_TYPE;
	}

	public String getPRODUCT_NUMBER() {
		return PRODUCT_NUMBER;
	}

	public void setPRODUCT_NUMBER(String PRODUCT_NUMBER) {
		this.PRODUCT_NUMBER = PRODUCT_NUMBER;
	}

	public String getLOCKEDBY() {
		return LOCKEDBY;
	}

	public void setLOCKEDBY(String LOCKEDBY) {
		this.LOCKEDBY = LOCKEDBY;
	}

	public Timestamp getLOCKDATE() {
		return LOCKDATE;
	}

	public void setLOCKDATE(Timestamp LOCKDATE) {
		this.LOCKDATE = LOCKDATE;
	}

	public String getBASEDEFINITION() {
		return BASEDEFINITION;
	}

	public void setBASEDEFINITION(String BASEDEFINITION) {
		this.BASEDEFINITION = BASEDEFINITION;
	}

	public String getBASEVERSION() {
		return BASEVERSION;
	}

	public void setBASEVERSION(String BASEVERSION) {
		this.BASEVERSION = BASEVERSION;
	}

	public String getINSTALLDESCRIPTION() {
		return INSTALLDESCRIPTION;
	}

	public void setINSTALLDESCRIPTION(String INSTALLDESCRIPTION) {
		this.INSTALLDESCRIPTION = INSTALLDESCRIPTION;
	}

	public String getUNIVERSENAME() {
		return UNIVERSENAME;
	}

	public void setUNIVERSENAME(String UNIVERSENAME) {
		this.UNIVERSENAME = UNIVERSENAME;
	}

	public String getUNIVERSEEXTENSION() {
		return UNIVERSEEXTENSION;
	}

	public void setUNIVERSEEXTENSION(String UNIVERSEEXTENSION) {
		this.UNIVERSEEXTENSION = UNIVERSEEXTENSION;
	}

	public String getENIQ_LEVEL() {
		return ENIQ_LEVEL;
	}

	public void setENIQ_LEVEL(String ENIQ_LEVEL) {
		this.ENIQ_LEVEL = ENIQ_LEVEL;
	}

	public String getLICENSENAME() {
		return LICENSENAME;
	}

	public void setLICENSENAME(String LICENSENAME) {
		this.LICENSENAME = LICENSENAME;
	}

}
