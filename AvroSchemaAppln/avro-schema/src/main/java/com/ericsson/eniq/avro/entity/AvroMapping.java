package com.ericsson.eniq.avro.entity;

public class AvroMapping {
	private String versionId;
	private String moClass;
	private String avroName;
	private String iqTable;
	private String dbColumn;
	private String avroColumn;
	private String dataFormat;
	private String fieldDataType;
	private String avroDataType;
	private int dataSize;
	public AvroMapping(String versionId, String moClass, String avroName, String iqTable, String dbColumn,
			String avroColumn, String dataFormat, String fieldDataType, String avroDataType, int dataSize) {
		super();
		this.versionId = versionId;
		this.moClass = moClass;
		this.avroName = avroName;
		this.iqTable = iqTable;
		this.dbColumn = dbColumn;
		this.avroColumn = avroColumn;
		this.dataFormat = dataFormat;
		this.fieldDataType = fieldDataType;
		this.avroDataType = avroDataType;
		this.dataSize = dataSize;
	}
	public AvroMapping() {
	}
	public String getVersionId() {
		return versionId;
	}
	public void setVersionId(String versionId) {
		this.versionId = versionId;
	}
	public String getMoClass() {
		return moClass;
	}
	public void setMoClass(String moClass) {
		this.moClass = moClass;
	}
	public String getAvroName() {
		return avroName;
	}
	public void setAvroName(String avroName) {
		this.avroName = avroName;
	}
	public String getIqTable() {
		return iqTable;
	}
	public void setIqTable(String iqTable) {
		this.iqTable = iqTable;
	}
	public String getDbColumn() {
		return dbColumn;
	}
	public void setDbColumn(String dbColumn) {
		this.dbColumn = dbColumn;
	}
	public String getAvroColumn() {
		return avroColumn;
	}
	public void setAvroColumn(String avroColumn) {
		this.avroColumn = avroColumn;
	}
	public String getDataFormat() {
		return dataFormat;
	}
	public void setDataFormat(String dataFormat) {
		this.dataFormat = dataFormat;
	}
	public String getFieldDataType() {
		return fieldDataType;
	}
	public void setFieldDataType(String fieldDataType) {
		this.fieldDataType = fieldDataType;
	}
	public String getAvroDataType() {
		return avroDataType;
	}
	public void setAvroDataType(String avroDataType) {
		this.avroDataType = avroDataType;
	}
	public int getDataSize() {
		return dataSize;
	}
	public void setDataSize(int dataSize) {
		this.dataSize = dataSize;
	}
	

}
