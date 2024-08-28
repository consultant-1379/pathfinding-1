package com.ericsson.eniq.avro.entity;

public class DefaultTags {
	private String dataFormatId;
	private String tagId;
	private String dataName;
	private String dataId;
	private String dataType;
	private int dataSize;

	public DefaultTags(String dataFormatId, String tagId, String dataName, String dataId, String dataType,
			int dataSize) {
		super();
		this.dataFormatId = dataFormatId;
		this.tagId = tagId;
		this.dataName = dataName;
		this.dataId = dataId;
		this.dataType = dataType;
		this.dataSize = dataSize;
	}

	public String getDataFormatId() {
		return dataFormatId;
	}

	public void setDataFormatId(String dataFormatId) {
		this.dataFormatId = dataFormatId;
	}

	public String getTagId() {
		return tagId;
	}

	public void setTagId(String tagId) {
		this.tagId = tagId;
	}

	public String getDataName() {
		return dataName;
	}

	public void setDataName(String dataName) {
		this.dataName = dataName;
	}

	public String getDataId() {
		return dataId;
	}

	public void setDataId(String dataId) {
		this.dataId = dataId;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public int getDataSize() {
		return dataSize;
	}

	public void setDataSize(int dataSize) {
		this.dataSize = dataSize;
	}
}
