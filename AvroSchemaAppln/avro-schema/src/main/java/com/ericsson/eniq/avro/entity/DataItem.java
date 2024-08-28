package com.ericsson.eniq.avro.entity;

public class DataItem {
	private String dataName;
	private String dataType;

	public DataItem(String dataName, String dataType) {
		this.dataName = dataName;
		this.dataType = dataType;
	}

	public String getDataName() {
		return dataName;
	}

	public void setDataName(String dataName) {
		this.dataName = dataName;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

}
