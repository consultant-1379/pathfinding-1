package com.ericsson.eniq.avro.entity;

public class DataFormat {
	private String typeId;

	public DataFormat(String typeId) {
		super();
		this.typeId = typeId;
	}

	public String getTypeId() {
		return typeId;
	}

	public void setTypeId(String typeId) {
		this.typeId = typeId;
	}

}
