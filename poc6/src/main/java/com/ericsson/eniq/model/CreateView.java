package com.ericsson.eniq.model;

public class CreateView {
	
	private String baseTableName;
	private String partitions;
	private String part;
	public String getBaseTableName() {
		return baseTableName;
	}
	public void setBaseTableName(String baseTableName) {
		this.baseTableName = baseTableName;
	}
	public String getPartitions() {
		return partitions;
	}
	public void setPartitions(String partitions) {
		this.partitions = partitions;
	}
	public String getPart() {
		return part;
	}
	public void setPart(String part) {
		this.part = part;
	}
	
	

}
