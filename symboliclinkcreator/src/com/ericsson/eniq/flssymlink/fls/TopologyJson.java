package com.ericsson.eniq.flssymlink.fls;

public class TopologyJson{
	private String nodeName;
	private String nodeType;
	private String dataType;
	private String fileLocation;
	private String fileCreationTimeInOss;
	private Long id;
	
	public TopologyJson() {
		super();
	}
	public TopologyJson(String nodeName, String nodeType, String dataType, String fileLocation,
			String fileCreationTimeInOss, Long id) {
		super();
		this.nodeName = nodeName;
		this.nodeType = nodeType;
		this.dataType = dataType;
		this.fileLocation = fileLocation;
		this.fileCreationTimeInOss = fileCreationTimeInOss;
		this.id = id;
	}
	public String getNodeName() {
		return nodeName;
	}
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	public String getNodeType() {
		return nodeType;
	}
	public void setNodeType(String nodeType) {
		this.nodeType = nodeType;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getFileLocation() {
		return fileLocation;
	}
	public void setFileLocation(String fileLocation) {
		this.fileLocation = fileLocation;
	}
	public String getFileCreationTimeInOss() {
		return fileCreationTimeInOss;
	}
	public void setFileCreationTimeInOss(String fileCreationTimeInOss) {
		this.fileCreationTimeInOss = fileCreationTimeInOss;
	}
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	
}
