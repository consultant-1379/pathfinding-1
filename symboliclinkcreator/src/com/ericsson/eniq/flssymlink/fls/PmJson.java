package com.ericsson.eniq.flssymlink.fls;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PmJson  {
	private String nodeType;
	private String nodeName;
	private String dataType;
	private long id;
	private String fileLocation;
	private String fileCreationTimeInOss;
	public PmJson(String nodeType, String nodeName, String dataType, long id, String fileLocation,
			String fileCreationTimeInOss) {
		super();
		this.nodeType = nodeType;
		this.nodeName = nodeName;
		this.dataType = dataType;
		this.id = id;
		this.fileLocation = fileLocation;
		this.fileCreationTimeInOss = fileCreationTimeInOss;
	}
	public PmJson() {
		super();
	}
	public String getNodeType() {
		return nodeType;
	}
	public void setNodeType(String nodeType) {
		this.nodeType = nodeType;
	}
	public String getNodeName() {
		return nodeName;
	}
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
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
	
}
