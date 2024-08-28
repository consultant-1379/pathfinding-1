package com.ericsson.eniq.Services;

public class InputForm {
	private String tpDirectory;
	private String tpName;
	private String stageFile;
	private String ossId;
	public String getOssId() {
		return ossId;
	}
	public void setOssId(String ossId) {
		this.ossId = ossId;
	}
	public String getTpName() {
		return tpName;
	}
	public String getStageFile() {
		return stageFile;
	}
	public void setStageFile(String stageFile) {
		this.stageFile = stageFile;
	}
	public void setTpName(String tpName) {
		this.tpName = tpName;
	}
	public String getTpDirectory() {
		return tpDirectory;
	}
	public void setTpDirectory(String tpDirectory) {
		this.tpDirectory = tpDirectory;
	}
	@Override
	public String toString() {
		return "InputForm [tpDirectory=" + tpDirectory + ", tpName=" + tpName + ", stageFile=" + stageFile + ", ossId="
				+ ossId + "]";
	}

	
	
		
}
