package com.ericsson.eniq.flssymlink.fls;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/*
 * Class for creating persisted token map for FLS Querying
 * 
 * @author xdhanac
 */
public class PersistedToken implements Serializable{
	/**
	 * serial id
	 */
	private static final long serialVersionUID = 1L;
	
	//tokenMap to be serialized
	Map<String,Map<String, Object>> tokenMap;
	String fileCreationTime;
	ArrayList<Long> ids;

	public PersistedToken(Map<String,Map<String, Object>> tokenMap,String fileCreationTime,ArrayList<Long> ids) {
		super();
		this.tokenMap = tokenMap;
		this.fileCreationTime = fileCreationTime;
		this.ids = ids;
	}

	public Map<String,Map<String, Object>>  getTokenMap() {
		return tokenMap;
	}

	public void setTokenMap(Map<String,Map<String, Object>>  tokenMap) {
		this.tokenMap = tokenMap;
	}
	public String  getFileCreationTime() {
		return fileCreationTime;
	}

	public void setFileCreationTime(String  fileCreationTime) {
		this.fileCreationTime = fileCreationTime;
	}
	public ArrayList<Long>  getIds() {
		return ids;
	}

	public void setIds(ArrayList<Long> ids) {
		this.ids = ids;
	}

	public PersistedToken() {
		super();
	}
	

}
