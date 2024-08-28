package com.ericsson.eniq.teckpack.tpm;

import java.util.HashMap;
import java.util.Map;

public class ExternalStatementImpl {


	String versionID = null;
	String name = null;
	Map<String, Object> properties = null;
	

	public ExternalStatementImpl(String versionID, String name) {
		this.versionID = versionID;
		this.name = name;
		this.properties =  new HashMap<String, Object>();
	}
	 /**
     * Populate the objects contents from a xlsDict object 
     * @param xlsDict
     */
	public void getPropertiesFromXls(Map<String, Object> xlsDict) {
		Map<String, Object> externalStatements = (Map<String, Object>) xlsDict.get("ExternalStatements");
		Map<String, Object> topNameTemp = (Map<String, Object>) externalStatements.get(this.name);
		for (Map.Entry<String, Object> topEntry : topNameTemp.entrySet()) {
			String Name = (String) topEntry.getKey();
			this.properties.put(Name, topEntry.getValue());
		}
	}
}	
	