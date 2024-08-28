package com.ericsson.eniq.teckpack.tpm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BusyHourImpl {


	String versionID = null;
	String name = null;
	List<Object>supportedTables=null;
	String rankingTable=null;
	Map<String, Object>busyHourPlaceHolders=null; 
	
	public BusyHourImpl(String versionID, String name) {

		this.versionID = versionID;
		this.name = name;
		this.supportedTables = new ArrayList<Object>();
		this.rankingTable = "";
		this.busyHourPlaceHolders = new HashMap<String, Object>();
	}
    /**
     * Populate the objects contents from a xlsDict object 
     * @param xlsDict
     */
	public void getPropertiesFromXls(Map<String, Object> xlsDict) {
		Map<String, Object> bhObjects = (Map<String, Object>) xlsDict.get("BHOBJECT");
		Map<String, Object> topNameTemp = (Map<String, Object>) bhObjects.get(this.name);
		
		for (Map.Entry<String, Object> topEntry : topNameTemp.entrySet()) {
				String Name = (String) topEntry.getKey();
				if (name.equals("BHOBJECT")){
					Map<String, Object> values = (Map<String, Object>) topEntry.getValue();
					for (Map.Entry<String, Object> val : values.entrySet()) {
						String entry = val.getKey();
						BusyHourPlaceHolderImpl bhpImpl = new BusyHourPlaceHolderImpl(this.versionID, entry, Name);
						bhpImpl.getPropertiesFromXls(xlsDict);
						this.busyHourPlaceHolders.put(Name, bhpImpl);
				}
		}
		}
	}
}
	
