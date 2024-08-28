package com.ericsson.eniq.teckpack.tpm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BusyHourPlaceHolderImpl {

	  	String name = null;
		String versionID = null;
		Map<String, Object> properties = null;
		List<Object>supportedTables=null;
		String BHOBjectName=null;
		List<Object> BHSourceTables=null;
		Map<String, Object> rankKeys = null;
		
		
		public BusyHourPlaceHolderImpl(String versionID, 
				 String bHOBjectName,String bhPlaceHolder) {

				this.name = name;
				this.versionID = versionID;
				this.properties =  new HashMap<String, Object>();
				this.supportedTables = supportedTables;
				this.BHOBjectName = bHOBjectName;
				this.BHSourceTables =  new ArrayList<Object>();
				this.rankKeys =  new HashMap<String, Object>();
			}


		public void getPropertiesFromXls(Map<String, Object> xlsDict) {
			// TODO Auto-generated method stub
			
		}
}
