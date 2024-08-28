package com.ericsson.eniq.Services;


import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

public class DatabaseCache {
	
	
	private static DatabaseCache dbCache=null;
	
	private Map<String,String> dbInformation =new HashMap<>();
	
	private static class InstanceHolder {
		  static final DatabaseCache dbc = new DatabaseCache();
	}
	
	public static void initialise(String etlrepUser,String etlrepPass,String driver,String repdbURL)
	{
		dbCache=new DatabaseCache();
		/*dbCache.etlrepUser=etlrepUser;
		dbCache.etlrepPass=etlrepPass;
		dbCache.driver=driver;
		dbCache.repdbURL=repdbURL;*/		
		dbCache.createCache(etlrepUser,etlrepPass,driver,repdbURL);
	}
	
	
	public void createCache(String etlrepUser,String etlrepPass, String driver, String repdbURL)
	{
		System.out.println(etlrepUser);
		
		dbInformation.put("etlrepUser", etlrepUser);
		dbInformation.put("etlrepPass", etlrepPass);
		dbInformation.put("repdbURL",repdbURL);
		dbInformation.put("driver", driver);
	}
	

	public static DatabaseCache getDbCache() {
		return InstanceHolder.dbc;
	}


	public String getDriverDetails(String driver) {
		return dbInformation.get("driver");
	}



	
}
