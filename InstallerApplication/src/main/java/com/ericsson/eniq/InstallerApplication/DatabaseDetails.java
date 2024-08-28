package com.ericsson.eniq.InstallerApplication;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.ericsson.eniq.Services.DatabaseCache;
import com.ericsson.eniq.Services.GetDatabaseDetails;

@Component
public class DatabaseDetails  implements ApplicationRunner {
	
	private static final Logger logger = LogManager.getLogger(DatabaseDetails.class);
	
	private static HashMap<String,String> dbDetails=new HashMap<String,String>();
	
	@Autowired
	private Environment env;
	

	public HashMap<String, String> getDbDetails() {
		return dbDetails;
	}


	public void setDbDetails(HashMap<String, String> dbDetails) {
		DatabaseDetails.dbDetails = dbDetails;
	}


	private void init()
	{
		//logger.info("Hello World from databasedetails");
		//logger.info(env.getProperty("db.repdb.url"));
		
		//DatabaseCache.initialise(env.getProperty("db.repdb.etlrep.user"), env.getProperty("db.repdb.etlrep.pass"), env.getProperty("db.driver"), env.getProperty("db.repdb.url"));
		
	
		dbDetails.put("repdbURL", env.getProperty("db.repdb.url"));
		dbDetails.put("dwhdbURL", env.getProperty("db.repdb.url"));
		dbDetails.put("driver", env.getProperty("db.driver"));
		dbDetails.put("etlrepUser", env.getProperty("db.repdb.etlrep.user"));
		dbDetails.put("etlrepPass", env.getProperty("db.repdb.etlrep.pass"));
		dbDetails.put("dwhrepUser", env.getProperty("db.repdb.dwhrep.user"));
		dbDetails.put("dwhrepPass", env.getProperty("db.repdb.dwhrep.pass"));
		dbDetails.put("dwhdbUser", env.getProperty("db.dwhdb.user"));
		dbDetails.put("dwhdbPass", env.getProperty("db.dwhdb.pass"));
		/*DatabaseCache dc=new DatabaseCache();
		dc.setDbInformation(dbDetails);
		
		logger.info(dc.getDbInformation());
		logger.info(dbDetails.get("repdbURL"));*/
	}


	@Override
	public void run(ApplicationArguments args) throws Exception {
		// TODO Auto-generated method stub
		init();
	}

}
