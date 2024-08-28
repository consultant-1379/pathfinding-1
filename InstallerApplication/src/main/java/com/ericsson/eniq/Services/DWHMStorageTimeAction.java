package com.ericsson.eniq.Services;


import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Map;

import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;


public class DWHMStorageTimeAction{

	private RockFactory etlreprock = null;
	private RockFactory dwhreprock = null;
	private RockFactory dwhrock = null;
	private RockFactory dbadwhrock = null;

	private final String tpName;
	private final String tpDirectory;

	private static final Logger logger = Logger.getLogger("DWHMStorageTimeAction");
	public DWHMStorageTimeAction(String tpName,String tpDirectory)
	{
		this.tpDirectory=tpDirectory;
		this.tpName=tpName;
	}
	
	public void execute()
	{
		
		try
		{
			GetDatabaseDetails getdb=new GetDatabaseDetails();
			
			final Map<String, String> databaseConnectionDetails =getdb.getDatabaseConnectionDetails();
			
			
			etlreprock = getdb.createEtlrepRockFactory(databaseConnectionDetails);

			
			this.dwhreprock=getdb.createDwhrepRockFactory(etlreprock);
			this.dwhrock=getdb.createDwhdbRockFactory(etlreprock);
			this.dbadwhrock=getdb.createDBADwhdbRockFactory(etlreprock);
			
			logger.info("Connections to database created.");
			VersionUpdateAction vua=new VersionUpdateAction(this.dwhreprock,etlreprock,this.dwhrock,this.tpName, logger);
			vua.execute(this.tpName);
            etlreprock = getdb.createEtlrepRockFactory(databaseConnectionDetails);

			
			this.dwhreprock=getdb.createDwhrepRockFactory(etlreprock);
			this.dwhrock=getdb.createDwhdbRockFactory(etlreprock);
			this.dbadwhrock=getdb.createDBADwhdbRockFactory(etlreprock);
			StorageTimeAction sta=new StorageTimeAction( this.dwhreprock, etlreprock, this.dwhrock, this.dbadwhrock, this.tpName,this.tpDirectory, logger);
			
		}catch (Exception e) {
		e.printStackTrace();
		try {
			throw new Exception("Unexpected failure");
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	} finally {
		try {
			if (etlreprock != null) {
				etlreprock.getConnection().close();
			}
			if (dwhreprock != null) {
				dwhreprock.getConnection().close();
			}
			if (dwhrock != null) {
				dwhrock.getConnection().close();
			}
			if (dbadwhrock != null) {
				dbadwhrock.getConnection().close();
			}
		} catch (final SQLException sqle) {
			System.out.print("Connection cleanup error - " + sqle.toString());
		}
		dwhreprock = null;
		etlreprock = null;
		dwhrock=null;
		dbadwhrock=null;
	}
		
	}
	

}

