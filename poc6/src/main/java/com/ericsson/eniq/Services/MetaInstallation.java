package com.ericsson.eniq.Services;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import ssc.rockfactory.RockFactory;

@Component
public class MetaInstallation {
	
	String tpName;
	String tpDirectory;
	private transient RockFactory etlrepRockFactory = null;
	private transient RockFactory dwhrepRockFactory = null;
	
	private static final Logger logger = LogManager.getLogger(MetaInstallation.class);
	
	public MetaInstallation()
	{
		
	}
	public MetaInstallation(String tpName, String tpDirectory)
	{
		this.tpName=tpName;
		this.tpDirectory=tpDirectory;
	}
	public void execute() throws Exception
	{
		
		BufferedReader br=null;
		FileReader fr=null;
		try {
			
			logger.info("Checking connection to database");
			GetDatabaseDetails getdb=new GetDatabaseDetails();
			
			final Map<String, String> databaseConnectionDetails =getdb.getDatabaseConnectionDetails();
			
			
			this.etlrepRockFactory = getdb.createEtlrepRockFactory(databaseConnectionDetails);
			this.dwhrepRockFactory=getdb.createDwhrepRockFactory(databaseConnectionDetails);

			logger.info("Connections to database created.");
			
			
			
			if(this.tpName.startsWith("INTF"))
			{
				fr=new FileReader(this.tpDirectory+"/"+this.tpName+"/interface/Tech_Pack_"+this.tpName+".sql");
				
			}
			else
			{
				fr=new FileReader(this.tpDirectory+"/"+this.tpName+"/sql/Tech_Pack_"+this.tpName+".sql");
			}
			br=new BufferedReader(fr);
			
			String line=br.readLine();
			while(line!=null)
			{
				//logger.info(line);
				
				this.dwhrepRockFactory.executeSql(line);
				line=br.readLine();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}catch(SQLException se)
		{
			 BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			se.printStackTrace();
		}
		finally
		{
			try {
				fr.close();
				br.close();
	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}

}
