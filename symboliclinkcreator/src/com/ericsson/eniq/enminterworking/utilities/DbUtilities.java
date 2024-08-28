package com.ericsson.eniq.enminterworking.utilities;


import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.repository.ETLCServerProperties;

public class DbUtilities {

	
	private ETLCServerProperties etlcProp ;
	private RockFactory etlrep = null;
	private RockFactory rf = null;

	
	// dbType can be either dwh or dwhrep
	public RockFactory connectTodb(RockFactory etlrep, String dbType) {
    // log = Logger.getLogger("symboliclinkcreator.nat");
    // log.finest("Etlrep    "+etlrep+"   dbtypr"+dbType);
		
		Meta_databases db_prop;
		Meta_databases where_obj;
		Meta_databasesFactory md_fact;
		List<Meta_databases> dbs;

		try {			
			where_obj = new Meta_databases(etlrep);
			
			where_obj.setConnection_name(dbType);
			where_obj.setType_name("USER");
			
			md_fact = new Meta_databasesFactory(etlrep, where_obj);
			dbs = md_fact.get();
			
			if (dbs.size() <= 0) {
				throw new RockException("Could not extract repDB log-on details.");
			}
			
			db_prop = dbs.get(0);
			if (dbType.equals("dwh")) {
				etlcProp.put("dwhdb_username", db_prop.getUsername());
				etlcProp.put("dwhdb_url", db_prop.getConnection_string());
				etlcProp.put("dwhdb_pwd", db_prop.getPassword());
				etlcProp.put("dwhdb_drivername", db_prop.getDriver_name());
			} else {
				etlcProp.put("dwhrepdb_username", db_prop.getUsername());
				etlcProp.put("dwhrepdb_url", db_prop.getConnection_string());
				etlcProp.put("dwhrepdb_pwd", db_prop.getPassword());
				etlcProp.put("dwhrepdb_drivername", db_prop.getDriver_name());
			}
		} catch (SQLException | RockException e) {
//			log.warning("Exception at connectTodb "+e.getMessage());
		}

		try {
			if (dbType.equals("dwh")) {
				//log.finest("dwhdb_url property value:"+etlcProp.getProperty("dwhdb_url"));
				rf = new RockFactory(etlcProp.getProperty("dwhdb_url"), etlcProp.getProperty("dwhdb_username"),
						etlcProp.getProperty("dwhdb_pwd"), etlcProp.getProperty("dwhdb_drivername"), "NodeAssignment",
						true);
			} else {
				rf = new RockFactory(etlcProp.getProperty("dwhrepdb_url"), etlcProp.getProperty("dwhrepdb_username"),
						etlcProp.getProperty("dwhrepdb_pwd"), etlcProp.getProperty("dwhrepdb_drivername"),
						"NodeAssignment_" + dbType, true);
			}
//			log.finest("Connection Successful");

		} catch (SQLException | RockException e) {
//			log.warning("Error in connecting to DWHDB in connectTodb method ");
		}
		finally{
			try {
				if (etlrep != null)
					etlrep.getConnection().close();
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}

		return rf;

	}

	public RockFactory connectToEtlrep() {
//		log = Logger.getLogger("symboliclinkcreator.nat");
		try {
			etlcProp = new ETLCServerProperties("/eniq/sw/conf/ETLCServer.properties");
//			log.warning("connect to etlrep");
		} catch (IOException e) {
			// e.printStackTrace();
//			log.warning("Cannot find the ETLCServerProperties file in connectToEtlrep method ");
		}
		try {
			etlrep = new RockFactory(etlcProp.getProperty(ETLCServerProperties.DBURL),
					etlcProp.getProperty(ETLCServerProperties.DBUSERNAME),
					etlcProp.getProperty(ETLCServerProperties.DBPASSWORD),
					etlcProp.getProperty(ETLCServerProperties.DBDRIVERNAME), "NodeAssignment", true);
		} catch (SQLException | RockException e) {
//			log.warning("some exception while connecting to ETLREP in connectToEtlrep method "+e.getMessage());
			
		}
//		log.finest("returning etlrep object  "+etlrep);
		return etlrep;
	}
	
	

}
