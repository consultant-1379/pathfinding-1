package com.ericsson.eniq.repository;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;

public abstract class DBUsersGet {

	public static List<Meta_databases> getMetaDatabases(final String user, final String connection) {

		final List <Meta_databases> databases = new ArrayList<Meta_databases> ();
		if (user == null || connection == null) {
			return databases;
		}
		
		String engUser = null;
		String engPass = null;
		String url = null;
		String driver = null;
		Properties props = null;
		
		final String confDir = System.getProperty("CONF_DIR", "/eniq/sw/conf");
		try {
			props = new ETLCServerProperties(confDir + File.separator + "ETLCServer.properties");
		} catch (Exception e) {
      e.printStackTrace();
			return databases;
		}
		engUser = props.getProperty(ETLCServerProperties.DBUSERNAME);
		engPass = props.getProperty(ETLCServerProperties.DBPASSWORD);
		url = props.getProperty(ETLCServerProperties.DBURL);
		driver = props.getProperty(ETLCServerProperties.DBDRIVERNAME);
		RockFactory etlRep=null;
		
		try {
			etlRep = new RockFactory(url, engUser, engPass, driver, "DBUsersScript", true);
			final Meta_databases whereMdb = new Meta_databases(etlRep);
			if (!user.equalsIgnoreCase("ALL") && !connection.equalsIgnoreCase("ALL") ){
			  whereMdb.setUsername(user);
			  whereMdb.setConnection_name(connection);
			}
			final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlRep,whereMdb);
			final Vector<Meta_databases> dbs = md_fact.get();
			for (Meta_databases md : dbs){
				databases.add(md);
			}
		} catch (SQLException e) {
      if(e.getNextException() == null){
			  e.printStackTrace();
      } else {
        e.getNextException().printStackTrace();
      }
		} catch (RockException e) {
      e.printStackTrace();
    }finally{
    	try {
    		if(etlRep!=null)
    			etlRep.getConnection().close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    return databases;
	}
	
}
