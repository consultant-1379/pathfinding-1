/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2019 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.repository.dbusers;

import java.io.File;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.repository.DBUsersGet;
import com.ericsson.eniq.repository.ETLCServerProperties;
import com.ericsson.eniq.repository.UpdateDBUsers;

import ssc.rockfactory.RockFactory;

/**
 *
 * @author esuramo
 *
 */
public class UpdateDBPass {

	private UpdateDBPass() {
	}

	public static void main(final String[] args) {
		int affectedRows = 0;
		String requestedUser = null;
		String oldPassword = null;
		String newPassword = null;
		RockFactory etlRep = null;

		if (args == null || (args.length != 1 && args.length != 3)) {
			System.out.println("Invalid parameters.");
			System.exit(1);
		}

		if (args[0].equalsIgnoreCase("ALL")) {
			////Encrypt all the un-encrypted passwords in Meta_Databases and exit
			try {
				Meta_databases whereMdb;
				Meta_databases newMdb;
				etlRep = getRockFactoryObj();
				whereMdb = new Meta_databases(etlRep);
				whereMdb.setEncryption_flag("N");
				final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlRep, whereMdb);
				final Vector<Meta_databases> dbs = md_fact.get();
				System.out.println("Encrypting all the un-encrypted passwords in Meta_Databases Table.");
				for (Meta_databases md : dbs) {
					newPassword = Base64.getEncoder().encodeToString(md.getPassword().trim().getBytes());
					newMdb = new Meta_databases(etlRep);
					newMdb.setPassword(newPassword);
					newMdb.setEncryption_flag("Y");
					newMdb.setDecryptionRequired(false);
					Meta_databases whereObject = new Meta_databases(etlRep);
					whereObject.setUsername(md.getUsername());
					whereObject.setPassword(md.getPassword());
					affectedRows = affectedRows + newMdb.updateDB(false, whereObject);
				}
				
				System.out.println(affectedRows + " rows affected.");
			} catch (Exception e) {
				System.out.println("Exception occurred while encrypting all the un-encrypyed passwords in Meta_Databases:" + e);
				e.printStackTrace();
				System.exit(3);
			} finally{
				try {
					if (etlRep != null)
						etlRep.getConnection().close();
				} catch (SQLException e) {
					e.printStackTrace();
				}	
			} 
		} else if (args[0].equalsIgnoreCase("ByPass")) {
			////Encrypt the new password and update it in the Meta_Databases table and then exit
			requestedUser = args[1];
			newPassword = Base64.getEncoder().encodeToString(args[2].trim().getBytes());
			try {
				Meta_databases whereObject;
				Meta_databases newMdb;
				etlRep = getRockFactoryObj();
				newMdb = new Meta_databases(etlRep);
				newMdb.setPassword(newPassword);
				newMdb.setEncryption_flag("Y");
				newMdb.setDecryptionRequired(false);
				whereObject = new Meta_databases(etlRep);
				whereObject.setUsername(requestedUser);
				System.out.println("Updating password for: " + requestedUser);
				affectedRows = newMdb.updateDB(false, whereObject);
				System.out.println(affectedRows + " rows affected.");
			} catch (Exception e) {
				System.out.println("Exception occurred while updating password for: " + requestedUser +":" + e);
				e.printStackTrace();
				System.exit(3);
			} finally{
				try {
					if (etlRep != null)
						etlRep.getConnection().close();
				} catch (SQLException e) {
					e.printStackTrace();
				}	
			} 
		} else {
			requestedUser = args[0];
			oldPassword = args[1];
			newPassword = args[2];

			try {
				final List<Meta_databases> databases = DBUsersGet.getMetaDatabases("ALL", "ALL");
				for (Meta_databases m : databases) {
					if (oldPassword.equals(m.getPassword()) && requestedUser.equalsIgnoreCase(m.getUsername())) {
						System.out.println("Updating Password for: " + requestedUser);
						affectedRows = UpdateDBUsers.updateMetaDatabases(requestedUser, oldPassword, newPassword);
						System.out.println(affectedRows + " rows affected.");
						System.exit(0);
					}
				}
				System.out.println(
						"The username '"+requestedUser+"' and password is incorrect and doesnot exist in the database.");
				System.exit(2);
			} catch (Exception e) {
				System.out.println("Exception occurred while updating password for: " + requestedUser +":" + e);
				e.printStackTrace();
				System.exit(3);
			}  
		}
		
		System.exit(0);
	}
	
	public static RockFactory getRockFactoryObj() throws Exception {
		final String confDir = System.getProperty("CONF_DIR", "/eniq/sw/conf");
		Properties props = new ETLCServerProperties(confDir + File.separator + "ETLCServer.properties");;
		return new RockFactory(props.getProperty(ETLCServerProperties.DBURL),
				props.getProperty(ETLCServerProperties.DBUSERNAME),
				props.getProperty(ETLCServerProperties.DBPASSWORD),
				props.getProperty(ETLCServerProperties.DBDRIVERNAME), "UpdateDBPassword", false);
	}
}
