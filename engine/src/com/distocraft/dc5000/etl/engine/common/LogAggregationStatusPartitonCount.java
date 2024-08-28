package com.distocraft.dc5000.etl.engine.common;

import java.io.File;
import java.io.FileInputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;


import com.ericsson.eniq.repository.AsciiCrypter;

/**
 * Used to get the Partition size of Log_aggregationStatus table from DWHType
 * 
 * @author eninkar
 */
public class LogAggregationStatusPartitonCount {

	private String DbUrl = null;
	private String DBUserName = null;
	private String DBPassword = null;
	private String DBDriverName = null;

	/**
	 * Setting Database connection related setting
	 */
	private void getServerProperties() {

		String etlcServerPropertiesFile;

		try {

			etlcServerPropertiesFile = System.getProperty("CONF_DIR");

			if (etlcServerPropertiesFile == null) {
				// System.out.println("System property CONF_DIR not defined. Using default");
				etlcServerPropertiesFile = "/eniq/sw/conf";
			}
			if (!etlcServerPropertiesFile.endsWith(File.separator)) {
				etlcServerPropertiesFile += File.separator;
			}

			etlcServerPropertiesFile += "ETLCServer.properties";

			final FileInputStream streamProperties = new FileInputStream(etlcServerPropertiesFile);
			final java.util.Properties appProps = new java.util.Properties();
			appProps.load(streamProperties);

			this.DbUrl = appProps.getProperty("ENGINE_DB_URL");
			this.DBUserName = appProps.getProperty("ENGINE_DB_USERNAME");
			
			
			String decryptedPassword=AsciiCrypter.getInstance().decrypt(appProps.getProperty("ENGINE_DB_PASSWORD"));
			this.DBPassword = decryptedPassword;
			
			this.DBDriverName = appProps.getProperty("ENGINE_DB_DRIVERNAME");

			
		} catch (Exception e) {

		}
	}

	/**
	 * To print the partition Size of Log_AggregationStatus type
	 */
	private void getCount() {

		int count = 0;

		getServerProperties();

		RockFactory dwhreprock = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {

			dwhreprock = connectDWHRep();
			
			if (dwhreprock == null) {
				throw new RockException("Database dwhrep is not defined in Meta_databases?!");
			}

			stmt = dwhreprock.getConnection().createStatement();

			final String sql = "select partitioncount from dwhrep.DWHType where typename='Log_AggregationStatus'";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				count = rs.getInt(1);
			}

			if (count == 0) {

				throw new Exception("No Partition Defined");
			}

			System.out.println(count);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (Exception e) {
			}
			try {
				stmt.close();
			} catch (Exception e) {
			}
			try {
				dwhreprock.getConnection().close();
			} catch (Exception e) {
			}
		}
	}

	private RockFactory connectDWHRep() throws RockException,SQLException {
		RockFactory etlreprock = null;
		RockFactory dwhreprock = null;
		
		try {
			etlreprock = new RockFactory(DbUrl, DBUserName, DBPassword, DBDriverName, "LogAggr", false);

			final Meta_databases md_cond = new Meta_databases(etlreprock);
			final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlreprock, md_cond);

			final List<Meta_databases> dbs = md_fact.get();

			for (Meta_databases db : dbs) {

				if (db.getConnection_name().equalsIgnoreCase("dwhrep") && db.getType_name().equals("USER")) {
					dwhreprock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
							db.getDriver_name(), "LogAggr", true);

				}

			} // for each Meta_databases

		} finally {
			try {
				etlreprock.getConnection().close();
			} catch (Exception e) {
			}
		}

		return dwhreprock;
	}

	public static void main(final String[] arg) {
		final LogAggregationStatusPartitonCount dwhPartitionCountTest = new LogAggregationStatusPartitonCount();
		dwhPartitionCountTest.getCount();
			}

}
