package com.ericsson.eniq.Services;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.Controller.InstallerController;
import com.ericsson.eniq.InstallerApplication.DatabaseDetails;
import com.ericsson.eniq.repository.ETLCServerProperties;

import ssc.rockfactory.RockFactory;

@Component
public class GetDatabaseDetails {

	private static final Logger logger = LogManager.getLogger(GetDatabaseDetails.class);

	public GetDatabaseDetails() {

	}

	/**
	 * @return
	 * @throws Exception
	 */
	public Map<String, String> getDatabaseConnectionDetails() throws Exception {
		// DatabaseCache dbInfo=new DatabaseCache();

		// logger.info(dbInfo.toString());

		DatabaseDetails dbInfo = new DatabaseDetails();
		final Map<String, String> dbConnDetails = dbInfo.getDbDetails();
		/*
		 * ETLCServerProperties props; try { props = new ETLCServerProperties();
		 * 
		 * } catch (IOException e) { throw new
		 * Exception("Could not read ETLCServer.properties", e); }
		 * 
		 * final Map<String, String> dbConnDetails =
		 * props.getDatabaseConnectionDetails();
		 */
		return dbConnDetails;
	}

	public RockFactory createEtlrepRockFactory(final Map<String, String> databaseConnectionDetails) throws Exception {

		// logger.info("Inside createETLrepRockFactory");
		// logger.info(databaseConnectionDetails);
		/*
		 * final String databaseUsername =
		 * databaseConnectionDetails.get("etlrepDatabaseUsername"); final String
		 * databasePassword = databaseConnectionDetails.get("etlrepDatabasePassword");
		 * final String databaseUrl =
		 * databaseConnectionDetails.get("etlrepDatabaseUrl"); final String
		 * databaseDriver = databaseConnectionDetails.get("etlrepDatabaseDriver");
		 */

		final String databaseUsername = databaseConnectionDetails.get("etlrepUser");
		final String databasePassword = databaseConnectionDetails.get("etlrepPass");
		final String databaseUrl = databaseConnectionDetails.get("repdbURL");
		final String databaseDriver = databaseConnectionDetails.get("driver");

		try {

			return new RockFactory(databaseUrl, databaseUsername, databasePassword, databaseDriver, "TPInstall", true);
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Unable to initialize database connection.", e);
		}
	}

	/*
	 * public RockFactory createDwhrepRockFactory(RockFactory etlrepRockFactory)
	 * throws Exception { try { //logger.info("Inside createDWHrepRockFactory");
	 * final Meta_databases whereMetaDatabases = new
	 * Meta_databases(etlrepRockFactory);
	 * whereMetaDatabases.setConnection_name("dwhrep");
	 * whereMetaDatabases.setType_name("USER"); final Meta_databasesFactory
	 * metaDatabasesFactory = new Meta_databasesFactory(etlrepRockFactory,
	 * whereMetaDatabases); final Vector<Meta_databases> metaDatabases =
	 * metaDatabasesFactory.get();
	 * 
	 * if ((metaDatabases != null) && (metaDatabases.size() == 1)) { final
	 * Meta_databases targetMetaDatabase = metaDatabases.get(0); return new
	 * RockFactory(targetMetaDatabase.getConnection_string(),
	 * targetMetaDatabase.getUsername(), targetMetaDatabase.getPassword(),
	 * etlrepRockFactory.getDriverName(), "TPInstall", true); } else { throw new
	 * Exception(
	 * "Unable to connect metadata (No dwhrep or multiple dwhreps defined in Meta_databases)"
	 * ); } } catch (final Exception e) { e.printStackTrace(); throw new
	 * Exception("Creating database connection to dwhrep failed.", e); }
	 * 
	 * }
	 */

	public RockFactory createDwhrepRockFactory(final Map<String, String> databaseConnectionDetails) throws Exception {

		final String databaseUsername = databaseConnectionDetails.get("dwhrepUser");
		final String databasePassword = databaseConnectionDetails.get("dwhrepPass");
		final String databaseUrl = databaseConnectionDetails.get("repdbURL");
		final String databaseDriver = databaseConnectionDetails.get("driver");

		try {

			return new RockFactory(databaseUrl, databaseUsername, databasePassword, databaseDriver, "TPInstall", true);
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Unable to initialize database connection.", e);
		}
	}

	public RockFactory createDwhdbRockFactory(final Map<String, String> databaseConnectionDetails) throws Exception {

		final String databaseUsername = databaseConnectionDetails.get("dwhdbUser");
		final String databasePassword = databaseConnectionDetails.get("dwhdbPass");
		final String databaseUrl = databaseConnectionDetails.get("dwhdbURL");
		final String databaseDriver = databaseConnectionDetails.get("driver");

		try {

			return new RockFactory(databaseUrl, databaseUsername, databasePassword, databaseDriver, "TPInstall", true);
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Unable to initialize database connection.", e);
		}

	}

//	public RockFactory createDwhdbRockFactory(RockFactory etlrepRockFactory) throws Exception {
//		try {
//			//logger.info("Inside createDWHrepRockFactory");
//			final Meta_databases whereMetaDatabases = new Meta_databases(etlrepRockFactory);
//			whereMetaDatabases.setConnection_name("dwh");
//			whereMetaDatabases.setType_name("USER");
//			final Meta_databasesFactory metaDatabasesFactory = new Meta_databasesFactory(etlrepRockFactory,
//					whereMetaDatabases);
//			final Vector<Meta_databases> metaDatabases = metaDatabasesFactory.get();
//
//			if ((metaDatabases != null) && (metaDatabases.size() == 1)) {
//				final Meta_databases targetMetaDatabase = metaDatabases.get(0);
//				return new RockFactory(targetMetaDatabase.getConnection_string(),
//						targetMetaDatabase.getUsername(), targetMetaDatabase.getPassword(),
//						etlrepRockFactory.getDriverName(), "TPInstall", true);
//			} else {
//				throw new Exception(
//						"Unable to connect metadata (No dwhrep or multiple dwhreps defined in Meta_databases)");
//			}
//		} catch (final Exception e) {
//			e.printStackTrace();
//			throw new Exception("Creating database connection to dwhrep failed.", e);
//		}
//	}

	public RockFactory createDBADwhdbRockFactory(final Map<String, String> databaseConnectionDetails) throws Exception {

		final String databaseUsername = databaseConnectionDetails.get("dwhDBAdbUser");
		final String databasePassword = databaseConnectionDetails.get("dwhDBAdbPass");
		final String databaseUrl = databaseConnectionDetails.get("dwhDBAdbURL");
		final String databaseDriver = databaseConnectionDetails.get("driver");

		try {

			return new RockFactory(databaseUrl, databaseUsername, databasePassword, databaseDriver, "TPInstall", true);
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Unable to initialize database connection.", e);
		}
	}

//	public RockFactory createDBADwhdbRockFactory(RockFactory etlrepRockFactory) throws Exception {
//		try {
//			//logger.info("Inside createDWHrepRockFactory");
//			final Meta_databases whereMetaDatabases = new Meta_databases(etlrepRockFactory);
//			whereMetaDatabases.setConnection_name("dwh");
//			whereMetaDatabases.setType_name("DBA");
//			final Meta_databasesFactory metaDatabasesFactory = new Meta_databasesFactory(etlrepRockFactory,
//					whereMetaDatabases);
//			final Vector<Meta_databases> metaDatabases = metaDatabasesFactory.get();
//
//			if ((metaDatabases != null) && (metaDatabases.size() == 1)) {
//				final Meta_databases targetMetaDatabase = metaDatabases.get(0);
//				return new RockFactory(targetMetaDatabase.getConnection_string(),
//						targetMetaDatabase.getUsername(), targetMetaDatabase.getPassword(),
//						etlrepRockFactory.getDriverName(), "TPInstall", true);
//			} else {
//				throw new Exception(
//						"Unable to connect metadata (No dwhrep or multiple dwhreps defined in Meta_databases)");
//			}
//		} catch (final Exception e) {
//			e.printStackTrace();
//			throw new Exception("Creating database connection to dwhrep failed.", e);
//		}
//	}

}
