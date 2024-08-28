package com.distocraft.dc5000.etl.engine.system;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.ServicenamesHelper;
import com.ericsson.eniq.repository.ETLCServerProperties;

/**
 * Wrapper class of ExecutionProfilerAction that can be called
 * to regenerate slot profiles on a ad-hoc basis e.g. after
 * the IQ nodes are reconfigured.
 * 
 * Exit Codes:
 * 
 * 0 = success
 * 1 = failed (general)
 * 2 = failed (problem connecting to db)
 * 3 = failed (no slots generated)
 * 
 * @author ebrujam
 *
 */
public class RegenerateSlotProfiles {

	final static String FAIL_MESSAGE = "Problem regenerating slots!";

	final static int EXIT_SUCCESS = 0;
	final static int EXIT_FAILURE = 1;
	final static int EXIT_FAILURE_DB_CONNECT = 2;
	final static int EXIT_FAILURE_NO_SLOTS = 3;
	
	final static Logger log;

	static {
		log = Logger.getLogger("RegenerateSlotProfiles");
	}
	
	/**
	 * Use main() to run ExecutionProfilerAction.execute() from shell.
	 * Takes no parameters. 
	 * 
	 * If slots are regenerated successfully, the new number of slots
	 * is output to standard out. 
	 * 
	 * If there is a problem, an error message is sent to standard error
	 * 
	 * @throws Exception 
	 *  
	 */
	public static void main(String[] args) throws Exception {
		
		// Is there any way to check if engine is running???
		
		// Set up

		log.info("Regenerating slot profiles on requested...");
		
		RockFactory rockFact = null;
		try {
			rockFact = getRockFact("RebuildSlots");
		} catch (Exception e) {
			log.severe("Cannot make database connection: " + e);
			System.err.println(FAIL_MESSAGE + "\n" + e);
			System.exit(EXIT_FAILURE_DB_CONNECT);
		} 
		
		regenerateSlots(rockFact);

		log.info("Slots regenerated.");
		System.exit(EXIT_SUCCESS);
	}

	/**
	 * Sets up and runs ExecutionProfilerAction.execute() to regenerate slots
	 * then checks to see if it was successful
	 * 
	 * @param rockFact
	 * @param log
	 * @throws Exception
	 */
	static void regenerateSlots(RockFactory rockFact) throws Exception {
		
		SlotRebuilder slotRebuilder = new SlotRebuilder(rockFact, log);
		slotRebuilder.rebuildSlots();

		// log/output results
		List<String> allNodes = ServicenamesHelper.getAllIQNodes();
		List<String> writerNodes = ServicenamesHelper.getWriterNodes();

		log.info("RegenerateSlotProfiles: All IQ nodes: " + allNodes);
		log.info("RegenerateSlotProfiles: IQ nodes used for slot generation: " + writerNodes);
		
		try {
			long slotCount = getRowCount(rockFact, "Meta_execution_slot");
			
			if (slotCount>0) {
				log.info("RegenerateSlotProfiles: Number of slots: " + slotCount);
				System.out.println("Slots=" + slotCount);
			} else {
				log.severe("RegenerateSlotProfiles: Regeneration ran successfully, but no slots were generated.");
				System.err.println(FAIL_MESSAGE + "\nRegenerateSlotProfiles: Regeneration ran successfully, but no slots were generated.");
				System.exit(EXIT_FAILURE_NO_SLOTS);
			}
		} catch (Exception e) {
			log.warning("Can't get count of execution slots: " + e);
			System.err.println(FAIL_MESSAGE + "\n" + e);
			System.exit(EXIT_FAILURE);
		}
		
	}
	
	/**
	 * Get connection to database
	 * 
	 * @param log
	 * @param conName
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 * @throws RockException
	 */
	private static RockFactory getRockFact(String conName) throws IOException, SQLException, RockException {

		final RockFactory rockFact;

		String etlcServerPropertiesFile = System.getProperty("CONF_DIR");

		if (etlcServerPropertiesFile == null) {
		  log.config("System property CONF_DIR not defined. Using default");
		  etlcServerPropertiesFile = "/eniq/sw/conf";
		}

		if (!etlcServerPropertiesFile.endsWith(File.separator)) {
		  etlcServerPropertiesFile += File.separator;
		}
		
		etlcServerPropertiesFile += "ETLCServer.properties";
		
		log.info("Reading server configuration from \"" + etlcServerPropertiesFile + "\"");

		final java.util.Properties appProps = new ETLCServerProperties(etlcServerPropertiesFile);
		final String etlrep_url = appProps.getProperty("ENGINE_DB_URL");
		final String etlrep_usr = appProps.getProperty("ENGINE_DB_USERNAME");
		final String etlrep_pwd = appProps.getProperty("ENGINE_DB_PASSWORD");
		final String etlrep_drv = appProps.getProperty("ENGINE_DB_DRIVERNAME");
		
	   	log.finest("Initializing DB connection to ETLREP");
		rockFact = new RockFactory(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, conName, true);
		log.finest("DB connection initialized");
		
		return rockFact;
		
	}

	/**
	 * Get count of number of rows in specified table
	 * 
	 * @param tableName Name of table 
	 * @return number of rows in the table
	 * @throws SQLException
	 */
	private static long getRowCount(RockFactory rockFact, String tableName) throws SQLException {
		long rowCount = 0;
		Statement rstm=null;
		ResultSet rs=null;
		
		try {
			rstm = rockFact.getConnection().createStatement();
		
			String sqlQuery = "select count(*) as rowcount from " + tableName;
			
			rs = rstm.executeQuery(sqlQuery);
			while(rs.next()) {
				rowCount = rs.getLong("rowcount");
			}
		} finally {
			try {
				rs.close();
			} catch (Exception e) {
				log.warning("Can't close result set: " + e);
			}

			try {
				rstm.close();
			} catch (Exception e) {
				log.warning("Can't close statement: " + e);
			}
		}
			
		return rowCount;
	}
	
}
