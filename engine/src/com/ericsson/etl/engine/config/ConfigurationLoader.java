package com.ericsson.etl.engine.config;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Configuration;
import com.ericsson.eniq.common.INIGet;


public class ConfigurationLoader {

	/**
	 * conf dir property name, defaults to CONF_DIR_DEFAULT
	 */
	private static final String CONF_DIR = "CONF_DIR";
	/**
	 * Default value for ${CONF_DIR}
	 */
	private static final String CONF_DIR_DEFAULT = "/eniq/sw/conf";
	/**
	 * Filename of .ini holding configuration
	 */
	private static final String CONFIG_FILENAME = "engine_slot_configuration.ini";
	/**
	 * execution slot profile prefix
	 */
	private static final String PROF_CONF_PREFIX = "executionProfile.";

	private final transient Logger log;
	private final transient  RockFactory dwhrep;

	/**
	 * @param dwhrep
	 * @param parentLog
	 */
	public ConfigurationLoader(final RockFactory dwhrep, final Logger parentLog) {
		this.log = Logger.getLogger(parentLog.getName() + ".ConfigurationLoader");
		this.dwhrep = dwhrep;
	}

	/**
	 * Loads data from ini file and uses it to set up configuration. Calls other methods
	 * to do the specific set up tasks
	 * @throws RockException 
	 * @throws SQLException 
	 */
	public void load() throws SQLException, RockException {	
		Map<String, Map<String, String>> iniConfig;

		// read slot configuration from ini file
		try {
			final File ini = new File(System.getProperty(CONF_DIR, CONF_DIR_DEFAULT), CONFIG_FILENAME);
			final INIGet iniReader = new INIGet();
			log.info("Reading slot configuration from " + ini.getCanonicalPath());
			iniConfig = iniReader.readIniFile(ini);
		} catch (IOException e) {
			log.warning("Problem getting configuration from file. " + e);
			iniConfig = new HashMap<String, Map<String, String>>();
		}

		Map<String, String> slotConfig=null;
		
		// If ini file contained at least one block, get first block and use as slotConfig
		// It doesn't matter what the block is called...
		if (!iniConfig.isEmpty()) {
			Object[] blockKeys = iniConfig.keySet().toArray();
			
			if (blockKeys.length>1) {
				log.severe("There is more than 1 block in " + CONFIG_FILENAME + " - there should only be one block. Will try and use first block as slot config.");
			}
			
			slotConfig = iniConfig.get(blockKeys[0]);
			log.info("Got [" + blockKeys[0] + "] block from ini file.");
		}
		
		loadSlotConfiguration(slotConfig);
	}

	/**
	 * Sets executionProfile slot configuration from loaded data. If no 
	 * data is found, a default configuration is used. 
	 * @param slotConfig Map of name-value pairs from ini file
	 * @throws RockException 
	 * @throws SQLException 
	 */
	protected void loadSlotConfiguration(final Map<String, String> slotConfig) throws SQLException, RockException {
		if (slotConfig==null || slotConfig.isEmpty()) {
			log.warning("Slot configuration not defined - using defaults.");
			setDefaultSlotConf();
		} else {
			log.info("Got slot configuration. Add records to db.");

			deleteConfigType(PROF_CONF_PREFIX);

			for (String paramName : slotConfig.keySet()) {
				addNewConfiguration(paramName, slotConfig.get(paramName));
			}

			log.info("Loaded slot configuration. " + slotConfig.size() + " record(s) added to dwhrep.Configuration.");
		}
	}

	/**
	 * Removes any exiting execution slot config data from Configuration table
	 * and adds in new records to define a default configuration.
	 * To be used if no slot configuration ini file can be found, or no records
	 * are defined.  
	 * 
	 * @throws SQLException
	 * @throws RockException
	 */
	protected void setDefaultSlotConf() throws SQLException, RockException {
		// remove old configuration
		deleteConfigType(PROF_CONF_PREFIX);

		// add default configuration
		addNewConfiguration("executionProfile.0.Normal","Y");
		addNewConfiguration("executionProfile.1.NoLoads","N");
		addNewConfiguration("executionProfile.2.InActive","N");

		addNewConfiguration("executionProfile.0.slot1.1.execute","adapter,Adapter,Aggregator,Alarm,Install,Loader,Mediation,Topology");
		addNewConfiguration("executionProfile.0.slot1.1.formula","3");
		addNewConfiguration("executionProfile.0.slot1.1.type","writer");
		addNewConfiguration("executionProfile.0.slot1.2.execute","Partition,Service,Support");
		addNewConfiguration("executionProfile.0.slot1.2.formula","1");
		addNewConfiguration("executionProfile.0.slot1.2.type","writer");
		
		addNewConfiguration("executionProfile.1.slot1.3.execute","Support,Install");
		addNewConfiguration("executionProfile.1.slot1.3.formula","1");
		addNewConfiguration("executionProfile.1.slot1.3.type","dwh");
		addNewConfiguration("executionProfile.1.slot2.4.execute","Support");
		addNewConfiguration("executionProfile.1.slot2.4.formula","2");
		addNewConfiguration("executionProfile.1.slot2.4.type","dwh");
		
		
		log.info("Loaded default slot configuration.");
	}

	/**
	 * Delete any existing execution slot config data from Configuration table.
	 * i.e. deletes any rows that start with "executionProfile."
	 * 
	 * @throws SQLException
	 * @throws RockException
	 */
	protected void deleteConfigType(final String configType) throws SQLException {
		Statement stm = null;
		try {
			stm = dwhrep.getConnection().createStatement();
			stm.executeUpdate("DELETE FROM Configuration WHERE PARAMNAME LIKE '"
					+ configType + "%'");
		} catch (SQLException e) {
			log.severe("Problem deleting existing configuration data like \"" + configType + "%\". " + e);
			throw e;
		} finally {
			if (stm != null) {
				stm.close();
			}
		}
	}

	/**
	 * Add a new configuration record to the Configuration table
	 * 
	 * @param paramName String for configuration.paramname
	 * @param paramValue String for configuration.paramvalue
	 * @throws SQLException
	 * @throws RockException
	 */
	protected void addNewConfiguration(final String paramName, 
			final String paramValue) throws SQLException, RockException  {
		final Configuration newConf = new Configuration(dwhrep, true);
		newConf.setParamname(paramName);
		newConf.setParamvalue(paramValue);
		try {
			newConf.saveDB();
		} catch (SQLException e) {
			log.severe("Problem adding configuration data into db. " + e);
			throw e;
		} catch (RockException e) {
			log.severe("Problem adding configuration data into db. " + e);
			throw e;
		}
	}

}
