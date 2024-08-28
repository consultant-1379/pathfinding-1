/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2010 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.system;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.ServicenamesHelper;
import com.distocraft.dc5000.common.ServicenamesHelperFactory;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_execution_slot;
import com.distocraft.dc5000.etl.rock.Meta_execution_slot_profile;
import com.distocraft.dc5000.repository.dwhrep.Configuration;
import com.distocraft.dc5000.repository.dwhrep.ConfigurationFactory;
import com.ericsson.eniq.repository.dbusers.AddServiceUser;
import com.ericsson.etl.engine.config.ConfigurationLoader;
import com.ericsson.etl.engine.config.ConfigurationLoaderFactory;

/**
 *
 */
public class SlotRebuilder {

	private static final int FIELDS_IN_PROFILE = 3;
	private static final String CORE_VARIABLE = "n";
	private static final String MULTIPLY_STR = "multiply";
	private static final String CONF_NAME_DELIM = ".";
	private static final String PROF_CONF_PREFIX = "executionProfile.";
	private static AddServiceUser addMissingServiceUsers = new AddServiceUser();
	private static boolean reloadConfig = true;
	
	private final RockFactory etlrep;
	private final Logger log;
	private final Logger parentLog;

	private final Map<String, Profile> profiles = new HashMap<String, Profile>();
	private final Map<String, Slot> slots = new HashMap<String, Slot>();

	enum SlotParamType {
		execute, formula, type;
	}

	SlotRebuilder(final RockFactory etlRepRock, final Logger parentLog) {
		this.log = Logger.getLogger(parentLog.getName() + ".SlotRebuilder");
		this.parentLog = parentLog;
		this.etlrep = etlRepRock;
	}
	
	/**
	 * Execute this action
	 * @throws SQLException 
	 * @throws RockException 
	 * @throws IOException 
	 */
	public void rebuildSlots() throws SQLException, RockException, IOException {

		RockFactory dwhrep = null;
		try {
			dwhrep = initDwhRepRock();

			// do we need to reload configuration table from ini file?
			// default=true (i.e. reload). Set as false to prevent reloading, e.g. for unit tests
			log.info("Reloading configuration from file? " + 
					(reloadConfig ? "YES" : "NO") + 
					" [reloadConfig==" + reloadConfig + "]");
			if (reloadConfig) { 
				final ConfigurationLoader configLoader = ConfigurationLoaderFactory.getInstance(dwhrep, parentLog);
				configLoader.load();
			}
			
			getConfiguration(dwhrep);

			final int nextSlotID = getNextSlotID();

			log.info("Deleting profiles");
			for (Profile profile : profiles.values()) {
				log.finer("Deleting profile " + profile.profileID + "(" + profile.profileName + ") from db");
				deleteProfile(profile.profileID);
			}

			addProfiles(nextSlotID);

			log.info("Excution Profile successfully updated");

		} finally {
			if (dwhrep != null) {
				try {
					dwhrep.getConnection().close();
				} catch (Exception e) {
					log.log(Level.FINE, "Error cleanup dwhrep connection", e);
				}
			}
		}
	}

	/**
	 * Creates a connection to DWHRep database.
	 */
	private RockFactory initDwhRepRock() throws RockException, SQLException {
		RockFactory ret = null;

		final Meta_databases mdCondition = new Meta_databases(this.etlrep);
		mdCondition.setConnection_name("dwhrep");
		mdCondition.setType_name("USER");
		final Meta_databasesFactory mdFactory = new Meta_databasesFactory(this.etlrep, mdCondition);

		final List<Meta_databases> databases = mdFactory.get();

		if (databases.size() >= 1) {
			final Meta_databases database = databases.get(0);

			ret = new RockFactory(database.getConnection_string(), database.getUsername(), database.getPassword(),
					database.getDriver_name(), "ExecutionProfiler", true);

		} else {
			throw new RockException("No dwhrep connection defined in etlrep.META_DATABASES");
		}

		return ret;
	}

	/**
	 * Reads dwhrep.Configuration from database and save it to local objects.
	 * 
	 * @param dwhrep RockFacktory connected to dwhrep
	 * @throws SQLException Database errors
	 * @throws RockException Database errors
	 * @throws IOException Errors reading hosts or service_names files
	 */
	private void getConfiguration(final RockFactory dwhrep) throws RockException, SQLException, IOException {
		log.info("Getting slot configuration from database");

		try {

			final Configuration confCond = new Configuration(dwhrep);
			final ConfigurationFactory confFact = new ConfigurationFactory(dwhrep, confCond, "WHERE PARAMNAME LIKE '"
					+ PROF_CONF_PREFIX + "%'");

			for (Configuration conf : confFact.get()) {
				final String paramName = conf.getParamname();
				log.finer("Reading parameter name: " + paramName);

				if (PROF_CONF_PREFIX.equalsIgnoreCase(paramName.substring(0, PROF_CONF_PREFIX.length()))) {
					final String paramValue = conf.getParamvalue();
					final String[] paramNameSplitted = paramName.split("\\" + CONF_NAME_DELIM);

					if (paramNameSplitted.length == FIELDS_IN_PROFILE) {
						// A Profile parameter. Looks like: executionProfile.0.Normal

						final Profile tempProfile = new Profile(paramNameSplitted[1], paramNameSplitted[2], paramValue);
						profiles.put(tempProfile.profileID, tempProfile);

						log.finer("Read a profile: " + tempProfile.profileID + " (" + tempProfile.profileName + ")");
					} else {
						// A Slot parameter. Looks like: executionProfile.0.slot1.0.formula

						Slot tempSlot = slots.get(paramNameSplitted[3]);
						if (tempSlot == null) {
							tempSlot = new Slot(paramNameSplitted[1], paramNameSplitted[2], paramNameSplitted[3]);
							slots.put(paramNameSplitted[3], tempSlot);
						}

						final String slotParam = paramNameSplitted[4];

						if ("execute".equalsIgnoreCase(slotParam)) {
							tempSlot.setTypes = paramValue;
						} else if ("formula".equalsIgnoreCase(slotParam)) {
							tempSlot.formula = paramValue;
						} else if ("type".equalsIgnoreCase(slotParam)) {
							tempSlot.accessType = paramValue;
						}

						log.finer("Read a slot: " + tempSlot.profileID + "." + tempSlot.slotID + "(" + tempSlot.slotName + ")");
					}
				}
			}

			final Map<String, List<String>> writerMappings = new HashMap<String, List<String>>(1);
			final NodeCoreMap nodesCores = getCpusPerServiceNode();
			writerMappings.put("writer", ServicenamesHelper.getWriterNodes());
			writerMappings.put("reader", ServicenamesHelper.getReaderNodes());

			for (Slot slot : slots.values()) {
				setAmountOfSlots(slot, nodesCores, writerMappings);
			}

			log.fine(profiles.size() + " profiles and " + slots.size() + " read from dwhrep.Configuration");

			printConfig();

		} catch (RockException rock) {
			log.warning("Error getting configuration from dwhrep.Configuration");
			throw rock;
		} catch (SQLException sql) {
			log.warning("Error getting configuration from dwhrep.Configuration");
			throw sql;
		}

	}

	
	/**
	 * Builds a list of CPU core counts for all IQ Writer Nodes. 
	 *  
	 * @return List of n-v pairs where n=service_name and v=core count
	 * @throws IOException
	 * @throws RockException
	 * @throws SQLException
	 */
	protected NodeCoreMap getCpusPerServiceNode() throws IOException, RockException, SQLException {
		addServiceUser();
		final NodeCoreMap nodes = new NodeCoreMap();
		final Map<String, ServicenamesHelper.ServiceHostDetails> eniqServices = ServicenamesHelper.getServiceDetails();
		final ServicenamesHelper snHelper = ServicenamesHelperFactory.getInstance();

		//Dont ssh to the same hostname twice
		final Map<String, Integer> cache = new HashMap<String, Integer>();

		for (ServicenamesHelper.ServiceHostDetails service : eniqServices.values()) {

			final int hostCoreCount;
			if(cache.containsKey(service.getServiceHostname())){
				hostCoreCount = cache.get(service.getServiceHostname());
			} else {
				/*final Meta_databases info = getLoginInfo(service.getServiceName());*/
				hostCoreCount = snHelper.getServiceHostCoreCount(service, "dcuser");
				cache.put(service.getServiceHostname(), hostCoreCount);
			}
			nodes.addNode(service.getServiceName(), hostCoreCount);
		}
		return nodes;
	}

  /*private Meta_databases getLoginInfo(final String serviceName) throws RockException, SQLException {
    final Meta_databases where = new Meta_databases(etlrep);
    where.setType_name("dcuser");
    where.setConnection_name(serviceName);
    final Meta_databasesFactory fac = new Meta_databasesFactory(etlrep, where);
    final List<Meta_databases> loginDetails = fac.get();
			if (loginDetails.isEmpty()) {
      throw new RockException("No Meta_databases entry for TYPENAME:" + where.getType_name() +
        " CONNECTION_NAME:" + where.getConnection_name() + " found.");
		}
    return loginDetails.get(0);
	}*/

	/**
	 * This method multiplies all n-variables (cpuCoreAmount) within formula and
	 * rounds the result up to ceiling, because for example 0.2n formula could
	 * result less than one
   *
	 * @param slot the slot type to calculate the cores/iq-nodes for
	 * @param nodesCores Node-core count map
	 * @throws SQLException Database errors
	 * @throws RockException Database errors
	 * @throws IOException Errors reading hosts or service_names files
	 */
  private void setAmountOfSlots(final Slot slot, final NodeCoreMap nodesCores, final Map<String, List<String>> mappings) throws RockException, IOException, SQLException {

		if (nodesCores == null) {
			throw new RuntimeException("PANIC, No CPU Core Count for IQ node type '" +
					slot.accessType + "'");
		}

		slot.slotsPerNode = new HashMap<String, Integer>();
    // Change dwh to dwhdb as the service is called dwhdb, keeps the META_EXECUTION_SLOT.SERVICE_NODE consistent.
    if(slot.accessType != null && slot.accessType.equals("dwh")){
      slot.accessType = "dwhdb";
    } else if (slot.accessType == null){
      slot.accessType = "dwhdb";
    }

    final List<String> nodesForSlot;
    if(mappings.containsKey(slot.accessType)){
      nodesForSlot = mappings.get(slot.accessType);
    } else {
      nodesForSlot = new ArrayList<String>();
      nodesForSlot.add(slot.accessType);
    }
    
    for(String node : nodesForSlot){
      if(!nodesCores.isDefined(node)){
        throw new NullPointerException("No service called " + node +" defined in service_names");
      }
			final int coreCount = nodesCores.getCores(node);
			final int slotsForNode = calculateSlotsforCores(slot.formula, coreCount);
			slot.slotsPerNode.put(node, slotsForNode);
		}
	}
	
	private int calculateSlotsforCores(final String formula, final int cpuCoreAmount) {

		double result = 1.0;

		if (formula == null) { 
			result = 0;
		} else {
			final String formulaToUse = formula.replaceAll(CORE_VARIABLE, MULTIPLY_STR + cpuCoreAmount + MULTIPLY_STR);
			final String[] numericParts = formulaToUse.split(MULTIPLY_STR);
	
			for (String numericPart : numericParts) {
				if (numericPart.length() > 0) {
					result *= Double.parseDouble(numericPart);
				}
			}
		}
		
		final int finalResult = (int) Math.ceil(result);
		log.finer("With formula: '" + formula + "' where '" + CORE_VARIABLE + "=" + cpuCoreAmount + "' the slot amount is "
				+ finalResult);

		return finalResult;
	}

	
	/**
	 * Deletes specified profile and related slots from db.
	 */
	private void deleteProfile(final String profileID) throws SQLException, RockException {

		final Meta_execution_slot slotCond = new Meta_execution_slot(etlrep);
		slotCond.setProfile_id(profileID);
		slotCond.deleteDB(slotCond);

		final Meta_execution_slot_profile profileCond = new Meta_execution_slot_profile(etlrep);
		profileCond.setProfile_id(profileID);
		profileCond.deleteDB(profileCond);

	}

	/**
	 * Insert defined profiles and slots into etlrep
   *
	 * @param nextSlotID The next free SLOT_ID
	 * @throws SQLException Database insert errors
	 * @throws RockException Database insert errors
	 */
	private void addProfiles(final int nextSlotID) throws RockException, SQLException {

		log.info("Adding profiles and slots");

		int slotIDCounter = nextSlotID;

		for (Profile profile : profiles.values()) {

			final Meta_execution_slot_profile mesp = new Meta_execution_slot_profile(etlrep);
			mesp.setActive_flag(profile.active);
			mesp.setProfile_id(profile.profileID);
			mesp.setProfile_name(profile.profileName);
			mesp.saveDB();

			log.fine("Profile " + profile.profileID + " (" + profile.profileName + ") added");

			for (Slot slot : slots.values()) {

				if (slot.profileID.equalsIgnoreCase(profile.profileID)) {

					if (slot.setTypes == null || slot.slotsPerNode == null) {
						log.fine("  Slot " + slot.profileID + "." + slot.slotID + " (" + slot.slotName
								+ ") no setTypes, formula or accessType defined");
					} else {

						for (String serviceName : slot.slotsPerNode.keySet()) {

							final int slotsPerNode = slot.slotsPerNode.get(serviceName);

							for (int i = 0; i < slotsPerNode; i++) {
								final Meta_execution_slot mes = new Meta_execution_slot(etlrep);
								mes.setSlot_id(Integer.toString(slotIDCounter));
								mes.setProfile_id(slot.profileID);
								mes.setSlot_name("Slot" + Integer.toString(slotIDCounter));
								mes.setAccepted_set_types(slot.setTypes);
								mes.setService_node(serviceName);
								mes.saveDB();
								slotIDCounter++;
							}

							log.fine("  Slot " + slot.profileID + "." + slot.slotID + " (" + slot.slotName + ") types " + slot.setTypes
									+ " " + slotsPerNode + " slots added into db for service " + serviceName);
						}
					}
				}
			}
		}
	}

	/**
	 * Queries last used slotID in database
	 */
	private int getNextSlotID() throws SQLException {
		int returnValue = 0;

		Statement statement = null;
		ResultSet resultSet = null;

		try {
			statement = etlrep.getConnection().createStatement();
			resultSet = statement
					.executeQuery("SELECT COALESCE(MAX(CAST(SLOT_ID AS integer))+1,0) AS nextID FROM META_EXECUTION_SLOT;");

			if (resultSet.next()) {
				returnValue = resultSet.getInt("nextID");
			}

		} catch (SQLException e) {
			log.warning("An error occured on getting the latest+1 id from database table meta_execution_slot");
			throw e;
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (Exception e) {
					log.info("Error closing resultSet");
				}
			}

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception e) {
					log.info("Error closing statement");
				}
			}
		}

		return returnValue;
	}

	/** 
	 * Call AddServiceUsers to add all missing connection details to meta_databases
	 */
	private void addServiceUser() {
		final String[] args = {"-all"};
		addMissingServiceUsers.addServiceUsers(args);
	}

	/** 
	 * To allow for mocking of AddServiceUsers...
	 */
	public static void setAddMissingServiceUsers(final AddServiceUser newAddServiceUser) {
		addMissingServiceUsers = newAddServiceUser;
	}
	
	/**
	 * Prints Profile and Slot configuration into log.
	 */
	private void printConfig() {
		log.fine("Configuration read from dwhrep.Configuration");

		for (Profile profile : profiles.values()) {
			log.fine("Profile " + profile.profileID + " (" + profile.profileName + ") active " + profile.active);
			for (Slot slot : slots.values()) {
				if (slot.profileID.equalsIgnoreCase(profile.profileID)) {
					for (String service : slot.slotsPerNode.keySet()) {
						final int slotsPerNode = slot.slotsPerNode.get(service);
						log.fine("  " + slot.slotID + " (" + slot.slotName + ") amount " + slotsPerNode + " types "
								+ slot.setTypes + " formula " + slot.formula);
					}
				}
			}
		}

		log.fine("Total " + profiles.size() + " profiles and " + slots.size() + " slots");
	}


	public static boolean isReloadConfig() {
		return reloadConfig;
	}

	public static void setReloadConfig(final boolean reload) {
		reloadConfig = reload;
	}

	// Internal classes to present Profile and Slot

	private final class Profile {

		public String profileID;
		public String profileName;
		public String active;

		public Profile(final String profileID, final String profileName, final String active) {
			this.profileID = profileID;
			this.profileName = profileName;
			this.active = active;
		}

	}

	private final class Slot {

		public String slotID;
		public String slotName;
		public String profileID;
		public String setTypes = null;
		public String formula = null;
		public String accessType = null;
		public Map<String, Integer> slotsPerNode = null;

		public Slot(final String profileID, final String slotName, final String slotID) {
			this.profileID = profileID;
			this.slotName = slotName;
			this.slotID = slotID;
		}
	}

	// internal class to hold node-core count mapping
	protected final class NodeCoreMap {
		private final Map<String, Integer> nodeCores = new HashMap<String, Integer>();

		/** 
		 * Add node to mapping
		 * 
		 * @param serviceName Service name of node to add
		 * @param cores Number of cores
		 */
		public void addNode(final String serviceName, final Integer cores) {
			nodeCores.put(serviceName, cores);
		}

		/**
		 * Get set of currently mapped nodes
		 * 
		 * @return Set of keys (service names) of nodes
		 */
		public Set<String> getNodes() {
			return nodeCores.keySet();
		}

		public boolean isDefined(final String serviceName){
			return nodeCores.containsKey(serviceName);
		}

		/**
		 * Get core count of specified node
		 * 
		 * @param serviceName Service name of node
		 * @return Number of cores
		 */
		public Integer getCores(final String serviceName) {
			return nodeCores.get(serviceName);
		}

		/**
		 * Get number of currently mapped nodes
		 * 
		 * @return Number of mapped nodes
		 */
		public int size() {
			return nodeCores.size();
		}
	}
}