package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.ServicenamesHelper;
import com.distocraft.dc5000.common.ServicenamesHelperFactory;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.system.SlotRebuilder.NodeCoreMap;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.common.testutilities.PollerBaseTestCase;
import com.ericsson.eniq.common.testutilities.ServicenamesTestHelper;
import com.ericsson.eniq.repository.dbusers.AddServiceUser;
import com.ericsson.etl.engine.config.ConfigurationLoader;
import com.ericsson.etl.engine.config.ConfigurationLoaderFactory;

/**
 * 
 * @author ejarsok
 * 
 */

public class SlotRebuilderTest {

	private static RockFactory rockFact;

	private static Logger log = Logger.getLogger("Logger");

	private static final File TMPDIR = new File(System.getProperty("java.io.tmpdir"), "SlotRebuilderTest");
	private static final Integer DUMMY_CORE_COUNT = 4;
	private static final Integer DWH_WRITER_19_CORE_COUNT = 8;
	private static final Integer DWH_WRITER_20_CORE_COUNT = 4;
	private static final Integer DWH_WRITER_21_CORE_COUNT = 2;
	private static final Integer DWH_WRITER_22_CORE_COUNT = 16;
	private static final String CONFIG_FILENAME = "engine_slot_configuration.ini";
	
	@BeforeClass
	public static void init() throws ClassNotFoundException, SQLException, RockException, IOException {
		StaticProperties.giveProperties(new Properties());
		Class.forName("org.hsqldb.jdbcDriver");

		rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
		final Statement stm = rockFact.getConnection().createStatement();

		try {
			stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(62), COLLECTION_SET_NAME VARCHAR(62),"
					+ "DESCRIPTION VARCHAR(62),VERSION_NUMBER VARCHAR(62),ENABLED_FLAG VARCHAR(62),TYPE VARCHAR(62))");
	
			stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");
	
			stm.execute("CREATE TABLE Meta_databases (USERNAME VARCHAR(31), VERSION_NUMBER VARCHAR(31), "
	    				+ "TYPE_NAME VARCHAR(31), CONNECTION_ID VARCHAR(31), CONNECTION_NAME VARCHAR(31), "
	    				+ "CONNECTION_STRING VARCHAR(31), PASSWORD VARCHAR(31), DESCRIPTION VARCHAR(64), DRIVER_NAME VARCHAR(31), "
	    				+ "DB_LINK_NAME VARCHAR(31), SERVICE_NODE varchar(64))");
	
			stm.execute("CREATE TABLE Meta_execution_slot (PROFILE_ID VARCHAR(31), SLOT_NAME VARCHAR(31),"
					+ "SLOT_ID VARCHAR(31), ACCEPTED_SET_TYPES VARCHAR(128), SERVICE_NODE varchar(64))");
	
			stm.execute("CREATE TABLE Meta_execution_slot_profile (PROFILE_NAME VARCHAR(31), PROFILE_ID VARCHAR(31),"
					+ "ACTIVE_FLAG VARCHAR(31))");
	
			stm.execute("CREATE TABLE Configuration (PARAMNAME VARCHAR(62), PARAMVALUE VARCHAR(128))");
	
			System.setProperty("ETC_DIR", TMPDIR.getPath());
			System.setProperty("CONF_DIR", TMPDIR.getPath());
	
			ServicenamesTestHelper.setupEmpty(TMPDIR);
			//ServicenamesTestHelper.createNiqIniWritersDefined();
			ServicenamesTestHelper.createSampleServicenamesFile();
			ServicenamesTestHelper.createHostsFile_Events();
		} finally {
			stm.close();
		}
		
	}

	@AfterClass
	public static void cleanup() throws SQLException {
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.execute("DROP TABLE Meta_collection_sets");
			stm.execute("DROP TABLE Meta_execution_slot");
			stm.execute("DROP TABLE Configuration");
			stm.execute("DROP TABLE Meta_execution_slot_profile");
		} finally {
			stm.close();
		}
		rockFact.getConnection().close();

		PollerBaseTestCase.delete(TMPDIR);
	}

	@Before
	public void setup() throws IOException, SQLException, RockException {
		/* 
		 * For most tests, we don't want ExecutionProfileAction to reload the configuration from 
		 * file as this would override the test config... so set reloadConfig to false
		 * To run tests that include reloading of configuration, set reloadConfig to true in 
		 * individual tests. 
		 */
		SlotRebuilder.setReloadConfig(false);
		
		/*
		 * ServicenamesHelperFactory is used by SlotRebuilder.getCpusPerIqNode
		 * to access ServicenamesHelper.getServiceHostCoreCount
		 * Set instance provided by ServicenamesHelperFactory to "MockedServicenamesHelper"
		 * to replace real call to getServiceHostCoreCount and return test data
		 */
		ServicenamesHelperFactory.setInstance(new MockedServicenamesHelper());
		
		// clear ConfigurationLoaderFactory mocked instance
		ConfigurationLoaderFactory.setMockedInstance(null);

		// reset niq.ini to default, single node
		ServicenamesTestHelper.createDefaultNiqIni();
		
		// set SlotRebuilder to use mocked AddMissingServiceUsers
		SlotRebuilder.setAddMissingServiceUsers(new MockedAddServiceUser());

		// make sure dwhrep/user and dwhdb/dcuser exist in meta_databases
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			final Meta_databases mdCondition = new Meta_databases(rockFact);
			mdCondition.setConnection_name("dwhrep");
			mdCondition.setType_name("USER");
			final Meta_databasesFactory mdFactory = new Meta_databasesFactory(rockFact, mdCondition);
			final List<Meta_databases> dwhrep_databases = mdFactory.get();
	
			if (dwhrep_databases.size() == 0) {
				stm.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', '1', 'dwhrep', "
						+ "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname', 'repdb')");
			}
		} finally {
			stm.close();
		}
	}
	
	@After
	public void teardown() throws SQLException {
		
		final Statement rstm = rockFact.getConnection().createStatement();
		try {
			rstm.executeUpdate("DELETE FROM Configuration");
			rstm.executeUpdate("DELETE FROM Meta_execution_slot");
			rstm.executeUpdate("DELETE FROM Meta_execution_slot_profile");
		} finally {
			rstm.close();
		}
	}

	/**
	 * Create a test config file
	 * @throws IOException
	 */
	private static void createTestSlotConfIni() throws IOException {

		if (!TMPDIR.exists() && !TMPDIR.mkdirs()) {
			fail("Failed to create " + TMPDIR.getPath());
		}
		TMPDIR.deleteOnExit();

		final File testSlotConfIni = new File(TMPDIR.getPath(), CONFIG_FILENAME);
		
		log.info("Creating temporary test config file: " + testSlotConfIni.getCanonicalPath());
		final PrintWriter configWriter = new PrintWriter(new FileWriter(testSlotConfIni, false));
		try{
			configWriter.println("[SLOT_CONFIGURATION]");
			configWriter.println("executionProfile.0.Normal=Y");
			configWriter.println("executionProfile.1.NoLoads=N");
			configWriter.println("executionProfile.0.slot1.0.execute=adapter,Adapter,Alarm,Install,Mediation");
			configWriter.println("executionProfile.0.slot1.0.formula=2");
			configWriter.println("executionProfile.0.slot1.0.type=writer");
			configWriter.println("executionProfile.0.slot2.1.execute=Loader,Topology");
			configWriter.println("executionProfile.0.slot2.1.formula=1");
			configWriter.println("executionProfile.0.slot2.1.type=writer");
		} finally {
			if(configWriter != null){
				configWriter.close();
			}
			testSlotConfIni.deleteOnExit();
		}
	}

	/** 
	 * Checks that initDwhRepRock fails if no connection to dwhrep can be made
	 * @throws SQLException 
	 * @throws EngineMetaDataException 
	 */
	@Test(expected=RockException.class)
	public void test_noDwhrepConnection() throws Exception {
		
		// remove dwhrep connection record from meta_databases
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("delete from META_DATABASES where CONNECTION_NAME='dwhrep'");
		} finally {
			stm.close();
		}

		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();
	}

	
	/** 
	 * Checks that execute() fails when ConfigurationLoader fails
	 * @throws SQLException 
	 */
	@Test(expected=SQLException.class)
	public void testExecuteFailsWhenConfigurationLoaderFails() throws Exception {
		
		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		
		// mock ConfigurationLoader to force exception
		ConfigurationLoaderFactory.setMockedInstance(new MockedConfigurationLoader(rockFact, log));
		// re-enable config loading
		SlotRebuilder.setReloadConfig(true);
		
		slotRebuild.rebuildSlots();
	}	
	
	/**
	 * Checks that correct number of slots are created for a basic single node
	 * installation when formula is set to an absolute value (rather than relative
	 * to number of cores)
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_simpleSlotCreation() throws Exception {

		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.Normal', '1')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.formula', '2')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.execute', 'Loader')");
		} finally {
			stm.close();
		}

		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();

		final long slotCount = getRowCount("Meta_execution_slot");
		final long profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 1 || slotCount != 2) {
			fail("Expected 1 profile got " + profiCount + " and 2 slots got " + slotCount);
		}

	}	

	/**
	 * Checks that correct number of slots are created after reloading updated
	 * Configuration values from ini file
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_slotCreationWithReloadedConfig() throws Exception {

		// FIRST: set up config manually and run slotRebuild.execute to generate some initial slots
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.Normal', '1')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.formula', '2')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.execute', 'Loader')");
		} finally {
			stm.close();
		}

		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();

		long slotCount = getRowCount("Meta_execution_slot");
		long profiCount = getRowCount("Meta_execution_slot_profile");

		// check that initial slots are what we'd expect them to be to ensure reload test is valid
		if(profiCount != 1 || slotCount != 2) {
			fail("Expected 1 profile got " + profiCount + " and 2 slots got " + slotCount);
		}

		// NEXT: set to reload config from file and re-run slotRebuild.execute() to create new slots
		SlotRebuilder.setReloadConfig(true);
		createTestSlotConfIni();
		slotRebuild.rebuildSlots();
		
		slotCount = getRowCount("Meta_execution_slot");
		profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 2 || slotCount != 3) {
			fail("Expected 2 profiles got " + profiCount + " and 3 slots got " + slotCount);
		}
	}	

	/**
	 * Checks that repeat calls of execute() work and 
	 * correct number of slots are created after reloading updated
	 * Configuration values from ini file
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_repeatCallsForSlotCreationWithReloadedConfig() throws Exception {
		
		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);

		long slotCount = getRowCount("Meta_execution_slot");
		long profiCount = getRowCount("Meta_execution_slot_profile");

		// set to reload config from file and repeat-run slotRebuild.execute() to create new slots
		SlotRebuilder.setReloadConfig(true);
		createTestSlotConfIni();
		slotRebuild.rebuildSlots();
		slotRebuild.rebuildSlots();
		
		slotCount = getRowCount("Meta_execution_slot");
		profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 2 || slotCount != 3) {
			fail("Expected 2 profiles got " + profiCount + " and 3 slots got " + slotCount);
		}
	}	

	
	/**
	 * Checks that no slots or profiles are created when the configuration
	 * table is empty
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_emptyConfiguration_noSlots() throws Exception {

		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();

		final long slotCount = getRowCount("Meta_execution_slot");
		final long profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 0 || slotCount != 0) {
			fail("Expected 0 profile got " + profiCount + " and 0 slots got " + slotCount);
		}

	}

	/** 
	 * Check that SlotRebuilder builds no slots if the formula is missing
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_formulaMissing_noSlots() throws Exception {

		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.Normal', '1')");
			//stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.formula', '2')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.execute', 'Loader')");
		} finally {
			stm.close();
		}

		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();

		final long slotCount = getRowCount("Meta_execution_slot");
		final long profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 1 || slotCount != 0) {
			fail("Expected 1 profile got " + profiCount + " and 0 slots got " + slotCount);
		}

	}

	/** 
	 * Check that SlotRebuilder builds no slots if the execute action
	 * is missing
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_executeActionMissing_noSlots() throws Exception {

		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.Normal', '1')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.formula', '2')");
			//stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.execute', 'Loader')");
		} finally {
			stm.close();
		}

		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();

		final long slotCount = getRowCount("Meta_execution_slot");
		final long profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 1 || slotCount != 0) {
			fail("Expected 1 profile got " + profiCount + " and 0 slots got " + slotCount);
		}

	}

	/**
	 * Check that number of slots are calculated correctly for different formulas
	 * for a basic single node installation
	 *  
	 * @throws Exception
	 */
	@Test
	public void test_formulasProduceCorrectSlots_singleNode() throws Exception {

		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.Normal', '1')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.formula', '2n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.execute', 'Loader')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.formula', '0.125n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.execute', 'Loader')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.formula', '0.9n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.execute', 'Loader')");
		} finally {
			stm.close();
		}

		/* With default test configuration (just dwh, with 4 cores) the formulas defined above should
		 * create 13 slots 
		 * 
		 * i.e. (2n * 4) + (0.125n * 4) + (0.9n * 4) = 12.1 (rounded up to 13)
		 */
		
		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();

		final long slotCount = getRowCount("Meta_execution_slot");
		final long profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 1 || slotCount != 13) {
			fail("Expected 1 profile got " + profiCount + " and 13 slots got " + slotCount);
		}

	}


	/**
	 * Check that number of slots are calculated correctly for different formulas
	 * for a multi node installation
	 *  
	 * @throws Exception
	 */
	@Test
	public void test_formulasProduceCorrectSlots_multiNode() throws Exception {

		// create test niq.ini with multiple writer nodes
		ServicenamesTestHelper.createNiqIniFourWritersDefined();
		
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.Normal', '1')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.formula', '2n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.execute', 'Loader')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.type', 'writer')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.formula', '0.125n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.execute', 'Loader')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.type', 'writer')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.formula', '0.9n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.execute', 'Loader')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.type', 'writer')");
		} finally {
			stm.close();
		}

		/* e.g with a 4 node test configuration: (DWH_WRITER_19 with 8 cores, 
		 * DWH_WRITER_20 with 4 cores, DWH_WRITER_21 with 2 cores and
		 * DWH_WRITER_22 with 16 cores). The formulas defined above should
		 * create 94 slots 
		 * 
		 * i.e. 8 * 3.025 = 24.2 (rounded up to 25)
		 *      4 * 3.025 = 12.1 (rounded up to 13)
		 *      2 * 3.025 = 6.05 (rounded up to 7)
		 *      16 * 3.025 + (0.9n * 16) = 48.4 (rounded up to 49)
		 *      
		 *      total slots = 94 (25 + 13 + 7 + 49)
		 *      
		 * Where 3.025 = Sum of formulas (2 + 0.125 + 0.9)     
		 */
		final int expectedSlots = (int) Math.ceil(DWH_WRITER_19_CORE_COUNT * 3.025) + 
				(int) Math.ceil(DWH_WRITER_20_CORE_COUNT * 3.025) + 
				(int) Math.ceil(DWH_WRITER_21_CORE_COUNT * 3.025) + 
				(int) Math.ceil(DWH_WRITER_22_CORE_COUNT * 3.025);
		
		
		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();

		final long slotCount = getRowCount("Meta_execution_slot");
		final long profiCount = getRowCount("Meta_execution_slot_profile");

    assertEquals("Profile Count is incorrect", 1, profiCount);
    assertEquals("Profile Slot count is incorrect", expectedSlots, slotCount);

//		if(profiCount != 1 || slotCount != expectedSlots) {
//			fail("Expected 1 profile got " + profiCount + " and "
//					+ expectedSlots + " slots got " + slotCount);
//		}

	}

	/**
	 * Check that with repeat runs of execute() method, number of slots are calculated correctly 
	 * and not multiplied
	 *  
	 * @throws Exception
	 */
	@Test
	public void test_repeatExecution() throws Exception {

		// create test niq.ini with multiple writer nodes
		ServicenamesTestHelper.createNiqIniFourWritersDefined();
		
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.Normal', '1')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.formula', '2n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.execute', 'Loader')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.type', 'writer')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.formula', '0.125n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.execute', 'Loader')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.type', 'writer')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.formula', '0.9n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.execute', 'Loader')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.type', 'writer')");
		} finally {
			stm.close();
		}

		/* e.g with a 4 node test configuration: (DWH_WRITER_19 with 8 cores, 
		 * DWH_WRITER_20 with 4 cores, DWH_WRITER_21 with 2 cores and
		 * DWH_WRITER_22 with 16 cores). The formulas defined above should
		 * create 94 slots 
		 * 
		 * i.e. 8 * 3.025 = 24.2 (rounded up to 25)
		 *      4 * 3.025 = 12.1 (rounded up to 13)
		 *      2 * 3.025 = 6.05 (rounded up to 7)
		 *      16 * 3.025 + (0.9n * 16) = 48.4 (rounded up to 49)
		 *      
		 *      total slots = 94 (25 + 13 + 7 + 49)
		 *      
		 * Where 3.025 = Sum of formulas (2 + 0.125 + 0.9)     
		 */
		final int expectedSlots = (int) Math.ceil(DWH_WRITER_19_CORE_COUNT * 3.025) + 
				(int) Math.ceil(DWH_WRITER_20_CORE_COUNT * 3.025) + 
				(int) Math.ceil(DWH_WRITER_21_CORE_COUNT * 3.025) + 
				(int) Math.ceil(DWH_WRITER_22_CORE_COUNT * 3.025);
		
		
		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();
		slotRebuild.rebuildSlots();
		slotRebuild.rebuildSlots();

		final long slotCount = getRowCount("Meta_execution_slot");
		final long profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 1 || slotCount != expectedSlots) {
			fail("Expected 1 profile got " + profiCount + " and " 
					+ expectedSlots + " slots got " + slotCount);
		}

	}

	/**
	 * Check that number of slots are calculated correctly after the configuration 
	 * table is updated 
	 *  
	 * @throws Exception
	 */
	@Test
	public void test_executionAfterConfigUpdate() throws Exception {

		// create test niq.ini with multiple writer nodes
		ServicenamesTestHelper.createNiqIniFourWritersDefined();
		
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.Normal', '1')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.formula', '2n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.execute', 'Loader')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.type', 'writer')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.formula', '0.125n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.execute', 'Loader')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot1.1.type', 'writer')");
		} finally {
			stm.close();
		}

		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();

		int expectedSlots = (int) Math.ceil(DWH_WRITER_19_CORE_COUNT * 2.125) + 
				(int) Math.ceil(DWH_WRITER_20_CORE_COUNT * 2.125) + 
				(int) Math.ceil(DWH_WRITER_21_CORE_COUNT * 2.125) + 
				(int) Math.ceil(DWH_WRITER_22_CORE_COUNT * 2.125);

		long slotCount = getRowCount("Meta_execution_slot");
		long profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 1 || slotCount != expectedSlots) {
			fail("Expected 1 profile got " + profiCount + " and " 
					+ expectedSlots + " slots got " + slotCount);
		}
		
		// update Configuration table and re-calculate slots
		
		final Statement stm2 = rockFact.getConnection().createStatement();
		try {
			stm2.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.formula', '0.9n')");
			stm2.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.execute', 'Loader')");
			stm2.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot2.2.type', 'writer')");
		} finally {
			stm2.close();
		}		
		// updated expectations
		expectedSlots = (int) Math.ceil(DWH_WRITER_19_CORE_COUNT * 3.025) + 
				(int) Math.ceil(DWH_WRITER_20_CORE_COUNT * 3.025) + 
				(int) Math.ceil(DWH_WRITER_21_CORE_COUNT * 3.025) + 
				(int) Math.ceil(DWH_WRITER_22_CORE_COUNT * 3.025);
		
		slotRebuild.rebuildSlots();

		slotCount = getRowCount("Meta_execution_slot");
		profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 1 || slotCount != expectedSlots) {
			fail("Expected 1 profile got " + profiCount + " and " 
					+ expectedSlots + " slots got " + slotCount);
		}

	}

	/**
	 * Check that any existing slots are properly cleared for configured profile ids. 
	 * Slots not linked to configured profile ids should not be cleared.
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_updateOfExsitingSlots() throws Exception {

		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.Normal', '1')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.formula', '2n')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.slot0.0.execute', 'Loader')");

			stm.executeUpdate("INSERT INTO Meta_execution_slot_profile VALUES('Normal','0','Y')");
			stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('0','Slot1','0','Loader', null)");
			stm.executeUpdate("INSERT INTO Meta_execution_slot_profile VALUES('NoLoads','1','Y')");
			stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('1','Slot2','1','Count', null)");
		} finally {
			stm.close();
		}

		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);
		slotRebuild.rebuildSlots();

		final long slotCount = getRowCount("Meta_execution_slot");
		final long profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 2 || slotCount != 9) {
			fail("Expected 2 profiles got " + profiCount + " and 9 slots got " + slotCount);
		}

	}

	/**
	 * Check that getCpusPerIqNode returns correct list
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_getCpusPerIqNode() throws Exception {

		ServicenamesTestHelper.createNiqIniWritersDefined();
		ServicenamesTestHelper.createSampleServicenamesFile();

		final SlotRebuilder slotRebuild = new MockedSlotRebuilder(rockFact, log);

		final NodeCoreMap coreCounts = slotRebuild.getCpusPerServiceNode();

		assertTrue(coreCounts.isDefined("dwh_writer_19"));
		assertTrue(coreCounts.isDefined("dwh_writer_20"));
		assertEquals(DWH_WRITER_19_CORE_COUNT, coreCounts.getCores("dwh_writer_19"));
		assertEquals(DWH_WRITER_20_CORE_COUNT, coreCounts.getCores("dwh_writer_20"));
	}
	
	/**
	 * Gets count of number of rows in specified table
	 * 
	 * @param tableName Name of table 
	 * @return number of rows in the table
	 * @throws SQLException
	 */
	private long getRowCount(final String tableName) throws SQLException {
		long rowCount = 0;
		final Statement rstm = rockFact.getConnection().createStatement();
		
		final String sqlQuery = "select count(*) as rowcount from " + tableName;
		
		final ResultSet rs = rstm.executeQuery(sqlQuery);
		try {
			while(rs.next()) {
				rowCount = rs.getLong("rowcount");
			}
		} finally {
			rs.close();
			rstm.close();
		}
		
		return rowCount;
	}
	
	/**
	 * Simple mocked version of SlotRebuilder that overrides 
	 * getCpusPerServiceNode to return a dummy value 
	 *
	 */
	public class MockedSlotRebuilder extends SlotRebuilder {
		public MockedSlotRebuilder(final RockFactory etlRepRock, final Logger parentLog)
						throws EngineMetaDataException {

			super(etlRepRock, parentLog);
		}

		@Override
		protected NodeCoreMap getCpusPerServiceNode() throws IOException {
			final NodeCoreMap map = new NodeCoreMap();
			final Map<String, ServicenamesHelper.ServiceHostDetails> services = ServicenamesHelper.getServiceDetails();
			final MockedServicenamesHelper mh = new MockedServicenamesHelper();
			for(ServicenamesHelper.ServiceHostDetails service : services.values()){
				map.addNode(service.getServiceName(), mh.getServiceHostCoreCount(service, null));
			}
			return map;
		}

	}

	/**
	 * Mocked version of ConfigurationLoader that always throws an exception
	 * when load() is called. Used to check that SlotRebuilder fails
	 * if ConfigurationLoader fails. 
	 *
	 */
	class MockedConfigurationLoader extends ConfigurationLoader {

		public MockedConfigurationLoader(final RockFactory dwhrep, final Logger parentLog) {
			super(dwhrep, parentLog);
		}
		
		@Override
		public void load() throws SQLException, RockException {
			throw new SQLException("Dummy exception");
		}
		
	}
	
	/** 
	 * Mock version of ServicenamesHelper that replaces getServiceHostCoreCount
	 * with a dummy version that eliminates the need to access real servers.
	 * ServicenamesHelperFactory is used to switch ServicenamesHelper over to the
	 * alternative version:
	 * 
	 * ServicenamesHelperFactory.setInstance(new MockedServicenamesHelper())
	 *
	 */
	class MockedServicenamesHelper extends ServicenamesHelper {

		/** 
		 * Override getServiceHostCoreCount to allow for testing without the need
		 * to connect to real servers.
		 * 
		 * @return Mocked core counts
		 */
		@Override
		public int getServiceHostCoreCount(final ServiceHostDetails hostDetails,
				final String passwd) throws IOException {
			
			if (hostDetails.getServiceName().equals("dwh_writer_19")) {
				return DWH_WRITER_19_CORE_COUNT;
			} else if (hostDetails.getServiceName().equals("dwh_writer_20")) {
				return DWH_WRITER_20_CORE_COUNT;
			} else if (hostDetails.getServiceName().equals("dwh_writer_21")) {
				return DWH_WRITER_21_CORE_COUNT;
			} else if (hostDetails.getServiceName().equals("dwh_writer_22")) {
				return DWH_WRITER_22_CORE_COUNT;
			} else {
				return DUMMY_CORE_COUNT;
			}
		}

	}
	
	class MockedAddServiceUser extends AddServiceUser {
		public void addServiceUsers(final String[] args) {
			Statement stm = null;
			try {
				stm = rockFact.getConnection().createStatement();
				final Meta_databases mdCondition = new Meta_databases(rockFact);
				mdCondition.setConnection_name("dwhdb");
				mdCondition.setType_name("dcuser");

				final Meta_databasesFactory mdFactory = new Meta_databasesFactory(rockFact, mdCondition);
				final List<Meta_databases> dwhdb_databases = mdFactory.get();

				if (dwhdb_databases.size() == 0) {
					System.out.println("MockedAddServiceUser adding new dcuser record.");
					stm.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'dcuser', '1', 'dwh_writer_19', "
							+ "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname', 'repdb')");
					stm.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'dcuser', '1', 'dwh_writer_20', "
							+ "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname', 'repdb')");
					stm.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'dcuser', '1', 'dwh_writer_21', "
							+ "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname', 'repdb')");
					stm.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'dcuser', '1', 'dwh_writer_22', "
							+ "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname', 'repdb')");
					stm.executeUpdate("insert into META_DATABASES VALUES ('dcuser', '0', 'dcuser', 19, 'dwhdb', '', "
							+ "'dcuser', 'dcuser for service dwhdb used by adminUI.', '', null, 'dwhdb');");
				}
				
			} catch (Exception e) {
				log.severe("MockedAddServiceUser. Problem adding record. " + e.getMessage());
			} finally {
				try {
					if (!stm.isClosed()) {
						stm.close();
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}

