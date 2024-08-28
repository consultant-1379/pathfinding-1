package com.distocraft.dc5000.etl.engine.system;

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
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
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

public class ExecutionProfilerActionTest {

	private static final Long collectionSetId = 1L;
	private static final Long transferActionId = 1L;
	private static final Long transferBatchId = 1L;
	private static final Long connectId = 1L;

	private static RockFactory rockFact;
	private static Meta_versions version;
	private static Meta_collections collection;
	private static Meta_transfer_actions trActions;

	private static Logger log = Logger.getLogger("Logger");

	private static final File TMPDIR = new File(System.getProperty("java.io.tmpdir"), "ExecutionProfilerActionTest");
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
	
			version = new Meta_versions(rockFact);
			collection = new Meta_collections(rockFact);
			trActions = new Meta_transfer_actions(rockFact);
	
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
	public void setup() throws IOException, SQLException, RockException, EngineMetaDataException {
		/* 
		 * For most tests, we don't want ExecutionProfileAction to reload the configuration from 
		 * file as this would override the test config... so set reloadConfig to false
		 * To run tests that include reloading of configuration, set reloadConfig to true in 
		 * individual tests. 
		 */
		SlotRebuilder.setReloadConfig(false);
		
		ExecutionProfilerAction.setSlotRebuilder(new MockedSlotRebuilder(rockFact, log));
		
		/*
		 * ServicenamesHelperFactory is used by ExecutionProfilerAction.getCpusPerIqNode
		 * to access ServicenamesHelper.getServiceHostCoreCount
		 * Set instance provided by ServicenamesHelperFactory to "MockedServicenamesHelper"
		 * to replace real call to getServiceHostCoreCount and return test data
		 */
		ServicenamesHelperFactory.setInstance(new MockedServicenamesHelper());
		
		// clear ConfigurationLoaderFactory mocked instance
		ConfigurationLoaderFactory.setMockedInstance(null);

		// reset niq.ini to default, single node
		ServicenamesTestHelper.createDefaultNiqIni();
		
		// set ExecutionProfilerAction to use mocked AddMissingServiceUsers
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

		final ExecutionProfilerAction epa = new ExecutionProfilerAction(version, collectionSetId, collection, transferActionId,
				transferBatchId, connectId, rockFact, trActions, log);
		epa.execute();
	}

	
	/** 
	 * Checks that execute() fails when ConfigurationLoader fails
	 * @throws SQLException 
	 */
	@Test(expected=SQLException.class)
	public void testExecuteFailsWhenConfigurationLoaderFails() throws Exception {
		
		final ExecutionProfilerAction epa = new ExecutionProfilerAction(version, collectionSetId, collection, transferActionId,
				transferBatchId, connectId, rockFact, trActions, log);
		
		// mock ConfigurationLoader to force exception
		ConfigurationLoaderFactory.setMockedInstance(new MockedConfigurationLoader(rockFact, log));
		// re-enable config loading
		MockedSlotRebuilder.setReloadConfig(true);
		
		epa.execute();
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
		
		final ExecutionProfilerAction epa = new ExecutionProfilerAction(version, collectionSetId, collection, transferActionId,
				transferBatchId, connectId, rockFact, trActions, log);

		long slotCount = getRowCount("Meta_execution_slot");
		long profiCount = getRowCount("Meta_execution_slot_profile");

		// set to reload config from file and repeat-run epa.execute() to create new slots
		SlotRebuilder.setReloadConfig(true);
		createTestSlotConfIni();
		epa.execute();
		epa.execute();
		
		slotCount = getRowCount("Meta_execution_slot");
		profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 2 || slotCount != 3) {
			fail("Expected 2 profiles got " + profiCount + " and 3 slots got " + slotCount);
		}
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
	 * Simple mocked version of ExecutionProfilerAction that overrides 
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
	 * when load() is called. Used to check that ExecutionProfilerAction fails
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

