package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
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
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.common.testutilities.PollerBaseTestCase;
import com.ericsson.eniq.common.testutilities.ServicenamesTestHelper;
import com.ericsson.eniq.repository.dbusers.AddServiceUser;

public class RegenerateSlotProfilesTest {

	private static Logger log = Logger.getLogger("Logger");

	private static RockFactory rockFact;

	private static final File TMPDIR = new File(System.getProperty("java.io.tmpdir"), "RegenerateSlotProfilesTest");
	private static final Integer DUMMY_CORE_COUNT = 4;
	private static final Integer DWH_WRITER_19_CORE_COUNT = 8;
	private static final Integer DWH_WRITER_20_CORE_COUNT = 4;
	private static final Integer DWH_WRITER_21_CORE_COUNT = 2;
	private static final Integer DWH_WRITER_22_CORE_COUNT = 16;

	@BeforeClass
	public static void init() throws Exception {
		StaticProperties.giveProperties(new Properties());
		Class.forName("org.hsqldb.jdbcDriver");

		rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
		final Statement stm = rockFact.getConnection().createStatement();

		try {
			stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(62), COLLECTION_SET_NAME VARCHAR(62),"
					+ "DESCRIPTION VARCHAR(62),VERSION_NUMBER VARCHAR(62),ENABLED_FLAG VARCHAR(62),TYPE VARCHAR(62))");

			stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'DWH_BASE', 'description', '1', 'N', 'type')");
			stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('2', 'DWH_BASE', 'description2', '1', 'Y', 'type')");

			stm.execute("CREATE TABLE Meta_databases (USERNAME VARCHAR(31), VERSION_NUMBER VARCHAR(31), "
					+ "TYPE_NAME VARCHAR(31), CONNECTION_ID VARCHAR(31), CONNECTION_NAME VARCHAR(31), "
					+ "CONNECTION_STRING VARCHAR(31), PASSWORD VARCHAR(31), DESCRIPTION VARCHAR(64), DRIVER_NAME VARCHAR(31), "
					+ "DB_LINK_NAME VARCHAR(31), SERVICE_NODE varchar(64))");

			stm.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', '1', 'dwhrep', "
					+ "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname', 'repdb')");

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

			stm.execute("CREATE TABLE Meta_execution_slot (PROFILE_ID VARCHAR(31), SLOT_NAME VARCHAR(31),"
					+ "SLOT_ID VARCHAR(31), ACCEPTED_SET_TYPES VARCHAR(31), SERVICE_NODE varchar(64))");

			stm.execute("CREATE TABLE Meta_execution_slot_profile (PROFILE_NAME VARCHAR(31), PROFILE_ID VARCHAR(31),"
					+ "ACTIVE_FLAG VARCHAR(31))");

			stm.execute("CREATE TABLE Configuration (PARAMNAME VARCHAR(62), PARAMVALUE VARCHAR(31))");
		} finally {
			stm.close();
		}

		System.setProperty("ETC_DIR", TMPDIR.getPath());
		System.setProperty("CONF_DIR", TMPDIR.getPath());

		ServicenamesTestHelper.setupEmpty(TMPDIR);
		ServicenamesTestHelper.createSampleServicenamesFile();
		ServicenamesTestHelper.createHostsFile_Events();
		
	}

	@AfterClass
	public static void cleanup() throws Exception {
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.execute("DROP TABLE Meta_collection_sets");
			stm.execute("DROP TABLE Meta_databases");
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
		 * ServicenamesHelperFactory is used by ExecutionProfilerAction.getCpusPerIqNode
		 * to access ServicenamesHelper.getServiceHostCoreCount
		 * Set instance provided by ServicenamesHelperFactory to "MockedServicenamesHelper"
		 * to replace real call to getServiceHostCoreCount and return test data
		 */
		ServicenamesHelperFactory.setInstance(new MockedServicenamesHelper());

		// reset niq.ini to default, single node
		ServicenamesTestHelper.createDefaultNiqIni();

		// set ExecutionProfilerAction to use mocked AddMissingServiceUsers
		SlotRebuilder.setAddMissingServiceUsers(new MockedAddServiceUser());

		// make sure dwhdb exists in meta_databases
		final Meta_databases mdCondition = new Meta_databases(rockFact);
		mdCondition.setConnection_name("dwhrep");
		mdCondition.setType_name("USER");
		final Meta_databasesFactory mdFactory = new Meta_databasesFactory(rockFact, mdCondition);
		final List<Meta_databases> databases = mdFactory.get();

		if (databases.size() == 0) {
			final Statement stm = rockFact.getConnection().createStatement();
			try {
				stm.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', '1', 'dwhrep', "
						+ "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname', 'repdb')");
			} finally {
				stm.close();
			}
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
	 * Check that slots are regenerated properly when config changes.
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_regenerateSlots() throws Exception {

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

		RegenerateSlotProfiles.regenerateSlots(rockFact);

		int expectedSlots = (int) Math.ceil(DWH_WRITER_19_CORE_COUNT * 2.125) + 
				(int) Math.ceil(DWH_WRITER_20_CORE_COUNT * 2.125) + 
				(int) Math.ceil(DWH_WRITER_21_CORE_COUNT * 2.125) + 
				(int) Math.ceil(DWH_WRITER_22_CORE_COUNT * 2.125);
		
		long slotCount = getRowCount("Meta_execution_slot");
		long profiCount =getRowCount("Meta_execution_slot_profile");

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
		
		RegenerateSlotProfiles.regenerateSlots(rockFact);

		slotCount = getRowCount("Meta_execution_slot");
		profiCount = getRowCount("Meta_execution_slot_profile");

		if(profiCount != 1 || slotCount != expectedSlots) {
			fail("Expected 1 profile got " + profiCount + " and " 
					+ expectedSlots + " slots got " + slotCount);
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

	/**
	 * Get count of number of rows in specified table
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
