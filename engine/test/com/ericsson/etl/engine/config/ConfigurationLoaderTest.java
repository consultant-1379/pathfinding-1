package com.ericsson.etl.engine.config;

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
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class ConfigurationLoaderTest {

	private static final String SOME_NEW_PARAM = "SomeNewParam";
	private static final String ANY_OTHER_VALUE = "AnyOtherValue";
	private static final File TMPDIR = new File(System.getProperty("java.io.tmpdir"), "ConfigurationLoaderTest");
	private static final String CONFIG_FILENAME = "engine_slot_configuration.ini";
	private static final String CONFIG_TABLE = "Configuration";
	private static final String PROF_CONF_PREFIX = "executionProfile.";

	private static final Logger TEST_LOG = Logger.getLogger("Logger");
	private static RockFactory rockFact;
	private static ConfigurationLoader confLoader = null;

	@BeforeClass
	public static void init(){

		System.setProperty("CONF_DIR", TMPDIR.getPath());
		try {
			setUpTestDB();
		} catch (Exception e) {
			fail("Failed to set up test database.");
		}
	}
	
	@AfterClass
	public static void cleanup() throws SQLException {
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.execute("DROP TABLE Configuration");
		} finally {
			stm.close();
			rockFact.getConnection().close();
		}
	}

	@Before
	public void setUp() throws SQLException {
		try {
			createTestSlotConfIni();
		} catch (IOException ex) {
			fail("Failed to create " + CONFIG_FILENAME + " in " + TMPDIR.getPath());
		}

		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("DELETE FROM Configuration");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.SomeParam', 'AnyValue')");
		} finally {
			stm.close();
		}
		
		confLoader = new ConfigurationLoader(rockFact, TEST_LOG);
	}
	
	/**
	 * Set up database and tables needed for tests

	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws RockException
	 */
	private static void setUpTestDB() throws ClassNotFoundException, SQLException, RockException {
		Class.forName("org.hsqldb.jdbcDriver");

		rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.execute("CREATE TABLE Configuration (PARAMNAME VARCHAR(62), PARAMVALUE VARCHAR(128) NOT NULL)");
			stm.execute("alter table Configuration add primary key (PARAMNAME)");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.0.Normal', 'Y')");
			stm.executeUpdate("INSERT INTO Configuration VALUES('executionProfile.1.NoLoads', 'N')");
		} finally {
			stm.close();
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

		final File testSlotConfIni = new File(TMPDIR, CONFIG_FILENAME);
		
		TEST_LOG.info("Creating temporary test config file: " + testSlotConfIni.getCanonicalPath());
		final PrintWriter configWriter = new PrintWriter(new FileWriter(testSlotConfIni, false));
		try{
			configWriter.println("[eniq_stats_xyz]");
			configWriter.println("executionProfile.0.Normal=Y");
			configWriter.println("executionProfile.1.NoLoads=N");
			configWriter.println("executionProfile.0.slot1.0.execute=adapter,Adapter,Alarm,Install,Mediation");
			configWriter.println("executionProfile.0.slot1.0.formula=1n");
			configWriter.println("executionProfile.0.slot1.0.type=writer");
			configWriter.println("executionProfile.0.slot2.1.execute=Loader,Topology");
			configWriter.println("executionProfile.0.slot2.1.formula=0.1n");
			configWriter.println("executionProfile.0.slot2.1.type=writer");
		} finally {
			if(configWriter != null){
				configWriter.close();
			}
			testSlotConfIni.deleteOnExit();		
		}
	}

	@Test
	public void testDeleteExistingConfiguration() throws SQLException {
		// check initial row count - there must be something to delete...
		assertTrue("No initial rows in configuration table.", (getRowCount(CONFIG_TABLE) > 0));

		// delete records
		confLoader.deleteConfigType(PROF_CONF_PREFIX);

		// check records deleted
		assertEquals("Configuration table still has records after delete.", 0, getRowCount(CONFIG_TABLE));
	}
	
	@Test
	public void testDeleteEmptyConfigurationNoErrorsThrown() throws SQLException {
		// check initial row count - there must be something to delete...
		assertTrue("No initial rows in configuration table.", (getRowCount(CONFIG_TABLE) > 0));

		// delete records
		confLoader.deleteConfigType(PROF_CONF_PREFIX);
		// check records deleted
		assertEquals("Configuration table still has records after delete.", 0, getRowCount(CONFIG_TABLE));

		// delete again to make sure it works when there is nothing to delete
		confLoader.deleteConfigType(PROF_CONF_PREFIX);
		assertEquals("Configuration table still has records after delete.", 0, getRowCount(CONFIG_TABLE));
	}

	@Test
	public void testDeleteExistingConfigurationOnlyDeletesExecutionProfiles() throws SQLException {
		// check initial row count - there must be something to delete...
		assertTrue("No initial rows in configuration table.", (getRowCount(CONFIG_TABLE) > 0));

		// add in extra records - not excecutionProfile records...
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.executeUpdate("INSERT INTO Configuration VALUES('somethingElse.SomeParam', 'AnyValue')");
		} finally {
			stm.close();
		}

		// delete records
		confLoader.deleteConfigType(PROF_CONF_PREFIX);

		// check records deleted but that there is still be one left
		assertEquals("Configuration table should still have a record.", 1, getRowCount(CONFIG_TABLE));
	}
	
	@Test
	public void testAddNewConfiguration() throws SQLException, RockException {
		final long initialRows = getRowCount(CONFIG_TABLE);
		
		confLoader.addNewConfiguration(SOME_NEW_PARAM, ANY_OTHER_VALUE);
		
		final long newRows = getRowCount(CONFIG_TABLE);
		
		assertEquals("New row not added to configuration table.", initialRows+1, newRows);
	}
	
	@Test (expected=java.sql.SQLIntegrityConstraintViolationException.class)
	public void testAddNullParamToConfigurationFails() throws SQLException, RockException {
		final long initialRows = getRowCount(CONFIG_TABLE);
		
		confLoader.addNewConfiguration(null, ANY_OTHER_VALUE);
		
		final long newRows = getRowCount(CONFIG_TABLE);
		
		assertEquals("New row was wrongly added to configuration table.", initialRows, newRows);
	}

	@Test (expected=java.sql.SQLIntegrityConstraintViolationException.class)
	public void testAddExistingParamToConfigurationFails() throws SQLException, RockException {
		final long initialRows = getRowCount(CONFIG_TABLE);
		
		confLoader.addNewConfiguration(SOME_NEW_PARAM, ANY_OTHER_VALUE);
		confLoader.addNewConfiguration(SOME_NEW_PARAM, ANY_OTHER_VALUE);
		
		final long newRows = getRowCount(CONFIG_TABLE);
		
		assertEquals("Second row wrongly added to configuration table.", initialRows+1, newRows);
	}
	
	@Test (expected=java.sql.SQLIntegrityConstraintViolationException.class)
	public void testAddEmptyValueToConfigurationFails() throws SQLException, RockException {
		final long initialRows = getRowCount(CONFIG_TABLE);
		
		confLoader.addNewConfiguration("executionProfile.SomeParam", "");
		
		final long newRows = getRowCount(CONFIG_TABLE);
		
		assertEquals("New row not added to configuration table.", initialRows, newRows);
	}

	@Test
	public void testSetDefaultConfiguration() throws SQLException, RockException {
		// set up default configuration
		// setDefaultConfiguration() should delete any existing configuration
		//first, but do it anyway, just to be sure
		confLoader.deleteConfigType(PROF_CONF_PREFIX); 
		confLoader.setDefaultSlotConf();
		
		final long expectedRows = 15;
		final long newRows = getRowCount(CONFIG_TABLE);
		// just check that something has been added - not interested in details for now
		assertEquals("Default config not added to configuration table.", expectedRows , newRows);
	}

	@Test
	public void testNoConfigFileLoadUsesDefaults() throws SQLException, RockException {

		final File testSlotConfIni = new File(TMPDIR, CONFIG_FILENAME);
		testSlotConfIni.delete();

		// load configuration from file
		confLoader.load();

		final long expectedRows = 15;
		final long newRows = getRowCount(CONFIG_TABLE);
		// just check that something has been added - not interested in details for now
		assertEquals("Default config not added to configuration table.", expectedRows , newRows);
	}
	
	@Test
	public void testEmptyConfigFileLoadUsesDefaults() throws SQLException, RockException, IOException {

		final File testSlotConfIni = new File(TMPDIR, CONFIG_FILENAME);
		testSlotConfIni.delete();
		testSlotConfIni.createNewFile();

		// load configuration from file
		confLoader.load();

		final long expectedRows = 15;
		final long newRows = getRowCount(CONFIG_TABLE);
		// just check that something has been added - not interested in details for now
		assertEquals("Default config not added to configuration table.", expectedRows , newRows);
	}
		
	@Test
	public void testLoad() throws SQLException, RockException {
		final long expectedRows = 8;
		final long initialRows = getRowCount(CONFIG_TABLE);

		// check that initial row count is not the same as those expected to be loaded
		assertTrue("Number of initial rows in configuration table matches expected number to be loaded. Test not valid.", 
				(initialRows != expectedRows));

		// load configuration from file
		confLoader.load();
		
		// just check that the new row count matches expected - not interested in details for now
		final long newRows = getRowCount(CONFIG_TABLE);
		assertEquals("New row count of configuration table different to expected.", expectedRows , newRows);
		
	}

	/**
	 * Gets count of number of rows in specified table
	 * 
	 * @param tableName Name of table 
	 * @return number of rows in the table
	 * @throws SQLException
	 */
	private long getRowCount(final String tableName) throws SQLException {
		long rowCount;
		final String sqlQuery = "select count(*) as rowcount from " + tableName;
		final Statement stm = rockFact.getConnection().createStatement();
		final ResultSet rowCountResults = stm.executeQuery(sqlQuery);

		try {
			if (rowCountResults.next()) {
				rowCount = rowCountResults.getLong("rowcount");
			} else {
				rowCount=0;
			}
		} finally {
			stm.close();
			rowCountResults.close();
		}

		return rowCount;
	}
}
