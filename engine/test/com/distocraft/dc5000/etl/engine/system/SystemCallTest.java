package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;

import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.distocraft.dc5000.etl.engine.main.EngineAdminFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.ServicenamesTestHelper;

public class SystemCallTest {

	private static RockFactory rockFact;
	private Mocked_meta_Transfer_actions mockedMTA;

	final Meta_versions version = new Meta_versions(rockFact);
	final Long collectionSetId=1L;
	final Meta_collections collection = new Meta_collections(rockFact);
	final Long transferActionId=1L;
	final Long transferBatchId=1L;
	final Long connectId=1L;

	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		StaticProperties.giveProperties(new Properties());
		Class.forName("org.hsqldb.jdbcDriver");

		rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
		final Statement stm = rockFact.getConnection().createStatement();

		try {
			stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(62), COLLECTION_SET_NAME VARCHAR(62),"
					+ "DESCRIPTION VARCHAR(62),VERSION_NUMBER VARCHAR(62),ENABLED_FLAG VARCHAR(62),TYPE VARCHAR(62))");
	
			stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");
		} finally {
			stm.close();
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		final Statement stm = rockFact.getConnection().createStatement();
		try {
			stm.execute("DROP TABLE Meta_collection_sets");
		} finally {
			stm.close();
		}
		rockFact.getConnection().close();
	}

	@Before
	public void setUp() throws Exception {
		mockedMTA = new Mocked_meta_Transfer_actions(rockFact);
	}

	/**
	 * SystemCall should not fail if action Contents contains
	 * under three words 
	 */
	@Test
	public void executeWorksWithLessThanThreeItemsInSystemCall() throws Exception {
		try {
			String actionContents = "java -version";
			mockedMTA.setActionContents(actionContents);
			SystemCall testSystemCall = new SystemCall(version, collectionSetId, collection,
				transferActionId, transferBatchId, connectId, rockFact, mockedMTA);
			testSystemCall.execute();
		} catch (Exception e) {
			fail("SystemCall failed for two argument action");
		}
	}

	/**
	 * SystemCall should call EngineAdmin.reloadAggregationCache() if third
	 * "word" in actionContents equals reloadAggregationCache
	 */
	@Test
	public void executeCallsReloadAggregationCache() throws Exception {
		
		String actionContents = "java -version reloadAggregationCache";
		mockedMTA.setActionContents(actionContents);
		
		MockedEngineAdmin mockedEngineAdmin = new MockedEngineAdmin("system call");
		EngineAdminFactory.setInstance(mockedEngineAdmin);
		
		SystemCall testSystemCall = new SystemCall(version, collectionSetId, collection,
			transferActionId, transferBatchId, connectId, rockFact, mockedMTA);
		testSystemCall.execute();
		
		assertEquals("reloadAggregationCache", mockedEngineAdmin.getTestCheck());
	}	
	
	/**
	 * SystemCall should ONLY call EngineAdmin.reloadAggregationCache() if third
	 * "word" in actionContents equals reloadAggregationCache.
	 * If third word is anything else, it should run the command as a system call
	 */
	@Test
	public void executeOnlyCallsReloadAggregationCacheWhenNeeded() throws Exception {
		String actionContents = "java -version somethingelse";
		mockedMTA.setActionContents(actionContents);
		
		MockedEngineAdmin mockedEngineAdmin = new MockedEngineAdmin("system call");
		EngineAdminFactory.setInstance(mockedEngineAdmin);
		
		SystemCall testSystemCall = new SystemCall(version, collectionSetId, collection,
			transferActionId, transferBatchId, connectId, rockFact, mockedMTA);
		testSystemCall.execute();
		
		assertEquals("system call", mockedEngineAdmin.getTestCheck());
	}	
	
	/**
	 * SystemCall should fail if running actionContents fails
	 *  
	 * @throws EngineMetaDataException 
	 * @throws EngineException 
	 */
	@Test(expected=EngineException.class)
	public void executeFailureCausesException() throws EngineMetaDataException, EngineException {
		String actionContents = "ThisCommandShouldNotExistAndShouldCauseException";
		mockedMTA.setActionContents(actionContents);
		SystemCall testSystemCall = new SystemCall(version, collectionSetId, collection,
			transferActionId, transferBatchId, connectId, rockFact, mockedMTA);
		testSystemCall.execute();
	}
		
    /** 
	 * Mocked version of meta_transfer_actions to use testing SystemCall.
	 * use setActionContents() to set the value returned from the 
	 * mocked getAction_contents()
	 */
	class Mocked_meta_Transfer_actions extends Meta_transfer_actions {

		String actionContents=null;
		
		public Mocked_meta_Transfer_actions(RockFactory rockFact) {
			super(rockFact);
		}

		public void setActionContents(String actionContents) {
			this.actionContents = actionContents;
		}

		@Override
		public String getAction_contents() {
			return actionContents;
		}
	}

	/** 
	 * Mocked version of EngineAdmin
	 * 
	 */
	class MockedEngineAdmin extends EngineAdmin {
		
		String testCheck = "test check";
		
		MockedEngineAdmin(String testCheck) {
			this.testCheck = testCheck;
		}
		
		public void reloadAggregationCache() {
			testCheck = "reloadAggregationCache";
		}
		
		public String getTestCheck() {
			return testCheck;
		}
	}
	
}
