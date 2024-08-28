package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;

import com.ericsson.eniq.common.testutilities.DirectoryHelper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * 
 * @author ejarsok
 * 
 */

public class SetTypeTriggerActionTest {

	private static TestSetTypeTriggerAction sa; // execute

	private static TestSetTypeTriggerAction sa2; // aexecute

	private static RockFactory rockFact = null;

	private static Statement stm;


  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "SetTypeTriggerActionTest");
	@BeforeClass
	public static void init() throws Exception {
    DirectoryHelper.mkdirs(TMP_DIR);
		StaticProperties.giveProperties(new Properties());
	    System.setProperty("dc5000.config.directory", TMP_DIR.getPath());
	    File prop = new File(TMP_DIR, "ETLCServer.properties");
		prop.deleteOnExit();
		try {
			PrintWriter pw = new PrintWriter(new FileWriter(prop));
			pw.write("name=value\n");
			pw.close();
		} catch (IOException e3) {
			e3.printStackTrace();
			fail("Can't write in file");
		}

		Long collectionSetId = 1L;
		Long transferActionId = 1L;
		Long transferBatchId = 1L;
		Long connectId = 1L;
		Logger clog = Logger.getLogger("Logger");

		try {
			Class.forName("org.hsqldb.jdbcDriver");
		} catch (ClassNotFoundException e2) {
			e2.printStackTrace();
			fail("init() failed, ClassNotFoundException");
		}

		Connection c;
		try {
			c = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");
			stm = c.createStatement();

			stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
					+ "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");

			stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");

			stm.execute("CREATE TABLE Meta_collections (COLLECTION_ID BIGINT, COLLECTION_NAME VARCHAR(20),"
					+ "COLLECTION VARCHAR(20), MAIL_ERROR_ADDR VARCHAR(20), MAIL_FAIL_ADDR VARCHAR(20), MAIL_BUG_ADDR VARCHAR(20),"
					+ "MAX_ERRORS BIGINT, MAX_FK_ERRORS BIGINT, MAX_COL_LIMIT_ERRORS BIGINT,"
					+ "CHECK_FK_ERROR_FLAG VARCHAR(20), CHECK_COL_LIMITS_FLAG VARCHAR(20), LAST_TRANSFER_DATE TIMESTAMP,"
					+ "VERSION_NUMBER VARCHAR(20), COLLECTION_SET_ID BIGINT, USE_BATCH_ID VARCHAR(20), PRIORITY BIGINT,"
					+ "QUEUE_TIME_LIMIT BIGINT, ENABLED_FLAG VARCHAR(20), SETTYPE VARCHAR(20), FOLDABLE_FLAG VARCHAR(20),"
					+ "MEASTYPE VARCHAR(20), HOLD_FLAG VARCHAR(20), SCHEDULING_INFO VARCHAR(20))");

			stm.executeUpdate("INSERT INTO Meta_collections VALUES('1', 'col_name', 'collection', 'me', 'mf', 'mb' ,"
					+ "5, 5, 5, 'y', 'y', '2006-10-10 14:46:40.000', '10', 1, '1', 1, 100, 'Y', 'type', 'n', 'mtype', 'y', 'info')");

			stm.execute("CREATE TABLE Meta_schedulings (VERSION_NUMBER VARCHAR(20), ID BIGINT, EXECUTION_TYPE VARCHAR(20),"
					+ "OS_COMMAND VARCHAR(20), SCHEDULING_MONTH BIGINT, SCHEDULING_DAY BIGINT, SCHEDULING_HOUR BIGINT,"
					+ "SCHEDULING_MIN BIGINT, COLLECTION_SET_ID BIGINT, COLLECTION_ID BIGINT, MON_FLAG VARCHAR(20), TUE_FLAG VARCHAR(20),"
					+ "WED_FLAG VARCHAR(20), THU_FLAG VARCHAR(20), FRI_FLAG VARCHAR(20), SAT_FLAG VARCHAR(20), SUN_FLAG VARCHAR(20),"
					+ "STATUS VARCHAR(20), LAST_EXECUTION_TIME TIMESTAMP, INTERVAL_HOUR BIGINT, INTERVAL_MIN BIGINT, NAME VARCHAR(20),"
					+ "HOLD_FLAG VARCHAR(20), PRIORITY BIGINT, SCHEDULING_YEAR BIGINT, TRIGGER_COMMAND VARCHAR(20), LAST_EXEC_TIME_MS BIGINT)");

		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("init() failed, SQLException");
		}

		try {
			rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);

		} catch (SQLException e) {
			e.printStackTrace();
			fail("init() failed, SQLException");
		} catch (RockException e) {
			e.printStackTrace();
			fail("init() failed, RockException");
		}
		Meta_versions version = new Meta_versions(rockFact);
		Meta_collections collection = new Meta_collections(rockFact);
		Meta_transfer_actions trActions = new Meta_transfer_actions(rockFact);
		Meta_transfer_actions trActions2 = new Meta_transfer_actions(rockFact);
		trActions.setAction_contents("setType=type\n");
		trActions2.setAction_contents("Monthly,Once");

		try {
			sa = new TestSetTypeTriggerAction(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
					rockFact, trActions, clog);
			sa2 = new TestSetTypeTriggerAction(version, collectionSetId, collection, transferActionId, transferBatchId,
					connectId, rockFact, trActions2, clog);
		} catch (EngineMetaDataException e) {
			e.printStackTrace();
			fail("init() failed, EngineMetaDataException");
		}
	}

	@Test
	public void testExecute() throws Exception {
		final TestScheduler ts = (TestScheduler)sa.connect();
		ts.clear();

		stm.executeUpdate("DELETE FROM Meta_schedulings");
		stm.executeUpdate("INSERT INTO Meta_schedulings VALUES('1', 1, 'wait', 'os_c', 1, 2, 3, 4, 1, 1, 'y', 'y', 'y',"
				+ "'y', 'y', 'y', 'y', 'ok', '2006-10-10 14:46:40.000', 10, 10, 'MSname1', 'y', 1, 2009, 't_co', 1234)");
		stm.executeUpdate("INSERT INTO Meta_schedulings VALUES('1', 1, 'wait', 'os_c', 1, 2, 3, 4, 1, 1, 'y', 'y', 'y',"
				+ "'y', 'y', 'y', 'y', 'ok', '2006-10-10 14:46:40.000', 10, 10, 'MSname2', 'y', 1, 2009, 't_co', 1234)");
		stm.executeUpdate("INSERT INTO Meta_schedulings VALUES('1', 1, 'foobar', 'os_c', 1, 2, 3, 4, 1, 1, 'y', 'y', 'y',"
				+ "'y', 'y', 'y', 'y', 'ok', '2006-10-10 14:46:40.000', 10, 10, 'MSname3', 'y', 1, 2009, 't_co', 1234)");

		sa.execute();

		assertEquals(new Integer(2), ts.size());

		assertEquals(true, ts.isTriggered("MSname1"));
		assertEquals(true, ts.isTriggered("MSname2"));
		assertEquals(false, ts.isTriggered("MSname3"));
		assertEquals(new Integer(2), ts.size());

	}

	@Test
	public void testExecute2() throws Exception {
		final TestScheduler ts = (TestScheduler)sa.connect();
		ts.clear();

		stm.execute("DELETE FROM Meta_schedulings");

		sa.execute();

		assertEquals(new Integer(0), ts.size());

	}

	@Test
	public void testAexecute() throws Exception {
		final TestScheduler ts = (TestScheduler)sa2.connect();
		ts.clear();

		sa2.aexecute();

		assertEquals(true, ts.isTriggered("Monthly"));
		assertEquals(true, ts.isTriggered("Once"));
		assertEquals(new Integer(2), ts.size());
		
	}

	@AfterClass
	public static void clean() {
    DirectoryHelper.delete(TMP_DIR);
		try {
			stm.execute("DROP TABLE Meta_collection_sets");
			stm.execute("DROP TABLE Meta_collections");
			stm.execute("DROP TABLE Meta_schedulings");
		} catch (SQLException e) {}
	}

	
	
}
