package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.ericsson.eniq.common.testutilities.DirectoryHelper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * 
 * @author ejarsok
 * 
 */

public class TriggerSetListInSchedulerActionTest {

	private static TriggerSetListInSchedulerAction ta;
	private static TriggerSetListInSchedulerAction ta2;

	private static Statement stm;
	private static RockFactory rockFactory;

	private static SetContext sctx;
	private static Meta_versions version;
	private static Meta_collections whereCollection;
	private static Meta_collectionsFactory mcf;
	private static Meta_collections collection;
	private static Meta_transfer_actions trActions;
	private static Long collectionSetId;
	private static Long transferActionId;
	private static Long transferBatchId;
	private static Long connectId;

  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "TriggerSetListInSchedulerActionTest");
	
	@BeforeClass
	public static void init() {
    DirectoryHelper.mkdirs(TMP_DIR);
		StaticProperties.giveProperties(new Properties());

	    System.setProperty("dc5000.config.directory", TMP_DIR.getPath());
	    File prop = new File(TMP_DIR, "ETLCServer.properties");
		prop.deleteOnExit();
		try {
			PrintWriter pw = new PrintWriter(new FileWriter(prop));
			pw.write("name=value");
			pw.close();
		} catch (IOException e3) {
			e3.printStackTrace();
			fail("Can't write in file");
		}

		collectionSetId = 1L;
		transferActionId = 1L;
		transferBatchId = 1L;
		connectId = 1L;
		ta = null;
		sctx = new SetContext();
		Connection c;

		try {
			Class.forName("org.hsqldb.jdbcDriver");
		} catch (ClassNotFoundException e2) {
			e2.printStackTrace();
			fail("init() failed, ClassNotFoundException");
		}

		try {
			c = DriverManager.getConnection("jdbc:hsqldb:mem:testDB", "SA", "");
			stm = c.createStatement();

			stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
					+ "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");

			stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");

			stm.execute("CREATE TABLE Meta_collections (COLLECTION_ID bigint, COLLECTION_NAME varchar(31), COLLECTION varchar(31), MAIL_ERROR_ADDR varchar(31), "
					+ "MAIL_FAIL_ADDR varchar(31), MAIL_BUG_ADDR varchar(31), MAX_ERRORS bigint, MAX_FK_ERRORS bigint, MAX_COL_LIMIT_ERRORS bigint, "
					+ "CHECK_FK_ERROR_FLAG varchar(1), CHECK_COL_LIMITS_FLAG varchar(1),LAST_TRANSFER_DATE timestamp,VERSION_NUMBER varchar(31), "
					+ "COLLECTION_SET_ID bigint, USE_BATCH_ID varchar(1), PRIORITY int, QUEUE_TIME_LIMIT int, ENABLED_FLAG varchar(1), SETTYPE varchar(10), "
					+ "FOLDABLE_FLAG varchar(1),MEASTYPE varchar(30), HOLD_FLAG varchar(1), SCHEDULING_INFO varchar(128))");
			try {
			stm.execute("CREATE TABLE META_DATABASES (USERNAME varchar(30), VERSION_NUMBER varchar(32), TYPE_NAME varchar(15), CONNECTION_ID numeric(32), CONNECTION_NAME varchar(30), CONNECTION_STRING varchar(200), PASSWORD varchar(30), DESCRIPTION varchar (32000), DRIVER_NAME varchar (100), DB_LINK_NAME varchar(128));");
			}
			catch(Exception e){}
			stm.executeUpdate("INSERT INTO Meta_collections (COLLECTION_ID, COLLECTION_NAME, COLLECTION_SET_ID, SETTYPE) VALUES(1, 'Loader_DC_E_TEST_MEASUREMENT', '1', 'Loader')");

			stm.execute("create table LOG_AGGREGATIONRULES (AGGREGATION varchar(255), TARGET_TABLE varchar(50))");
			stm.executeUpdate("INSERT INTO LOG_AGGREGATIONRULES values ('DC_E_MGW_REMOTEMSC_DAY', 'DC_E_MGW_REMOTEMSC_DAY')");
			stm.executeUpdate("INSERT INTO LOG_AGGREGATIONRULES values ('DC_E_MGW_REMOTEMSC_COUNT', 'DC_E_MGW_REMOTEMSC_COUNT')");
			stm.executeUpdate("INSERT INTO LOG_AGGREGATIONRULES values ('DC_E_MGW_REMOTEMSC_RAW', 'DC_E_MGW_REMOTEMSC_RAW')");
			stm.executeUpdate("INSERT INTO LOG_AGGREGATIONRULES values ('DC_E_MGW_REMOTEMSC_DAYBH', 'DC_E_MGW_REMOTEMSC_DAYBH')");
			stm.executeUpdate("insert into META_COLLECTIONS (COLLECTION_ID, COLLECTION_NAME, MAX_ERRORS, MAX_FK_ERRORS, MAX_COL_LIMIT_ERRORS, CHECK_FK_ERROR_FLAG, CHECK_COL_LIMITS_FLAG, VERSION_NUMBER, COLLECTION_SET_ID, PRIORITY, QUEUE_TIME_LIMIT, ENABLED_FLAG, SETTYPE, FOLDABLE_FLAG, HOLD_FLAG) values ('9', 'DWHM_Partition_DWH_BASE', '0', '0', '0', 'N', 'N', 'R3B_b68', '0', '3', '30', 'Y', 'Partition', 'Y', 'N');");
			stm.executeUpdate("INSERT INTO META_DATABASES (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME, DB_LINK_NAME) values ('SA', 0, 'USER', 0, 'dwh', 'jdbc:hsqldb:mem:testDB', '', 'DatabaseConnections_DWHDB', 'org.hsqldb.jdbcDriver', null)");

		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("init() failed, SQLException e1");
		}

		try {
			rockFactory = new RockFactory("jdbc:hsqldb:mem:testDB", "SA", "", "org.hsqldb.jdbcDriver", "conName", true);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Fail RockFactory Exception e");
		}

		version = new Meta_versions(rockFactory);
		whereCollection = new Meta_collections(rockFactory);
		try {
			mcf = new Meta_collectionsFactory(rockFactory, whereCollection);
			collection = (Meta_collections) mcf.get().firstElement();
			trActions = new Meta_transfer_actions(rockFactory);
			Meta_transfer_actions trActions2 = new Meta_transfer_actions(rockFactory);
			trActions.setAction_contents("Monthly,Once");
			trActions2.setAction_contents("Monthly,Once");
			try {
				ta = new TestTriggerSetListInSchedulerAction(version, collectionSetId, collection, transferActionId,
						transferBatchId, connectId, rockFactory, trActions, sctx);
				ta2 = new TestTriggerSetListInSchedulerAction(version, collectionSetId, collection, transferActionId,
						transferBatchId, connectId, rockFactory, trActions2, sctx);

			} catch (Exception e) {
				e.printStackTrace();
				fail("init() failed");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		/* Creating ETLC property file */
		File ETLCConfFile = new File(TMP_DIR, "ETLCServer.properties");
		ETLCConfFile.deleteOnExit();
		try {
			PrintWriter pw = new PrintWriter(new FileWriter(ETLCConfFile));
			pw.print("ENGINE_DB_URL=jdbc:hsqldb:mem:testDB\n");
			pw.print("ENGINE_DB_USERNAME=sa\n");
			pw.print("ENGINE_DB_PASSWORD=");
			pw.print("\n");
			pw.print("ENGINE_DB_DRIVERNAME=org.hsqldb.jdbcDriver\n");
			pw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testExecute() throws Exception {
		final TestScheduler ts = (TestScheduler)ta.connect();
		ts.clear();

		ta.execute();

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
			stm.execute("DROP TABLE META_DATABASES");
			try{
			stm.execute("DROP TABLE LOG_AGGREGATIONRULES");
			}
			catch(Exception e){}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void getAggregationBaseTableTestSuc() throws Exception {
		// Setting up dummy ETServer.properties file
		final String propToChange = "CONF_DIR";
		final String prevPro = System.getProperty(propToChange);
		System.setProperty("CONF_DIR",  TMP_DIR.getPath());

		/* Initializing test object */
		ta = new TriggerSetListInSchedulerAction(version, collectionSetId, collection, transferActionId, transferBatchId,
				connectId, rockFactory, trActions, sctx);
		final String aggregation = "Aggregator_DC_E_MGW_REMOTEMSC_DAY";
		final String expected = "DC_E_MGW_REMOTEMSC_DAY";
		final Method getAggBaseTable = TriggerSetListInSchedulerAction.class.getDeclaredMethod("getAggregationBaseTable",
				new Class[] { String.class });
		getAggBaseTable.setAccessible(true);
		final String actual = (String) getAggBaseTable.invoke(ta, aggregation);
		resetChangedProperty(propToChange, prevPro);
		assertEquals(expected, actual);
	}

	// To reset the CONF_DIR property if used by some other test
	private void resetChangedProperty(String propName, String propValue) {
		if (propValue != null)
			System.setProperty(propName, propValue);
	}

	@Test
	public void getAggregationBaseTableTestInvalidAgg() throws Exception {
		/* Initializing test object */
		ta = new TriggerSetListInSchedulerAction(version, collectionSetId, collection, transferActionId, transferBatchId,
				connectId, rockFactory, trActions, sctx);
		final String aggregation = "Aggregator_DC_E_TEST";
		final String expected = "";
		final Method getAggBaseTable = TriggerSetListInSchedulerAction.class.getDeclaredMethod("getAggregationBaseTable",
				new Class[] { String.class });
		getAggBaseTable.setAccessible(true);
		final String actual = (String) getAggBaseTable.invoke(ta, aggregation);
		assertEquals(expected, actual);
	}

}
