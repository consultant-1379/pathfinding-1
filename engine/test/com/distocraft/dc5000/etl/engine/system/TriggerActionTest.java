package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
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
public class TriggerActionTest {

	private static TriggerAction ta = null;
	private static Statement stm;

  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "TriggerActionTest");
	private final Mockery context = new JUnit4Mockery() {
		{
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};

	@BeforeClass
	public static void init() throws Exception {
    DirectoryHelper.mkdirs(TMP_DIR);
		StaticProperties.giveProperties(new Properties());

	    System.setProperty("dc5000.config.directory", TMP_DIR.getPath());
	    File prop = new File(TMP_DIR, "ETLCServer.properties");
		prop.deleteOnExit();
		try {
			final PrintWriter pw = new PrintWriter(new FileWriter(prop));
			pw.write("name=value");
			pw.close();
		} catch (IOException e3) {
			e3.printStackTrace();
			fail("Can't write in file");
		}

		Long collectionSetId = 1L;
		Long transferActionId = 1L;
		Long transferBatchId = 1L;
		Long connectId = 1L;
		RockFactory rockFact = null;
		Connection c;

		try {
			Class.forName("org.hsqldb.jdbcDriver");
		} catch (ClassNotFoundException e2) {
			e2.printStackTrace();
			fail("execute() failed, ClassNotFoundException");
		}

		try {
			c = DriverManager.getConnection("jdbc:hsqldb:mem:.", "SA", "");
			stm = c.createStatement();

			stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
					+ "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");

			stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");

		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("execute() failed, SQLException e1");
		}

		try {
			rockFact = new RockFactory("jdbc:hsqldb:mem:.", "SA", "", "org.hsqldb.jdbcDriver", "conName", true);
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Fail RockFactory SQLException e");
		} catch (RockException e) {
			e.printStackTrace();
			fail("Fail RockFactory RockException");
		}
		final Meta_versions version = new Meta_versions(rockFact);
		final Meta_collections collection = new Meta_collections(rockFact);
		final Meta_transfer_actions trActions = new Meta_transfer_actions(rockFact);
		trActions.setAction_contents("Monthly,Once");

		try {
			ta = new TestTriggerAction(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
					rockFact, trActions);

		} catch (EngineMetaDataException e) {
			e.printStackTrace();
			fail("execute() failed, EngineMetaDataException");
		}
	}

	@Test
	public void happyExecute() throws Exception {
		final TestScheduler ts = (TestScheduler) ta.connect();
		ts.clear();

		ta.execute();

		assertEquals(true, ts.isTriggered("Monthly"));
		assertEquals(true, ts.isTriggered("Once"));
		assertEquals(new Integer(2), ts.size());
	}

	@Test
	public void exceptionalExecute() throws Exception {
		final TestScheduler ts = (TestScheduler) ta.connect();
		ts.clear();

		ts.triggerThrow(true);

		try {

			ta.execute();
			fail("Action should throw if SchedulerAdmin throws");

		} catch (Exception e) {
			// expected
		}

	}

	@AfterClass
	public static void clean() {
    DirectoryHelper.delete(TMP_DIR);
		try {
			stm.execute("DROP TABLE Meta_collection_sets");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
