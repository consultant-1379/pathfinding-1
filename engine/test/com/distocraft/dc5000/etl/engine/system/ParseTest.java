package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineCom;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * 
 * @author ejarsok
 * 
 */

public class ParseTest {

	private static Parse parse;

	private static Statement stm;

	@BeforeClass
	public static void init() throws Exception {
		StaticProperties.giveProperties(new Properties());

		Long collectionSetId = 1L;
		Long transferActionId = 1L;
		Long transferBatchId = 1L;
		Long connectId = 1L;
		RockFactory rockFact = null;
		Logger clog = Logger.getLogger("Logger");
		SetContext sctx = new SetContext();
		EngineCom eCom = new EngineCom();

		try {
			Class.forName("org.hsqldb.jdbcDriver");
		} catch (ClassNotFoundException e2) {
			System.out.println("ONE");
			e2.printStackTrace();
			fail("init() failed, ClassNotFoundException");
		}
		Connection c;
		try {
			c = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");
			stm = c.createStatement();
            try { 
			stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
					+ "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");
            }
            catch(Exception e) {}
			stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");
            try {
			stm.execute("CREATE TABLE Meta_databases (USERNAME VARCHAR(31), VERSION_NUMBER VARCHAR(31), "
					+ "TYPE_NAME VARCHAR(31), CONNECTION_ID VARCHAR(31), CONNECTION_NAME VARCHAR(31), "
					+ "CONNECTION_STRING VARCHAR(31), PASSWORD VARCHAR(31), DESCRIPTION VARCHAR(31), DRIVER_NAME VARCHAR(31), "
					+ "DB_LINK_NAME VARCHAR(31))");
            }
            catch(Exception e){}
			stm.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', '1', 'dwhrep', "
					+ "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");

		} catch (SQLException e1) {
			System.out.println("TWO");
			e1.printStackTrace();
			fail("init() failed, SQLException");
		}

		try {
			rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
		} catch (SQLException e) {
			System.out.println("THREE");
			e.printStackTrace();
			fail("init() failed, SQLException");
		} catch (RockException e) {
			System.out.println("FOUR");
			e.printStackTrace();
			fail("init() failed, RockException");
		}
		Meta_versions version = new Meta_versions(rockFact);
		Meta_collections collection = new Meta_collections(rockFact);
		collection.setCollection_name("foobar");
		Meta_transfer_actions trActions = new Meta_transfer_actions(rockFact);
		trActions.setAction_contents("interfaceName=ifacename\n");
		ConnectionPool connectionPool = new ConnectionPool(rockFact);

		// DataFormatCache.initialize(rockFact);

		print(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions,
				connectionPool, sctx, clog, eCom);

		try {
			parse = new Parse(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact,
					trActions, connectionPool, sctx, clog, eCom);

		} catch (Exception e) {
			System.out.println("FIVE");
			e.printStackTrace();
			fail("init() failed, Exception");
		}

	}

	private static void print(final Object... objects) {
		for (int i = 0; i < objects.length; i++) {
			System.out.println(i + " \"" + objects[i] + "\"");
		}

	}

	@Test
	public void testDoesNothingToKeepAntHappy() {

	}

	@Test
	@Ignore
	public void testExecute() {
		// Parser problem
		try {
			parse.execute();
		} catch (Exception e) {
			e.printStackTrace();
			fail("testExecute() failed, Exception");
		}
	}

	@AfterClass
	public static void clean() {
		try {
			stm.execute("DROP TABLE Meta_collection_sets");
			stm.execute("DROP TABLE Meta_databases");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/*
	 * public static junit.framework.Test suite() { return new
	 * JUnit4TestAdapter(ParseTest.class); }
	 */
}
