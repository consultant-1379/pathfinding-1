package com.distocraft.dc5000.etl.engine.priorityqueue;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlot;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfile;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfileHandler;
import com.distocraft.dc5000.etl.engine.main.TransferEngine;
import com.ericsson.eniq.common.testutilities.DirectoryHelper;
import com.ericsson.eniq.common.testutilities.ServicenamesTestHelper;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

/**
 * Monitors availability of DWH database connections. If DWH database goes down
 * PriorityQueue is suspended.
 *
 * @author .
 */
public class DBConnectionMonitorTest {

  public static ExecutionSlotProfileHandler esph;
  private static Connection con = null;
  private static final Mockery context = new JUnit4Mockery();
  //  private TransferEngine mockedEngine = null;
  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "DBConnectionMonitorTest");

  private PriorityQueue mockedPriorityQueue = null;
  private ExecutionSlotProfileHandler mockedProfileHandler = null;
  private DBConnectionMonitor defaultTestInstance = null;
  private ExecutionSlotProfile mockedSlotProfile = null;
  private TransferEngine mockedEngine = null;
  private static int mockCounter = 1;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    ServicenamesTestHelper.setupEmpty(TMP_DIR);
    ServicenamesTestHelper.createDefaultNiqIni();
    StaticProperties.giveProperties(new Properties());

    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    Statement stmt = con.createStatement();
    stmt.execute("CREATE TABLE META_EXECUTION_SLOT_PROFILE (PROFILE_NAME VARCHAR(31), PROFILE_ID VARCHAR(31), "
      + "ACTIVE_FLAG VARCHAR(31))");
    stmt.execute("CREATE TABLE META_EXECUTION_SLOT (PROFILE_ID VARCHAR(31), SLOT_NAME VARCHAR(31), "
      + "SLOT_ID VARCHAR(31), ACCEPTED_SET_TYPES VARCHAR(31), SERVICE_NODE varchar(64))");
    stmt.executeUpdate("INSERT INTO META_EXECUTION_SLOT_PROFILE VALUES('profilename', 'profileid', 'y')");
    stmt.executeUpdate("INSERT INTO META_EXECUTION_SLOT VALUES('profileid', 'slotname', '0', 'testset', null)");
    stmt.execute("CREATE TABLE META_DATABASES (USERNAME VARCHAR(30), VERSION_NUMBER VARCHAR(32), "
      + "TYPE_NAME VARCHAR(15), CONNECTION_ID NUMERIC(38),CONNECTION_NAME VARCHAR(30), CONNECTION_STRING VARCHAR(400),"
      + "PASSWORD VARCHAR(30),DESCRIPTION VARCHAR(32000),DRIVER_NAME VARCHAR(100),DB_LINK_NAME VARCHAR(128))");
    stmt.executeUpdate("INSERT INTO META_DATABASES VALUES('sa','0','USER',2,'dwh','jdbc:hsqldb:mem:testdb','','The DataWareHouse Database','org.hsqldb.jdbcDriver',null)");
    stmt.executeUpdate("INSERT INTO META_DATABASES VALUES('sa','0','USER',10,'dwh_coor','jdbc:hsqldb:mem:testdb','','The DataWareHouse Database','org.hsqldb.jdbcDriver',null)");
    stmt.close();

    context.setImposteriser(ClassImposteriser.INSTANCE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    con.createStatement().execute("SHUTDOWN");
    con.close();
    DirectoryHelper.delete(TMP_DIR);
  }

  @Before
  public void setup() throws Exception {
    mockedSlotProfile = context.mock(ExecutionSlotProfile.class, "ExecutionSlotProfile_" + Integer.toString(mockCounter++));
    mockedPriorityQueue = context.mock(PriorityQueue.class, "PriorityQueue_" + Integer.toString(mockCounter++));
    mockedProfileHandler = context.mock(ExecutionSlotProfileHandler.class, "ExecutionSlotProfileHandler_" + Integer.toString(mockCounter++));
    mockedEngine = context.mock(TransferEngine.class, "TransferEngine_" + Integer.toString(mockCounter++));
    defaultTestInstance = new DBConnectionMonitor(mockedEngine, mockedPriorityQueue, mockedProfileHandler, "", "", "", "");

    Statement stmt = con.createStatement();
    stmt.executeUpdate("DELETE FROM META_EXECUTION_SLOT WHERE SERVICE_NODE='unknown_db'");
    stmt.close();
  }

  @After
  public void after() {
    mockedSlotProfile = null;
    mockedPriorityQueue = null;
    mockedProfileHandler = null;
    mockedEngine = null;
    defaultTestInstance = null;
  }

  @Test
  public void test_updateDatabases() throws Exception {

    final List<ExecutionSlot> slots = new ArrayList<ExecutionSlot>(1);
    slots.add(new ExecutionSlot(42, "TestSlot-dwhdb", "set_types", "dwhdb"));
    slots.add(new ExecutionSlot(9001, "TestSlot-ignore", "set_types", "this_should_be_ignored"));

    final ExecutionSlotProfile fff = new ExecutionSlotProfile("Normal", "0") {
      @Override
      public Iterator<ExecutionSlot> getAllExecutionSlots() {
        return slots.iterator();
      }
    };

    final Map<String, ExecutionSlotProfile> allExecSlotProfiles = new HashMap<String, ExecutionSlotProfile>();
    allExecSlotProfiles.put("Normal", fff);
    
    context.checking(new Expectations() {{
      allowing(mockedProfileHandler).getAllExecutionProfiles();
      will(returnValue(allExecSlotProfiles));
      allowing(mockedSlotProfile).getAllExecutionSlots();
      will(returnValue(slots.iterator()));
    }});

    try {
      UnitDatabaseTestCase.setup(UnitDatabaseTestCase.TestType.unit);
      final RockFactory etlrep = UnitDatabaseTestCase.getRockFactory(UnitDatabaseTestCase.Schema.etlrep);
      defaultTestInstance = new DBConnectionMonitor(mockedEngine, mockedPriorityQueue, mockedProfileHandler,
        etlrep.getDbURL(), etlrep.getUserName(), etlrep.getPassword(), etlrep.getDriverName());
      final boolean updated = defaultTestInstance.updateDatabases();
      Assert.assertTrue("Service list was not updated correctly!", updated);
      Assert.assertEquals("Service list size isnt correct!", 2, defaultTestInstance.dbconnections.size());
      Assert.assertTrue("No entry found for dwhdb", defaultTestInstance.dbconnections.containsKey("dwhdb"));
      Assert.assertTrue("No entry found for dwh_coor", defaultTestInstance.dbconnections.containsKey("dwh_coor"));
      Assert.assertFalse("Entry for 'this_should_be_ignored' found, this should not exist!",
        defaultTestInstance.dbconnections.containsKey("this_should_be_ignored"));
    } catch(Exception e) {
      e.getMessage();
    }finally {
      UnitDatabaseTestCase.__afterclass__();
    }
  }

  @Test
  public void test_isDatabase_ItsNot() throws IOException {
    do_isDatabase("engine", false);
  }

  @Test
  public void test_isDatabase_ItIs() throws IOException {
    do_isDatabase("dwhdb", true);
  }

  @Test
  public void test_isDatabase_Unknown() throws IOException {
    do_isDatabase("this_is_not_defined", false);
  }

  private void do_isDatabase(final String name, final boolean expected) throws IOException {
    final boolean checkResult = defaultTestInstance.isDatabase(name);
    Assert.assertEquals("isDatabase should have returned " + expected + "!", expected, checkResult);
  }

  @Test
  public void test_checkDataBase() throws RockException, SQLException {

    final RockFactory mockedRock = context.mock(RockFactory.class);
    final Connection mockedConnection = context.mock(Connection.class);
    final Statement mockedStatement = context.mock(Statement.class);
    final ResultSet mockedResultSet = context.mock(ResultSet.class);

    final DBConnectionMonitor.DBConnection mockedDbConnection = context.mock(DBConnectionMonitor.DBConnection.class);
    context.checking(new Expectations() {{
      allowing(mockedDbConnection).getRock();
      will(returnValue(mockedRock));

      allowing(mockedRock).getConnection();
      will(returnValue(mockedConnection));

      allowing(mockedConnection).createStatement();
      will(returnValue(mockedStatement));

      oneOf(mockedStatement).executeQuery("SELECT getdate()");
      will(returnValue(mockedResultSet));

      oneOf(mockedResultSet).next();
      will(returnValue(true));

      oneOf(mockedResultSet).close();
      oneOf(mockedStatement).close();
      oneOf(mockedConnection).close();
    }});
    defaultTestInstance.checkDataBase(mockedDbConnection);
    context.assertIsSatisfied();
  }

  @Test
  public void test_getConnectionMonitorDetails_UnAvailable() throws MalformedURLException, NotBoundException, RemoteException {
    final String dbName = "db_failed";
    final DBConnectionMonitor connectionMonitor = new DBConnectionMonitor(
      mockedEngine, mockedPriorityQueue, mockedProfileHandler, "", "", "", "") {
      @Override
      protected boolean updateDatabases() throws Exception {
        final DBConnection db_failed = new DBConnection(dbName, "", "", "", "");
        super.dbconnections.put(db_failed.getName(), db_failed);
        return true;
      }

      @Override
      protected void checkDataBase(final DBConnection dbcon) throws SQLException, RockException {
        throw new SQLException("This is an expected Exception!");
      }
    };
    context.checking(new Expectations() {{
      one(mockedPriorityQueue).setActive(true);
      oneOf(mockedProfileHandler).getActiveExecutionProfile();
      will(returnValue(mockedSlotProfile));
      oneOf(mockedSlotProfile).name();
      will(returnValue("Normal"));
      one(mockedEngine).setActiveExecutionProfile("NoLoads", false);
    }});
    connectionMonitor.run();
    context.assertIsSatisfied();
    final Map<String, String> details = DBConnectionMonitor.getConnectionMonitorDetails();
    Assert.assertNotNull("Returned Map should not be null!", details);
    Assert.assertEquals("Size of returned Map is wrong!", 1, details.size());
    Assert.assertTrue("Wrong connection information returned!", details.containsKey(dbName));
    Assert.assertEquals("Wrong database connection information returned", "UnAvailable", details.get(dbName));
  }

  @Test
  public void test_getConnectionMonitorDetails_Available() throws MalformedURLException, NotBoundException, RemoteException {
    final String dbName = "db_ok";
    final DBConnectionMonitor connectionMonitor = new DBConnectionMonitor(
      mockedEngine, mockedPriorityQueue, mockedProfileHandler, "", "", "", "") {
      @Override
      protected boolean updateDatabases() throws Exception {
        final DBConnection db_ok = new DBConnection(dbName, "", "", "", "");
        super.dbconnections.put(db_ok.getName(), db_ok);
        return true;
      }

      @Override
      protected void checkDataBase(final DBConnection dbcon) throws SQLException, RockException {
        //
      }
    };
    context.checking(new Expectations() {{
      oneOf(mockedPriorityQueue).isActive();
      will(returnValue(false));
      one(mockedPriorityQueue).setActive(true);
      oneOf(mockedProfileHandler).getActiveExecutionProfile();
      will(returnValue(mockedSlotProfile));
      oneOf(mockedSlotProfile).name();
      will(returnValue("Normal"));
      one(mockedEngine).setActiveExecutionProfile("Normal", false);
      one(mockedEngine).reloadProperties();
      one(mockedEngine).waitForCache();
    }});
    connectionMonitor.run();
    context.assertIsSatisfied();
    final Map<String, String> details = DBConnectionMonitor.getConnectionMonitorDetails();
    Assert.assertNotNull("Returned Map should not be null!", details);
    Assert.assertEquals("Size of returned Map is wrong!", 1, details.size());
    Assert.assertTrue("Wrong connection information returned!", details.containsKey(dbName));
    Assert.assertEquals("Wrong database connection information returned", "Available", details.get(dbName));
  }

  @Test
  public void test_checkOkEnablesEngine() throws MalformedURLException, NotBoundException, RemoteException {
    final String dbName = "db_ok";
    final DBConnectionMonitor connectionMonitor = new DBConnectionMonitor(
      mockedEngine, mockedPriorityQueue, mockedProfileHandler, "", "", "", "") {

      @Override
      protected boolean updateDatabases() throws Exception {
        final DBConnection db_ok = new DBConnection(dbName, "", "", "", "");
        super.dbconnections.put(db_ok.getName(), db_ok);
        return true;
      }

      @Override
      protected void checkDataBase(final DBConnection dbcon) throws SQLException, RockException {
        //
      }
    };
    context.checking(new Expectations() {{
      allowing(mockedPriorityQueue).isActive();
      will(returnValue(false));
      one(mockedPriorityQueue).setActive(true);
      oneOf(mockedProfileHandler).getActiveExecutionProfile();
      will(returnValue(mockedSlotProfile));
      oneOf(mockedSlotProfile).name();
      will(returnValue("Normal"));
      one(mockedEngine).reloadProperties();
      one(mockedEngine).waitForCache();
    }});
    context.checking(new Expectations() {{
      one(mockedEngine).setActiveExecutionProfile("Normal", false);
    }});

    connectionMonitor.run();
    context.assertIsSatisfied();
  }

  @Test
  public void test_checkOk() throws RemoteException {
    final String dbName = "db_ok";
    final DBConnectionMonitor connectionMonitor = new DBConnectionMonitor(
      mockedEngine, mockedPriorityQueue, mockedProfileHandler, "", "", "", "") {
      @Override
      protected boolean updateDatabases() throws Exception {
        final DBConnection db_ok = new DBConnection(dbName, "", "", "", "");
        super.dbconnections.put(db_ok.getName(), db_ok);
        return true;
      }

      @Override
      protected void checkDataBase(final DBConnection dbcon) throws SQLException, RockException {
        //
      }
    };
    context.checking(new Expectations() {{
      oneOf(mockedPriorityQueue).isActive();
      will(returnValue(false));
      one(mockedPriorityQueue).setActive(true);
      oneOf(mockedProfileHandler).getActiveExecutionProfile();
      will(returnValue(mockedSlotProfile));
      oneOf(mockedSlotProfile).name();
      will(returnValue("Normal"));
      one(mockedEngine).setActiveExecutionProfile("Normal", false);
      one(mockedEngine).reloadProperties();
      one(mockedEngine).waitForCache();
    }});

    connectionMonitor.run();
  }

  @Test
  public void test_checkDbError_DisablesEngine() throws MalformedURLException, NotBoundException, RemoteException {
    context.checking(new Expectations() {{
      one(mockedPriorityQueue).setActive(true);
      oneOf(mockedProfileHandler).getActiveExecutionProfile();
      will(returnValue(mockedSlotProfile));
      oneOf(mockedSlotProfile).name();
      will(returnValue("Normal"));
    }});
    context.checking(new Expectations() {{
      one(mockedEngine).setActiveExecutionProfile("NoLoads", false);
    }});
    final String dbName = "db_failed";
    final DBConnectionMonitor connectionMonitor = new DBConnectionMonitor(
      mockedEngine, mockedPriorityQueue, mockedProfileHandler, "", "", "", "") {
      @Override
      protected boolean updateDatabases() throws Exception {
        final DBConnection db_failed = new DBConnection(dbName, "", "", "", "");
        super.dbconnections.put(db_failed.getName(), db_failed);
        return true;
      }

      @Override
      protected void checkDataBase(final DBConnection dbcon) throws SQLException, RockException {
        throw new SQLException("This is an expected Exception!");
      }
    };
 
    connectionMonitor.run();
    context.assertIsSatisfied();
  }

  @Test
  public void test_updateDatabases_DisablesEngine() throws MalformedURLException, NotBoundException, RemoteException {
    context.checking(new Expectations() {{
      one(mockedPriorityQueue).setActive(true);
    }});
    context.checking(new Expectations() {{
      one(mockedEngine).setActiveExecutionProfile("NoLoads", false);
    }});
    final DBConnectionMonitor connectionMonitor = new DBConnectionMonitor(
      mockedEngine, mockedPriorityQueue, mockedProfileHandler, "", "", "", "") {
      @Override
      protected boolean updateDatabases() throws Exception {
        throw new Exception("This is an expected Exception!");
      }
    };
    connectionMonitor.run();
    context.assertIsSatisfied();
  }

  @Test
  public void test_updateDatabases_Failed() throws MalformedURLException, NotBoundException, RemoteException {
    context.checking(new Expectations() {{
      one(mockedPriorityQueue).setActive(true);
    }});
    context.checking(new Expectations() {{
      one(mockedEngine).setActiveExecutionProfile("NoLoads", false);
    }});

    final DBConnectionMonitor connectionMonitor = new DBConnectionMonitor(
      mockedEngine, mockedPriorityQueue, mockedProfileHandler, "", "", "", "") {
      @Override
      protected boolean updateDatabases() throws Exception {
        return false;
      }
    };
    connectionMonitor.run();
    context.assertIsSatisfied();
  }

  @Test
  public void testUpdateDatabases() throws Exception {

    esph = new ExecutionSlotProfileHandler("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver");
    final PriorityQueue pq = new PriorityQueue(1000, 15, new PersistenceHandler(), esph);

    context.checking(new Expectations() {{
      oneOf(mockedEngine).setActiveExecutionProfile("NoLoads", false);
      one(mockedEngine).reloadProperties();
    }});

    DBConnectionMonitor objectUnderTest = new DBConnectionMonitor(mockedEngine, pq, esph, "jdbc:hsqldb:mem:testdb", "sa", "",
      "org.hsqldb.jdbcDriver");
    objectUnderTest.run();
    final Map<String, DBConnectionMonitor.DBConnection> dbconnections = new HashMap<String, DBConnectionMonitor.DBConnection>();
    dbconnections.put("dwh", objectUnderTest.new DBConnection("dwh", "jdbc:hsqldb:mem:testdb", "sa", "",
      "org.hsqldb.jdbcDriver"));
    dbconnections.put("dwh_coor", objectUnderTest.new DBConnection("dwh_coor", "jdbc:hsqldb:mem:testdb", "sa", "",
      "org.hsqldb.jdbcDriver"));

    assertEquals(dbconnections.keySet(), objectUnderTest.dbconnections.keySet());
    assertEquals(dbconnections.get("dwh").dbdrv, objectUnderTest.dbconnections.get("dwh").dbdrv);
    assertEquals(dbconnections.get("dwh").dbpwd, objectUnderTest.dbconnections.get("dwh").dbpwd);
    assertEquals(dbconnections.get("dwh").dburl, objectUnderTest.dbconnections.get("dwh").dburl);
    assertEquals(dbconnections.get("dwh").dbusr, objectUnderTest.dbconnections.get("dwh").dbusr);
    assertEquals(dbconnections.get("dwh").name, objectUnderTest.dbconnections.get("dwh").name);

  }

  @Test
  public void testrunMethodInDbConnectionMonitorForReaderUp() throws Exception {

    esph = new ExecutionSlotProfileHandler("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver");
    final PriorityQueue pq = new PriorityQueue(1000, 15, new PersistenceHandler(), esph);
    DBConnectionMonitor objectUnderTest = new stubbedDBConnectionMonitor(mockedEngine, pq, esph, "jdbc:hsqldb:mem:testdb", "sa", "",
      "org.hsqldb.jdbcDriver");
    context.checking(new Expectations() {{
      one(mockedEngine).setActiveExecutionProfile("Normal", false);
      one(mockedEngine).reloadProperties();
      one(mockedEngine).waitForCache();
    }});
    try {
    	
      objectUnderTest.run();

      assertEquals(true, pq.isActive());

    } catch (Exception e) {
      fail("Not Supposed to get exception" + e);
    }
  }

  @Test
  public void testrunMethodInDbConnectionMonitorForReaderUpRepeatRuns() throws Exception {

    esph = new ExecutionSlotProfileHandler("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver");
    final PriorityQueue pq = new PriorityQueue(1000, 15, new PersistenceHandler(), esph);
    DBConnectionMonitor objectUnderTest = new stubbedDBConnectionMonitor(mockedEngine, pq, esph, "jdbc:hsqldb:mem:testdb", "sa", "",
      "org.hsqldb.jdbcDriver");
    
    context.checking(new Expectations() {{
      exactly(2).of(mockedEngine).setActiveExecutionProfile("Normal", false);
      exactly(2).of(mockedEngine).reloadProperties();
      exactly(2).of(mockedEngine).waitForCache();
    }});
    try {

      objectUnderTest.run();
      assertEquals(true, pq.isActive());

      objectUnderTest.run();
      assertEquals(true, pq.isActive());
    } catch (Exception e) {
      fail("Not Supposed to get exception" + e);
    }
  }

  @Test
  public void testrunMethodInDbConnectionMonitorForReaderDown() throws Exception {

    esph = new ExecutionSlotProfileHandler("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver");
    final PriorityQueue pq = new PriorityQueue(1000, 15, new PersistenceHandler(), esph);
    context.checking(new Expectations() {{
      oneOf(mockedEngine).setActiveExecutionProfile("NoLoads", false);
      oneOf(mockedEngine).waitForCache();
    }});
    DBConnectionMonitor objectUnderTest = new stubbedDBConnectionMonitor1(mockedEngine, pq, esph, "jdbc:hsqldb:mem:testdb", "sa", "",
      "org.hsqldb.jdbcDriver");

    objectUnderTest.run();

    assertEquals(true, pq.isActive());

  }

  @Test
  public void testrunMethodInDbConnectionMonitorForMissingConnection() throws Exception {

    Statement stmt = con.createStatement();
    stmt.executeUpdate("INSERT INTO META_EXECUTION_SLOT VALUES('profileid', 'slotname2', '0', 'testset', 'unknown_db')");
    stmt.close();

    esph = new ExecutionSlotProfileHandler("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver");
    final PriorityQueue pq = new PriorityQueue(1000, 15, new PersistenceHandler(), esph);
    DBConnectionMonitor objectUnderTest = new stubbedDBConnectionMonitor(mockedEngine, pq, esph, "jdbc:hsqldb:mem:testdb", "sa", "",
      "org.hsqldb.jdbcDriver");
    try {
      context.checking(new Expectations() {{
        oneOf(mockedEngine).setActiveExecutionProfile("Normal", false);
        one(mockedEngine).reloadProperties();
        one(mockedEngine).waitForCache();
      }});

      objectUnderTest.run();

      // a slot assigned to unknown db should NOT put queue to inactive
      // unknown databases are now ignored
      assertEquals(true, pq.isActive());

    } catch (Exception e) {
      // an unknown db should not cause an exception... fail if we get one
      fail("Not Supposed to get exception" + e);
    }
  }

  @Test
  public void testrunMethodInDbConnectionMonitorForMissingDwhdbStillUp() throws Exception {

    Statement stmt = con.createStatement();
    stmt.executeUpdate("INSERT INTO META_EXECUTION_SLOT VALUES('profileid', 'slotname2', '0', 'testset', 'dwhdb')");
    stmt.close();

    esph = new ExecutionSlotProfileHandler("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver");
    final PriorityQueue pq = new PriorityQueue(1000, 15, new PersistenceHandler(), esph);
    DBConnectionMonitor objectUnderTest = new stubbedDBConnectionMonitor(mockedEngine, pq, esph, "jdbc:hsqldb:mem:testdb", "sa", "",
      "org.hsqldb.jdbcDriver");
    try {
      // if the service name is dwhdb and dwhdb doesn't exist in meta_databases
      // DBConnectionMonitor should look for dwh instead and not switch to noloads
      context.checking(new Expectations() {{
        allowing(mockedEngine).setActiveExecutionProfile("Normal", false);
        allowing(mockedEngine).reloadProperties();
        allowing(mockedEngine).waitForCache();
      }});
      
      objectUnderTest.run();
      assertEquals(true, pq.isActive());
      objectUnderTest.run();
      assertEquals(true, pq.isActive());
    } catch (Exception e) {
      fail("Not Supposed to get exception" + e);
    }
  }

  private class stubbedDBConnectionMonitor extends DBConnectionMonitor {

    public stubbedDBConnectionMonitor(TransferEngine engine, PriorityQueue queue, ExecutionSlotProfileHandler profileHolder, String etlrepUrl,
                                      String etlrepUsr, String etlrepPwd, String etlrepDrv) throws Exception {
      super(engine, queue, profileHolder, etlrepUrl, etlrepUsr, etlrepPwd, etlrepDrv);
      // TODO Auto-generated constructor stub
    }

    @Override
    protected void checkDataBase(DBConnection dbcon) throws SQLException, RockException {
      final RockFactory rock = dbcon.getRock();
      final Statement stmt = rock.getConnection().createStatement();
      // junit throwing error for sql in code. So, overiding.
      ResultSet rs = stmt.executeQuery("CALL CURRENT_DATE");
       rs.next();
      stmt.close();
      rock.getConnection().close();
    }

  }

  private class stubbedDBConnectionMonitor1 extends DBConnectionMonitor {

    public stubbedDBConnectionMonitor1(TransferEngine engine, PriorityQueue queue, ExecutionSlotProfileHandler profileHolder,
                                       String etlrepUrl, String etlrepUsr, String etlrepPwd, String etlrepDrv) throws Exception {
      super(engine, queue, profileHolder, etlrepUrl, etlrepUsr, etlrepPwd, etlrepDrv);
    }

    @Override
    protected void checkDataBase(DBConnection dbcon) throws SQLException, RockException {
      throw new SQLException();
    }

  }

}
