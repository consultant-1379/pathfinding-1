package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.ericsson.eniq.common.Constants;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;

/**
 * 
 * @author ejarsok
 *
 */

@RunWith (JMock.class)
public class GateKeeperActionTest {
  private Mockery context = new JUnit4Mockery();

  private Mockery concreteContext = new JUnit4Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
  }}; 

  private static Method init;

  private static Field stringDate;
  private static Field aggregationField;
  private static Field longDate;

  private static Statement stm;
  
  private static Long collectionSetId;
  private static Long transferActionId;
  private static Long transferBatchId;
  private static Long connectId;
  
  private static Logger clog;
  
  private PhysicalTableCache physicalTableCacheMock = null;
  private PhysicalTableCache resultPhysicalTableCacheMock = null;
  private Logger testLogger = Logger.getLogger("GateKeeperActionTest");
  
  private static RockFactory rockFact = null;
  
  @Before
  public void before() {
    physicalTableCacheMock = concreteContext.mock(PhysicalTableCache.class, "physicalTableCacheMock");
    resultPhysicalTableCacheMock = concreteContext.mock(PhysicalTableCache.class, "resultPhysicalTableCacheMock");
  }
  
  @BeforeClass
  public static void init() {
    
    setupDatabase();
    try{
      Class secretClass = GateKeeperAction.class;

      init = secretClass.getDeclaredMethod("init", null);
      stringDate = secretClass.getDeclaredField("stringDate");
      longDate = secretClass.getDeclaredField("longDate");
      aggregationField = secretClass.getDeclaredField("aggregation");
      
      init.setAccessible(true);
      stringDate.setAccessible(true);
      longDate.setAccessible(true);
      aggregationField.setAccessible(true);
    }catch (Exception e){
      e.printStackTrace();
      fail("init() failed, Exception");
    }
  }
  
  @After
  public void after() {
    
  }


private static GateKeeperAction setupGateKeeperAction(String setCollectionName, String setSchedulingInfo,
    String setActionContents, String setWhereClause, String threshold) {
    GateKeeperAction gateKeeperAction = null;

    Properties prop = new Properties();
    prop.setProperty("GateKeeper.thresholdLimit", threshold);

    try {
      StaticProperties.giveProperties(prop);
    } catch (Exception e3) {
      e3.printStackTrace();
      fail("StaticProperties failed");
    }

    Meta_versions version = new Meta_versions(rockFact);
    Meta_collections collection = new Meta_collections(rockFact);
    Meta_transfer_actions trActions = new Meta_transfer_actions(rockFact);
    ConnectionPool connectionPool = new ConnectionPool(rockFact);

    collection.setCollection_name(setCollectionName);
    collection.setScheduling_info(setSchedulingInfo);
    trActions.setAction_contents(setActionContents);
    trActions.setWhere_clause(setWhereClause);

    try {
      gateKeeperAction = new GateKeeperAction(version, collectionSetId, collection, transferActionId, transferBatchId,
          connectId, rockFact, connectionPool, trActions, clog);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    return gateKeeperAction;
  }


private static void setupDatabase() {

    collectionSetId = 1L;
    transferActionId = 1L;
    transferBatchId = 1L;
    connectId = 1L;

    clog = Logger.getLogger("Logger");

    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e2) {
      e2.printStackTrace();
      fail("init() failed, ClassNotFoundException");
    }
    Connection c;
    

    try {
      c = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");

      AggregationStatusCache.init("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver");
    PhysicalTableCache.initialize(rockFact, "jdbc:hsqldb:mem:testdb", "SA", "", "jdbc:hsqldb:mem:testdb", "SA", "");

      stm = c.createStatement();
      
      //Create the LOG_AGGREGATIONSSTATUS table...      
      stm.execute("create table LOG_AGGREGATIONSTATUS(AGGREGATION varchar(255),"+
          "TYPENAME varchar(255)," +
          "TIMELEVEL varchar(10),"+
          "DATADATE date,"+
          "DATE_ID date,"+
          "INITIAL_AGGREGATION timestamp,"+
        "STATUS varchar(16),"+
        "DESCRIPTION varchar(250),"+
        "ROWCOUNT integer,"+
        "AGGREGATIONSCOPE varchar(50),"+
        "LAST_AGGREGATION timestamp,"+
        "LOOPCOUNT integer,"+
        "THRESHOLD timestamp)");
      
      
      stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
          + "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");

      stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");
      
      
      stm.execute("CREATE TABLE Meta_databases (USERNAME VARCHAR(31), VERSION_NUMBER VARCHAR(31), "
          + "TYPE_NAME VARCHAR(31), CONNECTION_ID VARCHAR(31), CONNECTION_NAME VARCHAR(31), "
          + "CONNECTION_STRING VARCHAR(31), PASSWORD VARCHAR(31), DESCRIPTION VARCHAR(31), DRIVER_NAME VARCHAR(31), "
          + "DB_LINK_NAME VARCHAR(31))");
      
      stm.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'typenames', '1', 'connectionname', "
          + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");

    
      stm.execute("CREATE TABLE LOG_AGGREGATIONRULES (AGGREGATION VARCHAR(20), TARGET_TYPE VARCHAR(20),"
          + "TARGET_LEVEL VARCHAR(20))");

      stm.executeUpdate("INSERT INTO LOG_AGGREGATIONRULES VALUES('aggre', 'type', 'level')");
      
      rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
    } catch (Exception e) {
      e.printStackTrace();
      fail("init() failed, Exception");
    }
}

  
  @Test
  public void testIsGateClosed() {
    GateKeeperAction gka = new GateKeeperAction(testLogger);
    assertEquals(true, gka.isGateClosed());
  }

  @Test
  public void testInit() {
    GateKeeperAction gateKeeperAction = null;
    
      String setCollectionName = "aggre";
      String retries = "180";
      String setSchedulingInfo = "aggDate=100000\n";
      String setActionContents = "SELECT count(*) result FROM LOG_AGGREGATIONSTATUS WHERE TYPENAME = 'DC_E_BSS_BSC'"
          + "AND TIMELEVEL = 'DAY' AND DATADATE = $date AND AGGREGATIONSCOPE = 'DAY' AND STATUS NOT IN ('AGGREGATED')"
          + "UNION ALL SELECT count(*) result FROM DC_E_BSS_BSC_RAW WHERE DATE_ID = $date ";
      String setWhereClause = "foobar";
      
      gateKeeperAction = setupGateKeeperAction(setCollectionName, setSchedulingInfo,
        setActionContents, setWhereClause, retries);

    try {
      init.invoke(gateKeeperAction, null);
      
      assertEquals("1970-01-01 01:01:40", stringDate.get(gateKeeperAction));
      assertEquals(100000L, longDate.get(gateKeeperAction));

    } catch (Exception e) {
      e.printStackTrace();
      fail("testInit() failed");
    }
  }

  @Test
  public void testConvert() {
    GateKeeperAction gka = new GateKeeperAction(testLogger);
    String s = gka.convert("5.5.2008", "date is $date");
    assertEquals("date is '5.5.2008'", s);
  }
  
  /**
   * There will be a few exceptions thrown during this test case. Please ignore as they are 
   * only because the PhysicalTableCache is not setup for this test. 
   * Aggregation should be set to blocked even if it is run several times. It should not go to FAILEDDEPENDENCY
   * after 5 tries.
   */
  @Test
  public void testExecuteSeveralTimes() {
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      final long longDate = sdf.parse("2010-11-23 00:00:00").getTime();

      GateKeeperAction gateKeeperAction = null;
      
      //Add data into the LOG_AGGREGATIONSTATUS table...
      //Threshold is not set (0 value).      
      stm.executeUpdate("INSERT INTO LOG_AGGREGATIONSTATUS VALUES('DC_E_CPP_VCLTP_DAYBH_VCLTP', 'DC_E_CPP_VCLTP', 'DAYBH', '2010-11-23', '2010-11-23', null, " +
      "'BLOCKED', null, 0, 'DAY', null, 0, null)");

      String setCollectionName = "DC_E_CPP_VCLTP_DAYBH_VCLTP";
      String thresholdLimit = "180";      

      String setSchedulingInfo = "aggDate="+longDate+"\n";
      String setActionContents = "SELECT count(*) result FROM LOG_AGGREGATIONSTATUS WHERE AGGREGATION = 'DC_E_CPP_VCLTP_DAYBH_VCLP'"; //This is done on purpose 
      String setWhereClause = "foobar";

      gateKeeperAction = setupGateKeeperAction(setCollectionName, setSchedulingInfo,
          setActionContents, setWhereClause, thresholdLimit);

      aggregationField.set(gateKeeperAction, setCollectionName);

      // Execute the gatekeeper 6 times (more than the old loopcount limit of 4):
      for (int i =0; i<6; i++) {
        gateKeeperAction.execute();   
      }
      
      AggregationStatus aggregationStatus = AggregationStatusCache.getStatus(setCollectionName, longDate);
      assertFalse("Aggregation status should not be FAILEDDEPENDENCY if gatekeeper is executed several times", 
          "FAILEDDEPENDENCY".equals(aggregationStatus.STATUS));     
      assertTrue("Aggregation status should be BLOCKED if gatekeeper is executed several times", 
          "BLOCKED".equals(aggregationStatus.STATUS));
      
      assertTrue("Aggregation loop count should be 6 if gatekeeper is executed 6 times", 
          aggregationStatus.LOOPCOUNT == 6);
    } catch (Exception e) {
      e.printStackTrace();
      fail("testExecute() failed, Exception");
    }
  }
  
  /**
   * If the threshold time limit has passed, the status of the aggregation
   * should be FAILEDDEPENDENCY.
   */
  @Test
  public void testExecuteThresholdInPast() {
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      final long longDate = sdf.parse("2011-03-25 00:00:00").getTime();

      GateKeeperAction gateKeeperAction = null;
      
      // Set up the threshold time as being one minute ago: 
      Calendar timeLimit = Calendar.getInstance();
      timeLimit.add(Calendar.MINUTE, -1);
      long test = timeLimit.getTimeInMillis();
      Date newtest = new Date(test);
      String thedate = sdf.format(newtest);
      
      //Add data into the LOG_AGGREGATIONSTATUS table...
      //Threshold is set to one minute ago:         
      stm.executeUpdate("INSERT INTO LOG_AGGREGATIONSTATUS VALUES('DC_E_CPP_VCLTP_DAYBH_VCLTP', 'DC_E_CPP_VCLTP', 'DAYBH', '2011-03-25', '2011-03-25', null, " +
      "'BLOCKED', null, 0, 'DAY', null, 0, '" + thedate + "')");

      String setCollectionName = "DC_E_CPP_VCLTP_DAYBH_VCLTP";
      String thresholdLimit = "1";  // This will not be used here because we won't be setting the threshold (it's already set in the insert statement above).    

      String setSchedulingInfo = "aggDate="+longDate+"\n";
      String setActionContents = "SELECT count(*) result FROM LOG_AGGREGATIONSTATUS WHERE AGGREGATION = 'DC_E_CPP_VCLTP_DAYBH_VCLP'"; //This is done on purpose 
      String setWhereClause = "foobar";

      gateKeeperAction = setupGateKeeperAction(setCollectionName, setSchedulingInfo,
          setActionContents, setWhereClause, thresholdLimit);

      aggregationField.set(gateKeeperAction, setCollectionName);

      // Execute the gatekeeper:
      gateKeeperAction.execute();
      
      AggregationStatus aggregationStatus = AggregationStatusCache.getStatus(setCollectionName, longDate);
      assertTrue("Aggregation status should be FAILEDDEPENDENCY if threshold time limit is exceeded", 
          "FAILEDDEPENDENCY".equals(aggregationStatus.STATUS));
    } catch (Exception e) {
      e.printStackTrace();
      fail("testExecute() failed, Exception");
    }
  }
  
  @Test
  public void testSetAggToClosedStatus() {       
    GateKeeperAction gateKeeperAction = new GateKeeperAction(testLogger);
    gateKeeperAction.setProperties(new Properties());   
    
    final AggregationStatus aggStatus = new AggregationStatus();
    // Give the threshold a value (0 = not set).
    aggStatus.THRESHOLD = 0l;
    aggStatus.STATUS = "test";
    final int thresholdValue = 60;
    
    // If time limit is not defined, status should be set to BLOCKED:
    final AggregationStatus result = gateKeeperAction.setAggToClosedStatus(aggStatus, thresholdValue, Constants.AGG_BLOCKED_STATUS, 
        Constants.AGG_FAILED_STATUS, Constants.AGG_FAILED_DEPENDENCY_STATUS);
    assertTrue("Aggregation should be in status BLOCKED if time limit has not been set yet.", 
        result.STATUS.equals(Constants.AGG_BLOCKED_STATUS));
  }
  
  @Test
  public void testSetAggToClosedStatusSetsThreshold() {
    try {
      GateKeeperAction gateKeeperAction = new GateKeeperAction(testLogger);
      gateKeeperAction.setProperties(new Properties());

      final AggregationStatus aggStatus = new AggregationStatus();
      aggStatus.THRESHOLD = 0l;
      // aggStatus.STATUS = "LOADED";
      final int thresholdValue = 60;

      // If the threshold time value is 0 then it should be set:
      final AggregationStatus result = gateKeeperAction.setAggToClosedStatus(aggStatus, thresholdValue, Constants.AGG_BLOCKED_STATUS, 
          Constants.AGG_FAILED_STATUS, Constants.AGG_FAILED_DEPENDENCY_STATUS);
      assertTrue("Threshold time limit should be set if it's not set already", result.THRESHOLD != 0);
    } catch (Exception exc) {
      fail(exc.toString());
    }
  }   
  
  @Test
  public void testSetAggToClosedStatusLimitPassed() {
    GateKeeperAction gateKeeperAction = new GateKeeperAction(testLogger) {

      protected long getCurrentTime() {
        // Return a time greater than the threshold time that has been set up:
        return 2l;
      }
    };    
    gateKeeperAction.setProperties(new Properties());

    final AggregationStatus aggStatus = new AggregationStatus();

    // Give the threshold a value:
    aggStatus.THRESHOLD = 1l;
    final int thresholdValue = 60;

    // If time limit has been passed, status should be set to
    // FAILEDDEPENDENCY:
    final AggregationStatus result = gateKeeperAction.setAggToClosedStatus(aggStatus, thresholdValue, Constants.AGG_BLOCKED_STATUS, 
        Constants.AGG_FAILED_STATUS, Constants.AGG_FAILED_DEPENDENCY_STATUS);
    assertTrue("Aggregation should be in status FAILEDDEPENDENCY if time limit has been passed",
        result.STATUS.equals(Constants.AGG_FAILED_DEPENDENCY_STATUS));
  }
  
  @Test
  public void testSetAggToClosedStatusLimitNotPassed() {       
    GateKeeperAction gateKeeperAction = new GateKeeperAction(testLogger) {
      
      protected long getCurrentTime() {
        // Return a time less than the threshold time that has been set up:
        return 100l;
      }     
    };
    gateKeeperAction.setProperties(new Properties());
    
    final AggregationStatus aggStatus = new AggregationStatus();
    // Give the threshold a value. 
    aggStatus.THRESHOLD = 1000l;
    final int thresholdValue = 60;
    
    // If time limit is defined but has not been passed, status should be set to BLOCKED:
    final AggregationStatus result = gateKeeperAction.setAggToClosedStatus(aggStatus, thresholdValue, Constants.AGG_BLOCKED_STATUS, 
        Constants.AGG_FAILED_STATUS, Constants.AGG_FAILED_DEPENDENCY_STATUS);
    assertTrue("Aggregation should be in status BLOCKED if time limit has not been passed", 
        result.STATUS.equals(Constants.AGG_BLOCKED_STATUS));
  }
  
    
  @AfterClass
  public static void clean() {
    try {
      stm.execute("DROP TABLE Meta_collection_sets");
      stm.execute("DROP TABLE Meta_databases");
      stm.execute("DROP TABLE LOG_AGGREGATIONRULES");
      stm.execute("DROP TABLE LOG_AGGREGATIONSTATUS");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
