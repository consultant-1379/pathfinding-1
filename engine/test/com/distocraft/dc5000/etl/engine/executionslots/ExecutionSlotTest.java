package com.distocraft.dc5000.etl.engine.executionslots;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.common.EngineCom;
import com.distocraft.dc5000.etl.engine.main.EngineThread;
import com.distocraft.dc5000.etl.engine.plugin.PluginLoader;

/**
 * 
 * @author ejarsok
 *
 */

public class ExecutionSlotTest {

  private static Field hold;
  
  private static Field runningSet;
  
  private static Field removeAfterExecution;
  
  private static Field approvedSettypes;
  
  private static Statement stm;
  
  @BeforeClass
  public static void init() {
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
          + "DESCRIPTION VARCHAR(20), VERSION_NUMBER VARCHAR(20), ENABLED_FLAG VARCHAR(20), TYPE VARCHAR(20))");

      stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");
      
      
      stm.execute("CREATE TABLE Meta_collections (COLLECTION_ID BIGINT, COLLECTION_NAME VARCHAR(20),"
          + "COLLECTION VARCHAR(20), MAIL_ERROR_ADDR VARCHAR(20), MAIL_FAIL_ADDR VARCHAR(20), MAIL_BUG_ADDR VARCHAR(20),"
          + "MAX_ERRORS BIGINT, MAX_FK_ERRORS BIGINT, MAX_COL_LIMIT_ERRORS BIGINT,"
          + "CHECK_FK_ERROR_FLAG VARCHAR(20), CHECK_COL_LIMITS_FLAG VARCHAR(20), LAST_TRANSFER_DATE TIMESTAMP,"
          + "VERSION_NUMBER VARCHAR(20), COLLECTION_SET_ID BIGINT, USE_BATCH_ID VARCHAR(20), PRIORITY BIGINT,"
          + "QUEUE_TIME_LIMIT BIGINT, ENABLED_FLAG VARCHAR(20), SETTYPE VARCHAR(20), FOLDABLE_FLAG VARCHAR(20),"
          + "MEASTYPE VARCHAR(20), HOLD_FLAG VARCHAR(20), SCHEDULING_INFO VARCHAR(20))");

      stm.executeUpdate("INSERT INTO Meta_collections VALUES('1', 'col_name', 'collection', 'me', 'mf', 'mb' ,"
          + "5, 5, 5, 'y', 'y', '2006-10-10 00:00:00.0' , '1', 1, '1', 1, 100, 'Y', 'type', 'n', 'mtype', 'y', 'info')");

      stm.execute("CREATE TABLE Meta_transfer_actions (VERSION_NUMBER VARCHAR(64), TRANSFER_ACTION_ID BIGINT, "
          + "COLLECTION_ID BIGINT, COLLECTION_SET_ID BIGINT, ACTION_TYPE VARCHAR(64), TRANSFER_ACTION_NAME VARCHAR(64), "
          + "ORDER_BY_NO BIGINT, DESCRIPTION VARCHAR(64), ENABLED_FLAG VARCHAR(64), CONNECTION_ID BIGINT, "
          + "WHERE_CLAUSE_02 VARCHAR(64), WHERE_CLAUSE_03 VARCHAR(64), ACTION_CONTENTS_03 VARCHAR(64), "
          + "ACTION_CONTENTS_02 VARCHAR(64), ACTION_CONTENTS_01 VARCHAR(64), WHERE_CLAUSE_01 VARCHAR(64))");
      
    } catch (SQLException e1) {
      e1.printStackTrace();
      fail("init() failed, SQLException");
    }
    
    
    ExecutionSlot es = new ExecutionSlot(0, null);
    Class<? extends ExecutionSlot> secretClass = es.getClass();
    
    try {
      hold = secretClass.getDeclaredField("hold");
      runningSet = secretClass.getDeclaredField("runningSet");
      removeAfterExecution = secretClass.getDeclaredField("removeAfterExecution");
      approvedSettypes = secretClass.getDeclaredField("approvedSetTypes");
      
      hold.setAccessible(true);
      runningSet.setAccessible(true);
      removeAfterExecution.setAccessible(true);
      approvedSettypes.setAccessible(true);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("init() failed");
    } 
  }
    
  @Test
  public void testIsAccepted() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    assertFalse(es.isAccepted(null));
  }
  
  @Test
  public void testIsAccepted2() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    Logger l = Logger.getLogger("Log");;
    EngineThread et = new EngineThread("ESLOT", 10L, l, new EngineCom());
    
    assertTrue("true expected", es.isAccepted(et));
  }
  
  @Test
  public void testIsAccepted3() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    Logger l = Logger.getLogger("Log");;
    EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
    
    assertFalse("false expected", es.isAccepted(et));
  }
  
  @Test
  public void testIsAccepted4() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT", "all", "dwh");
    
    Logger l = Logger.getLogger("Log");;
    EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
    
    try {
      Field shutdownSet = EngineThread.class.getDeclaredField("shutdownSet");
      
      shutdownSet.setAccessible(true);
      
      shutdownSet.set(et, false);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsAccepted3() failed");
    }
    
    assertTrue("true expected", es.isAccepted(et));
  }
  
  @Test
  public void testIsAccepted5() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT", "settype", "dwh");
    
    Logger l = Logger.getLogger("Log");;
    EngineThread et = new EngineThread("settype", 10L, l, new EngineCom());
    
    try {
      Field shutdownSet = EngineThread.class.getDeclaredField("shutdownSet");
      
      shutdownSet.setAccessible(true);
      
      shutdownSet.set(et, false);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsAccepted3() failed");
    }
    
    assertTrue("true expected", es.isAccepted(et));
  }
  
  @Test
  public void testIsAccepted6() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT", "foobar", "dwh");
    
    Logger l = Logger.getLogger("Log");;
    EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
    
    try {
      Field shutdownSet = EngineThread.class.getDeclaredField("shutdownSet");
      
      shutdownSet.setAccessible(true);
      
      shutdownSet.set(et, false);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsAccepted3() failed");
    }
    
    assertFalse("false expected", es.isAccepted(et));
  }
  
  @Test
  public void testIsFree() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");

    assertTrue("True expected", es.isFree());
  }
  
  @Test
  public void testIsFree2() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    Logger l = Logger.getLogger("Log");;
    try {
      //EngineThread et = new EngineThread("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "set_name", "col_name", new PluginLoader("pluginPath"), l, new EngineCom());
      EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
      runningSet.set(es, et);
      
      assertTrue("True expected", es.isFree());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsFree() failed");
    }
  }
  
  @Test
  public void testIsFree4() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    Logger l = Logger.getLogger("Log");;
    try {
      //EngineThread et = new EngineThread("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "set_name", "col_name", new PluginLoader("pluginPath"), l, new EngineCom());
      EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
      et.start();
      runningSet.set(es, et);
      
      assertFalse("False expected", es.isFree());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsFree() failed");
    }
  }
  
  @Test
  public void testIsFree5() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");

    try {
      hold.set(es, true);
      assertFalse("False expected", es.isFree());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsFree4() failed");
    }  
  }

  @Test
  public void testExecute() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    Logger l = Logger.getLogger("Log");
    
    try {
      EngineThread et = new EngineThread("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "set_name", "col_name", new PluginLoader("pluginPath"), null, null, l, new EngineCom());
      es.execute(et);
      EngineThread ret = (EngineThread) runningSet.get(es);
      
      assertTrue("True expected", ret.isActive());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testExecute() failed");
    }
  }
    
  @Test
  public void testRemoveAfterExecution() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    es.removeAfterExecution(true);
    
    try {
      Boolean b = (Boolean) removeAfterExecution.get(es);
      assertTrue("True expected", b);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testRemoveAfterExecution() failed");
    }
  }
  
  @Test
  public void testIsRemovedAfterExecution() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    try {
      assertFalse("False expected", es.isRemovedAfterExecution());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsRemovedAfterExecution() failed");
    }
  }
  
  @Test
  public void testHold() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    es.hold();
    try {
      Boolean b = (Boolean) hold.get(es);
      assertTrue("True expected", b);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testRemoveAfterExecution() failed");
    }
  }
  
  @Test
  public void testRestart() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    try {
      hold.set(es, true);
      es.restart();
      Boolean b = (Boolean) hold.get(es);
      assertFalse("False expected", b);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testRestart() failed");
    }
  }
  
  @Test
  public void testIsOnHold() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    assertFalse("False expected", es.isOnHold());
  }
  
  @Test
  public void testIsLocked() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    assertFalse("False expected", es.islocked());
  }
  
  @Test
  public void testSetAndGetName() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    es.setName("settedName");
    assertEquals("settedName", es.getName());
  }
  
  @Test
  public void testSetAndGetApprovedSettypes() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT", new Vector<String>(), "dwh");
    Vector<String> v = new Vector<String>();
    ArrayList<String> al = new ArrayList<String>();
    v.add("foo");
    v.add("bar");
    
    al.add("foo");
    al.add("bar");
    
    es.setApprovedSettypes(v);
    final List<String> rv = es.getApprovedSettypes();
      
    assertTrue(rv.containsAll(al));

  }
  
  @Test
  public void testGetSlotId() {
    ExecutionSlot es = new ExecutionSlot(10, "ESLOT");
    
    assertEquals(10, es.getSlotId());
  }
  
  @Test
  public void testSetApprovedSettypes() {
    ExecutionSlot es = new ExecutionSlot(10, "ESLOT");
    
    es.setApprovedSettypes("foobar");
    
    try {
      Set<String> v = (Set<String>) approvedSettypes.get(es);
      assertTrue("True expected", v.contains("foobar"));
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testSetApprovedSettypes() failed");
    }
  }
  
  @Test
  public void testSetApprovedSettypes2() {
    ExecutionSlot es = new ExecutionSlot(10, "ESLOT");
    ArrayList<String> al = new ArrayList<String>();
    al.add("foo");
    al.add("bar");
    
    es.setApprovedSettypes("foo,bar");
    
    try {
    	Set<String> v = (Set<String>) approvedSettypes.get(es);
      assertTrue("True expected", v.containsAll(al));
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testSetApprovedSettypes() failed");
    }
  }
  
  @Test
  public void testSetApprovedSettypes3() {
    ExecutionSlot es = new ExecutionSlot(10, "ESLOT", "foo,bar", "dwh");
    ArrayList<String> al = new ArrayList<String>();
    al.add("foo");
    al.add("bar");
    
    try {
      Set<String> v =  (Set<String>)approvedSettypes.get(es);
      assertTrue("True expected", v.containsAll(al));
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testSetApprovedSettypes() failed");
    }
  }
  
  @AfterClass
  public static void clean() {
    try {
      stm.execute("DROP TABLE Meta_collection_sets");
      stm.execute("DROP TABLE Meta_collections");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  @Test
  public void testToString() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    assertEquals("ExecutionSlot ESLOT ()", es.toString());
  }
  
  @Test
  public void testgetDBName() {
	ExecutionSlot es = new ExecutionSlot(10, "ESLOT");
	  
    assertEquals("dwh", es.getDBName());
  }
  
  @Test
  public void testGetRunningSet() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    
    Logger l = Logger.getLogger("Log");
    EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
    
    try {
      runningSet.set(es, et);
      es.getRunningSet();
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testGetRunningSet() failed");
    }
  }
  
  @Test
  public void testSetAndGetDBNameForDefault() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT");
    assertEquals("dwh", es.getDBName());
  }
  
  @Test
  public void testSetAndGetDBNameForDWH() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT", "X,Y", "dwh");
    assertEquals("dwh", es.getDBName());
  }
  
  @Test
  public void testSetAndGetDBNameForReader() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT", "X,Y", "reader");
    assertEquals("reader", es.getDBName());
  }
  
  @Test
  public void testSetAndGetDBNameForWriter() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT", "X,Y", "writer");
    assertEquals("writer", es.getDBName());
  }
  
  @Test
  public void testSetAndGetDBNameForEmpty() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT", "X,Y", "");
    assertEquals("dwh", es.getDBName());
  }
  
  @Test
  public void testSetAndGetDBNameForNULL() {
    ExecutionSlot es = new ExecutionSlot(0, "ESLOT", "X,Y", null);
    assertEquals("dwh", es.getDBName());
  }
  
 
  /*public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ExecutionSlotTest.class);
  }*/
}
