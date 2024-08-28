package com.distocraft.dc5000.etl.engine.executionslots;

import static org.junit.Assert.*;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.dbunit.Assertion;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.distocraft.dc5000.common.StaticProperties;
import com.ericsson.eniq.common.testutilities.DirectoryHelper;

public class ExecutionSlotProfileListTest {

  private static Field locked;
  
  private static Field exeSlotsProfileList;
  
  private static Connection c;
  
  private static Statement stm;
  
  //ExecutionSlotProfile
  private static Field executionSlotList;
  
  @BeforeClass
  public static void init() throws Exception {
    StaticProperties.giveProperties(new Properties());
    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e2) {
      e2.printStackTrace();
      fail("execute() failed, ClassNotFoundException");
    }

    try {
      c = DriverManager.getConnection("jdbc:hsqldb:mem:.", "SA", "");
      stm = c.createStatement();

      stm.execute("CREATE TABLE Meta_execution_slot_profile (PROFILE_NAME VARCHAR(20), PROFILE_ID VARCHAR(20), ACTIVE_FLAG VARCHAR(20))");

      stm.executeUpdate("INSERT INTO Meta_execution_slot_profile VALUES('Pname1', 'PID1', 'Y')");

      stm.executeUpdate("INSERT INTO Meta_execution_slot_profile VALUES('Pname2', 'PID2', 'N')");

      stm.execute("CREATE TABLE Meta_execution_slot (PROFILE_ID VARCHAR(20), SLOT_NAME VARCHAR(20), SLOT_ID VARCHAR(20),"
            + "ACCEPTED_SET_TYPES VARCHAR(20), SERVICE_NODE varchar(64))");

      stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('PID1', 'Sname', '1', 'sTypes', null)");
      
    } catch (SQLException e1) {
      e1.printStackTrace();
      fail("init() failed, SQLException e1");
    }
    
    try {
      locked = ExecutionSlotProfileList.class.getDeclaredField("locked");
      exeSlotsProfileList = ExecutionSlotProfileList.class.getDeclaredField("exeSlotsProfileList");
      
      locked.setAccessible(true);
      exeSlotsProfileList.setAccessible(true);
      
      //    ExecutionSlotProfile
      executionSlotList = ExecutionSlotProfile.class.getDeclaredField("executionSlotList");
      
      executionSlotList.setAccessible(true);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("init() failed");
    }
  }
  
  @Test
  public void testResetProfiles() {
    try {
      ExecutionSlotProfileList espl = new ExecutionSlotProfileList("jdbc:hsqldb:mem:.", "SA", "", "org.hsqldb.jdbcDriver");
      ExecutionSlotProfile esp = null;
      
      espl.resetProfiles();
      
      Vector v = (Vector) exeSlotsProfileList.get(espl);
      esp = (ExecutionSlotProfile) v.get(0);
      
      assertNotNull(esp);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testResetProfiles() failed");
    }
  }

  @Test
  public void testCreateProfile() {
    try {
      ExecutionSlotProfileList espl = new ExecutionSlotProfileList(1);
      
      Vector v = (Vector) exeSlotsProfileList.get(espl);
      ExecutionSlotProfile esp = (ExecutionSlotProfile) v.get(0);
      
      List<ExecutionSlot> esv = (List<ExecutionSlot>) executionSlotList.get(esp);
      
      ExecutionSlot ex = esv.get(0);
      
      String expected = "0,Default0";
      String actual = ex.getSlotId() + "," + ex.getName();
      
      assertEquals(expected, actual);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testCreateProfile() failed");
    }
  }

  @Test
  public void testWriteProfile() {
    try {
      ExecutionSlotProfileList espl = new ExecutionSlotProfileList("jdbc:hsqldb:mem:.", "SA", "", "org.hsqldb.jdbcDriver");
      
      ExecutionSlotProfile esp = new ExecutionSlotProfile("Pname2", "PID2");
      esp.activate();
      
      Vector v = new Vector();
      v.add(esp);
      exeSlotsProfileList.set(espl, v);

      espl.writeProfile();
      
      ResultSet rs = null;
      try {	
      	rs = stm.executeQuery("SELECT PROFILE_ID,ACTIVE_FLAG from Meta_execution_slot_profile ORDER BY PROFILE_ID");
      	
      	while (rs.next()) {
      		final String id = rs.getString(1);
      		final String flag = rs.getString(2);

      		if (id.equals("PID1") && flag.equals("N")) {
      			// ok
      		} else if (id.equals("PID2") && flag.equals("Y")) {
      			// ok
      		} else {
      			fail("Change of enable was not updated in the db");
      		}
      	}
      	
      } finally {
      	rs.close();
      }
            
    } catch (Exception e) {
      e.printStackTrace();
      fail("testWriteProfile() failed");
    }
  }

  @Test
  public void testSetActiveProfile() {
    try {
      ExecutionSlotProfileList espl = new ExecutionSlotProfileList("jdbc:hsqldb:mem:.", "SA", "", "org.hsqldb.jdbcDriver");
      
      Boolean b = espl.setActiveProfile("Pname1");
      
      assertTrue("true expecteed", b);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testSetActiveProfile() failed");
    }
  }
  
  @Test
  public void testSetActiveProfile2() {
    try {
      ExecutionSlotProfileList espl = new ExecutionSlotProfileList("jdbc:hsqldb:mem:.", "SA", "", "org.hsqldb.jdbcDriver");
      
      Boolean b = espl.setActiveProfile("foobar");
      
      assertFalse("false expecteed", b);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testSetActiveProfile() failed");
    }
  }

  @Test
  public void testGetActiveExecutionProfile() {
    try {
      ExecutionSlotProfileList espl = new ExecutionSlotProfileList(0);
      ExecutionSlotProfile ex = espl.getActiveExecutionProfile();
      
      String expected = "DefaultProfile 0";
      String actual = ex.name() + " " + ex.ID();
      
      assertEquals(expected, actual);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsProfileLocked() failed");
    }
  }

  @Test
  public void testLockProfile() {
    try {
      ExecutionSlotProfileList espl = new ExecutionSlotProfileList(0);
      espl.lockProfile();
      
      Boolean b = (Boolean) locked.get(espl);
      assertTrue("true expected", b);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsProfileLocked() failed");
    }
  }

  @Test
  public void testUnLockProfile() {
    try {
      ExecutionSlotProfileList espl = new ExecutionSlotProfileList(0);
      locked.set(espl, true);
      espl.unLockProfile();
      
      Boolean b = (Boolean) locked.get(espl);
      assertFalse("false expected", b);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsProfileLocked() failed");
    }
  }

  @Test
  public void testIsProfileLocked() {
    try {
      ExecutionSlotProfileList espl = new ExecutionSlotProfileList(0);
      
      assertFalse("false expected", espl.isProfileLocked());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsProfileLocked() failed");
    }
  }

  @AfterClass
  public static void clean() {
    try {
      stm.execute("DROP TABLE Meta_execution_slot_profile");
      stm.execute("DROP TABLE Meta_execution_slot");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
}
