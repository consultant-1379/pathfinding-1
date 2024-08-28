package com.distocraft.dc5000.etl.engine.executionslots;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.distocraft.dc5000.common.StaticProperties;


public class ExecutionSlotProfileHandlerTest {
  
  // ExecutionSlotProfileHandler
  private static Field exeSlotsProfileMap;
  
  private static Field activeSlotProfile;
  
  private static Field locked;
  
  private static Field url;

  private static Field userName;

  private static Field password;

  private static Field dbDriverName;
  
  // ExecutionSlotProfile
  private static Field executionSlotList;
  
  private static Connection c;
  
  private static Statement stm;
  
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
            + "ACCEPTED_SET_TYPES VARCHAR(120), SERVICE_NODE varchar(64))");

      stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('PID1', 'Sname', '1', 'sTypes', null)");
      
    } catch (SQLException e1) {
      e1.printStackTrace();
      fail("init() failed, SQLException e1");
    }
    
    try {
      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(0);
      final Class secretClass = esph.getClass();
      
      final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
      final Class secretClass2 = esp.getClass();
      
      //    ExecutionSlotProfileHandler
      exeSlotsProfileMap = secretClass.getDeclaredField("allExecSlotProfiles");
      activeSlotProfile = secretClass.getDeclaredField("activeSlotProfile");
      locked = secretClass.getDeclaredField("locked");
      url = secretClass.getDeclaredField("url");
      userName = secretClass.getDeclaredField("userName");
      password = secretClass.getDeclaredField("password");
      dbDriverName = secretClass.getDeclaredField("dbDriverName");
      
      exeSlotsProfileMap.setAccessible(true);
      activeSlotProfile.setAccessible(true);
      locked.setAccessible(true);
      url.setAccessible(true);
      userName.setAccessible(true);
      password.setAccessible(true);
      dbDriverName.setAccessible(true);
      
      //    ExecutionSlotProfile
      executionSlotList = secretClass2.getDeclaredField("executionSlotList");
      
      executionSlotList.setAccessible(true);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("init() failed");
    }
  }

  @Test
  public void testResetProfiles() {
    try {
      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(0);
      ExecutionSlotProfile esp = null;
      
      url.set(esph, "jdbc:hsqldb:mem:.");
      userName.set(esph, "SA");
      password.set(esph, "");
      dbDriverName.set(esph, "org.hsqldb.jdbcDriver");
      
      esph.resetProfiles();
      
      final HashMap hm = (HashMap) exeSlotsProfileMap.get(esph);
      
      esp = (ExecutionSlotProfile) hm.get("Pname1");
      
      assertNotNull(esp);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testResetProfiles() failed");
    }
  }

  @Test
  public void testCreateActiveProfile() {
    try {
      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(0);
      
      final ExecutionSlotProfile esp = esph.createActiveProfile(1);
      final List<ExecutionSlot> v = (List<ExecutionSlot>) executionSlotList.get(esp);
      
      final ExecutionSlot ex = v.get(0);
      
      final String expected = "0,Default0";
      final String actual = ex.getSlotId() + "," + ex.getName();
      
      assertEquals(expected, actual);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testCreateActiveProfile() failed");
    }
  }

  @Test
  public void testWriteProfile() throws Exception {

      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler("jdbc:hsqldb:mem:.", "SA", "", "org.hsqldb.jdbcDriver");
      
      final ExecutionSlotProfile esp = new ExecutionSlotProfile("Pname2", "PID2");
      
      activeSlotProfile.set(esph, esp);
      
      // Changes Pname2 executionslotprofile active flag to y
      esph.writeProfile();
      
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

  }

  @Test
  public void testSetActiveProfileString() {
    try {
      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler("jdbc:hsqldb:mem:.", "SA", "", "org.hsqldb.jdbcDriver");
      
      final Boolean b = esph.setActiveProfile("Pname1", null);
      assertTrue("true expected", b);
        
    } catch (Exception e) {
      e.printStackTrace();
      fail("testSetActiveProfileString() failed");
    }
  }
  
  @Test
  public void testSetActiveProfileString2() {
    try {
      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(0);
      
      final Boolean b = esph.setActiveProfile("foobar", null);
      assertFalse("false expected", b);
        
    } catch (Exception e) {
      e.printStackTrace();
      fail("testSetActiveProfileString() failed");
    }
  }

  @Test
  public void testGetActiveExecutionProfile() {
    try {
      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(0);
      
      assertNotNull("Not null expected", esph.getActiveExecutionProfile());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testRemoveSlot() failed");
    }
  }

  @Test
  public void testLockProfile() {
    try {
      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(0);
      esph.lockProfile();
      
      assertTrue("True expected", esph.isProfileLocked());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testLockProfile() failed");
    }
  }

  @Test
  public void testUnLockProfile() {
    try {
      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(0);
      
      locked.set(esph, true);
      esph.unLockProfile();
      
      assertFalse("False expected", esph.isProfileLocked());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testUnLockProfile() failed");
    }
  }

  @Test
  public void testIsProfileLocked() {
    try {
      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(0);
      
      assertFalse("False expected", esph.isProfileLocked());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsProfileLocked() failed");
    }
  }

  @Test
  public void testGetNumberOfAdapterSets() {
      try {
		stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('PID1', 'Sname', '10', 'adapter,Adapter,Alarm,Install,Mediation', 'dwh')");
		stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('PID1', 'Sname', '11', 'adapter,Adapter,Alarm,Install,Mediation', 'reader')");
		stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('PID1', 'Sname', '12', 'adapter,Adapter,Alarm,Install,Mediation', 'writer')");
		stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('PID1', 'Sname', '13', 'Support,Install', null)");
	} catch (SQLException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}

    try {
      ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(0);
      ExecutionSlotProfile esp = null;
      
      url.set(esph, "jdbc:hsqldb:mem:.");
      userName.set(esph, "SA");
      password.set(esph, "");
      dbDriverName.set(esph, "org.hsqldb.jdbcDriver");
      
      esph.resetProfiles();
      esph.resetProfiles();
      
      int numberOfSets = esph.getNumberOfAdapterSlots();
      assertEquals(3, numberOfSets);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testResetProfiles() failed");
    }
  }

  @Test
  public void testGetNumberOfAggregatorSets() {
      try {
		stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('PID1', 'Sname', '10', 'adapter,Adapter,Alarm,Install,Mediation', 'dwh')");
		stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('PID1', 'Sname', '11', 'Loader,Topology,Aggregator', 'writer')");
		stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('PID1', 'Sname', '12', 'Service,Support', 'dwh')");
		stm.executeUpdate("INSERT INTO Meta_execution_slot VALUES('PID1', 'Sname', '13', 'Loader,Topology,Aggregator', 'reader')");
	} catch (SQLException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}

    try {
      ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(0);
      ExecutionSlotProfile esp = null;
      
      url.set(esph, "jdbc:hsqldb:mem:.");
      userName.set(esph, "SA");
      password.set(esph, "");
      dbDriverName.set(esph, "org.hsqldb.jdbcDriver");
      
      esph.resetProfiles();
      esph.resetProfiles();
      
      int numberOfSets = esph.getNumberOfAggregatorSlots();
      assertEquals(2, numberOfSets);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testResetProfiles() failed");
    }
  }

  
  
  /**
   * Cheks that the profile does not contain any slots marked for removal
   *
   */
  @Test
  public void testIsProfileClean() {
    try {
      final ExecutionSlotProfileHandler esph = new ExecutionSlotProfileHandler(5);
      
      esph.cleanActiveProfile();
      
      assertTrue(esph.isProfileClean());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testGetExecutionProfileNames() failed");
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
