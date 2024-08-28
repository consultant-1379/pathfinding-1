package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;


public class ConfigTest {

  private static Config conf;
  
  private static Statement stm;
  
  @BeforeClass
  public static void init() throws Exception {
    StaticProperties.giveProperties(new Properties());

    Long collectionSetId = 1L;
    Long transferActionId = 1L;
    Long transferBatchId = 1L;
    Long connectId = 1L;
    RockFactory rockFact = null;
    
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
    
    try {
      conf = new Config(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);
    } catch (EngineMetaDataException e) {
      e.printStackTrace();
      fail("init() failed");
    }
  }
  
  @Test
  public void testExecute() {
    try {
      conf.execute(); // Nothing to test
      
    } catch (EngineException e) {
      e.printStackTrace();
      fail("testExecute() failed");
    }
  }
  
  @Test
  public void testExecute2() {
    try {
      conf = new Config();
      conf.execute(); // Nothing to test
      
    } catch (EngineException e) {
      e.printStackTrace();
      fail("testExecute2() failed");
    }
  }

  @AfterClass
  public static void clean() {
    try {
      stm.execute("DROP TABLE Meta_collection_sets");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  /*public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ConfigTest.class);
  }*/
}
