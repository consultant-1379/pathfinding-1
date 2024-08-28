package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;

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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.Constants;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.main.TestITransferEngineRMI;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * 
 * @author ejarsok
 * 
 */

public class ChangeProfileActionTest {

  private static ChangeProfileAction cpa;

  private static Statement stm;
  
//  private static Map<String, String> env = System.getenv();

  @BeforeClass
  public static void init() throws Exception {
    StaticProperties.giveProperties(new Properties());
    setUpPropertiesFileAndProperty();

    Long collectionSetId = 1L;
    Long transferActionId = 1L;
    Long transferBatchId = 1L;
    Long connectId = 1L;
    RockFactory rockFact = null;

    Class.forName("org.hsqldb.jdbcDriver");

    Connection c;

    c = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");
    stm = c.createStatement();

    stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
        + "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");

    stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");

    rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);

    Meta_versions version = new Meta_versions(rockFact);
    Meta_collections collection = new Meta_collections(rockFact);
    Meta_transfer_actions trActions = new Meta_transfer_actions(rockFact);
    trActions.setAction_contents("profileName=pname\nforbidenTypes=type1,type2");

    cpa = new ChangeProfileAction(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
        rockFact, trActions);

  }

  /**
   * @throws IOException
   */
  private static void setUpPropertiesFileAndProperty() throws IOException {
//	String userHome = env.get("WORKSPACE");
	String userHome = System.getProperty("java.io.tmpdir");
	System.out.println("userHome:"+userHome);

    System.setProperty(Constants.DC_CONFIG_DIR_PROPERTY_NAME, userHome);

    File prop = File.createTempFile("ETLCServer.properties", "", new File(userHome));
    prop.deleteOnExit();

    PrintWriter pw = new PrintWriter(new FileWriter(prop));
    pw.write("name=value");
    pw.close();
  }

  @Test
  public void testExecute() throws Exception {
    TestITransferEngineRMI ttRMI = new TestITransferEngineRMI(false);
    cpa.execute();
    assertEquals("pname", ttRMI.getActiveExecutionProfile());
  }

  @Test
  public void testExecute2() {
    try {
      TestITransferEngineRMI ttRMI = new TestITransferEngineRMI(true);
      cpa.execute();
      fail("testExecute2() failed, should't execute this line");
    } catch (Exception e) {

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

  /*
   * public static junit.framework.Test suite() { return new
   * JUnit4TestAdapter(ChangeProfileActionTest.class); }
   */
}
