package com.distocraft.dc5000.etl.engine.connect;

import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Comparator;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;

public class ConnectionPoolTest implements Comparator<Object> {

  public static String versionNumber = "0";

  public static Long connectId = 0l;

  private static Connection con = null;

  // Metadata connection
  public static RockFactory rockFact;

  // table connection
  public static RockFactory tableRockFact;

  // The transfer action object
  public TransferActionBase trActionBase;

  private static Statement stmt;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");

    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_databases ( USERNAME VARCHAR(31)  ,VERSION_NUMBER VARCHAR(31) ,TYPE_NAME VARCHAR(31) ,CONNECTION_ID BIGINT  ,CONNECTION_NAME VARCHAR(31) ,CONNECTION_STRING VARCHAR(31) ,PASSWORD VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,DRIVER_NAME VARCHAR(31) ,DB_LINK_NAME VARCHAR(31))");

    /* Initializing rockfactory */
    rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */

    stmt.executeUpdate("INSERT INTO Meta_databases VALUES( 'sa'  ,'0'  ,'testTYPE_NAME'  ,0  ,'testCONNECTION_NAME'  ,'jdbc:hsqldb:mem:testdb'  ,''  ,'testDESCRIPTION'  ,'org.hsqldb.jdbcDriver'  ,'testDB_LINK_NAME' )");
    stmt.executeUpdate("INSERT INTO Meta_databases VALUES( 'sa'  ,'0'  ,'testTYPE_NAME'  ,1  ,'testCONNECTION_NAME'  ,'jdbc:hsqldb:mem:testdb'  ,''  ,'testDESCRIPTION'  ,'org.hsqldb.jdbcDriver'  ,'testDB_LINK_NAME' )");

  }

  @Test
  public void getConnectTest() throws Exception {

    final ConnectionPoolTest connPoolTest = new ConnectionPoolTest();

    RockFactory rockFact1 = null;

    RockFactory rockFact3 = null;

    final ConnectionPool conPool = new ConnectionPool(rockFact);

    rockFact1 = conPool.getConnect(null, versionNumber, connectId);

    assertNotNull("Checking RockFact1 Db Connection is established", rockFact1.getConnection());

    setConnectionDetails("0", 1l);

    conPool.getConnect(null, versionNumber, connectId);

    setConnectionDetails("0", 0l);

    rockFact3 = conPool.getConnect(null, versionNumber, connectId);

    final int result = connPoolTest.compare(rockFact3, rockFact1);

    Assert.assertEquals("Checking for Objects are equal/Retrieving the old connection", result, 0);

    Assert.assertEquals("Before cleanPool(),Number of connctions", conPool.count(), 2);

    conPool.cleanPool();

    Assert.assertEquals("Checking cleanPool() method", conPool.count(), 0);

  }

  private static void setConnectionDetails(final String versionNumber2, final Long connectId2) {

    versionNumber = versionNumber2;
    connectId = connectId2;

  }

  @Override
  public int compare(final Object o1, final Object o2) {

    int result = 1;

    final RockFactory rockFact1 = (RockFactory) o1;
    final RockFactory rockFact2 = (RockFactory) o2;

    if (rockFact1.getDbURL().equals(rockFact2.getDbURL()) && (rockFact1.getUserName().equals(rockFact2.getUserName()))
        && (rockFact1.getPassword().equals(rockFact2.getPassword()))
        && (rockFact1.getAutoCommit() == rockFact2.getAutoCommit())) {

      result = 0;
    } else {
      result = 1;
    }

    return result;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_databases");
    con = null;

  }
}
