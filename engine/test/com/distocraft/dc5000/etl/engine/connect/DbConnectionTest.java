package com.distocraft.dc5000.etl.engine.connect;

import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import java.util.Comparator;

public class DbConnectionTest implements Comparator {

 
  public static String versionNumber = "0";

  public static Long connectId = 0l;

  // Metadata connection
  public static RockFactory rockFact;

  // table connection
  public static RockFactory tableRockFact;

  // The transfer action object
  public TransferActionBase trActionBase;

  private static Statement stmt;
  
  private static Connection con = null;
  
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
  public void connectionTest() throws ClassNotFoundException, SQLException, EngineMetaDataException, RockException {

    final DbConnectionTest connPoolTest = new DbConnectionTest();

    RockFactory rockFact1 = null;

    RockFactory rockFact2 = null;

    final DbConnection dbCon = new DbConnection(null, rockFact, versionNumber, connectId);

    rockFact1 = dbCon.createConnection();

    assertNotNull("Checking RockFact(rockFact1)Db Connection is established", rockFact1.getConnection());

    rockFact2 = dbCon.createConnection();

    setConnectionDetails("0", 1l);

    final int result = connPoolTest.compare(rockFact1, rockFact2);

    Assert.assertEquals("Checking for Connection", result, 0);

  }

  private static void setConnectionDetails(final String versionNumber2, final Long connectId2) {

    versionNumber = versionNumber2;
    connectId = connectId2;

  }

  @Override
  public int compare(final Object o1, final Object o2) {
    // TODO Auto-generated method stub

    final RockFactory rockFact1 = (RockFactory) o1;
    final RockFactory rockFact2 = (RockFactory) o2;

    if (rockFact1.getDbURL().equals(rockFact2.getDbURL()) && (rockFact1.getUserName().equals(rockFact2.getUserName()))
        && (rockFact1.getPassword().equals(rockFact2.getPassword()))
        && (rockFact1.getAutoCommit() == rockFact2.getAutoCommit())) {

      return 0;
    }

    else{
      return 1;
    }
      
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_databases");
  
  }

}
