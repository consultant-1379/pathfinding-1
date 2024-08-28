package com.distocraft.dc5000.etl.rock;

import static org.junit.Assert.*;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Vector;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;

/**
 * Test class for Meta_system_monitorsFactory. Testing handling of all the objects in
 * Meta_system_monitors table.
 */
public class Meta_system_monitorsFactoryTest {

  private static Meta_system_monitorsFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_system_monitors whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_system_monitorsFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_system_monitors ( MONITOR VARCHAR(31)  ,HOSTNAME VARCHAR(31) ,TYPE VARCHAR(31) ,CONFIGURATION VARCHAR(31) ,EXECUTED TIMESTAMP  ,STATUS VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_system_monitors(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_system_monitors");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_system_monitors VALUES( 'testMONITOR3'  ,'testHOSTNAME3'  ,'testTYPE3'  ,'testCONFIGURATION3'  ,'2003-03-03 00:00:00.0'  ,'testSTATUS3' )");
	  stmt.executeUpdate("INSERT INTO Meta_system_monitors VALUES( 'testMONITOR2'  ,'testHOSTNAME2'  ,'testTYPE2'  ,'testCONFIGURATION2'  ,'2002-02-02 00:00:00.0'  ,'testSTATUS2' )");
	  stmt.executeUpdate("INSERT INTO Meta_system_monitors VALUES( 'testMONITOR1'  ,'testHOSTNAME1'  ,'testTYPE1'  ,'testCONFIGURATION1'  ,'2001-01-01 00:00:00.0'  ,'testSTATUS1' )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_system_monitorsFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_system_monitors");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_system_monitorsFactory constructor. All rows found from Meta_system_monitors
   * table are put into vector.
   */
  @Test
  public void testMeta_system_monitorsFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_system_monitorsFactory(rockFactory, whereObject);

    /* Asserting all Meta_system_monitorss are found and put into vector */
    try {
      Vector<Meta_system_monitors> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getMonitor() + ", " +  actualVector.get(1).getMonitor() + ", " +  actualVector.get(2).getMonitor();
      String expected = "3, testMONITOR3, testMONITOR2, testMONITOR1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_system_monitorss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_system_monitorsFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_system_monitorsFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_system_monitorsFactory constructor. All rows found from Meta_system_monitors
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_system_monitorsFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_system_monitorsFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_system_monitorss are found and put into vector */
    try {     
      Vector<Meta_system_monitors> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).isValidateData() + ", " +  actualVector.get(1).isValidateData() + ", " +  actualVector.get(2).isValidateData();
      String expected = 3 + ", " + true + ", " + true + ", " + true;
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_system_monitorsFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_system_monitorsFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_system_monitorsFactory constructor. All rows found from Meta_system_monitors
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_system_monitorsFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_system_monitorsFactory(rockFactory, whereObject, "ORDER BY MONITOR");

    /* Asserting all Meta_system_monitorss are found and put into vector */
    try {
      Vector<Meta_system_monitors> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getMonitor() + ", " +  actualVector.get(1).getMonitor() + ", " +  actualVector.get(2).getMonitor();
      String expected = "3, testMONITOR1, testMONITOR2, testMONITOR3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_system_monitorss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_system_monitorsFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_system_monitorsFactory(null, whereObject, "ORDER BY MONITOR");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtWithGenericInput() throws Exception {

    assertEquals( "testMONITOR2" , objUnderTest.getElementAt(1).getMonitor().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Meta_system_monitors objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_system_monitors objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_system_monitors> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getMonitor() + ", " +  actualVector.get(1).getMonitor() + ", " +  actualVector.get(2).getMonitor();
      String expected = "3, testMONITOR3, testMONITOR2, testMONITOR1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_system_monitors objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_system_monitors testObject = new Meta_system_monitors(rockFactory, "testMONITOR" + i, "testHOSTNAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_system_monitors objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameVector() throws Exception {

    /* Creating another vector with the same vector */
    Vector otherVector = (Vector) vec.get(objUnderTest);

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_system_monitors objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_system_monitors objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_system_monitors testObject = new Meta_system_monitors(rockFactory, "testMONITOR1", "testHOSTNAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_system_monitors objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_system_monitors testObject = new Meta_system_monitors(rockFactory, "testMONITOR" + i, "testHOSTNAME" + i);
      otherVector.add(testObject);
    }
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test deleting objects from the database.
   */
  @Test
  public void testDeleteDB() throws Exception {
    
    /* Calling the tested object */
    String actual = objUnderTest.deleteDB() + ", ";
    
    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_system_monitors");
    while (res.next()) {
      rows = res.getInt(1);
    }
    
    /* Asserting object is deleted from the database */
    actual += rows;
    assertEquals(3 + ", " + 0, actual);
  }
  
  /**
   * Test object cloning.
   */
  @Test
  public void testClone() throws Exception {
    
    /* Asserting if cloning works */
    Object clonedObject = objUnderTest.clone();
    assertEquals(Meta_system_monitorsFactory.class, clonedObject.getClass());
  }
}