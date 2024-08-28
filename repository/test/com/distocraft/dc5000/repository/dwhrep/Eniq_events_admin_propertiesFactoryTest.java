package com.distocraft.dc5000.repository.dwhrep;

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
 * Test class for Eniq_events_admin_propertiesFactory. Testing handling of all the objects in
 * Eniq_events_admin_properties table.
 */
public class Eniq_events_admin_propertiesFactoryTest {

  private static Eniq_events_admin_propertiesFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Eniq_events_admin_properties whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Eniq_events_admin_propertiesFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Eniq_events_admin_properties ( PARAM_NAME VARCHAR(31)  ,PARAM_VALUE VARCHAR(31) ,DATE_MODIFIED TIMESTAMP  ,MODIFIED_BY VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Eniq_events_admin_properties(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Eniq_events_admin_properties");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Eniq_events_admin_properties VALUES( 'testPARAM_NAME3'  ,'testPARAM_VALUE3'  ,'2003-03-03 00:00:00.0'  ,'testMODIFIED_BY3' )");
	  stmt.executeUpdate("INSERT INTO Eniq_events_admin_properties VALUES( 'testPARAM_NAME2'  ,'testPARAM_VALUE2'  ,'2002-02-02 00:00:00.0'  ,'testMODIFIED_BY2' )");
	  stmt.executeUpdate("INSERT INTO Eniq_events_admin_properties VALUES( 'testPARAM_NAME1'  ,'testPARAM_VALUE1'  ,'2001-01-01 00:00:00.0'  ,'testMODIFIED_BY1' )");

    /* Initializing tested object before each test */
    objUnderTest = new Eniq_events_admin_propertiesFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Eniq_events_admin_properties");
    objUnderTest = null;
  }
  
  /**
   * Testing Eniq_events_admin_propertiesFactory constructor. All rows found from Eniq_events_admin_properties
   * table are put into vector.
   */
  @Test
  public void testEniq_events_admin_propertiesFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Eniq_events_admin_propertiesFactory(rockFactory, whereObject);

    /* Asserting all Eniq_events_admin_propertiess are found and put into vector */
    try {
      Vector<Eniq_events_admin_properties> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getParam_name() + ", " +  actualVector.get(1).getParam_name() + ", " +  actualVector.get(2).getParam_name();
      String expected = "3, testPARAM_NAME3, testPARAM_NAME2, testPARAM_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Eniq_events_admin_propertiess was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testEniq_events_admin_propertiesFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Eniq_events_admin_propertiesFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Eniq_events_admin_propertiesFactory constructor. All rows found from Eniq_events_admin_properties
   * table are put into vector and data validation is on.
   */
  @Test
  public void testEniq_events_admin_propertiesFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Eniq_events_admin_propertiesFactory(rockFactory, whereObject, true);

    /* Asserting all Eniq_events_admin_propertiess are found and put into vector */
    try {     
      Vector<Eniq_events_admin_properties> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testEniq_events_admin_propertiesFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Eniq_events_admin_propertiesFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Eniq_events_admin_propertiesFactory constructor. All rows found from Eniq_events_admin_properties
   * table are put into vector and data validation is on.
   */
  @Test
  public void testEniq_events_admin_propertiesFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Eniq_events_admin_propertiesFactory(rockFactory, whereObject, "ORDER BY PARAM_NAME");

    /* Asserting all Eniq_events_admin_propertiess are found and put into vector */
    try {
      Vector<Eniq_events_admin_properties> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getParam_name() + ", " +  actualVector.get(1).getParam_name() + ", " +  actualVector.get(2).getParam_name();
      String expected = "3, testPARAM_NAME1, testPARAM_NAME2, testPARAM_NAME3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Eniq_events_admin_propertiess was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testEniq_events_admin_propertiesFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Eniq_events_admin_propertiesFactory(null, whereObject, "ORDER BY PARAM_NAME");
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

    assertEquals( "testPARAM_NAME2" , objUnderTest.getElementAt(1).getParam_name().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Eniq_events_admin_properties objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Eniq_events_admin_properties objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Eniq_events_admin_properties> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getParam_name() + ", " +  actualVector.get(1).getParam_name() + ", " +  actualVector.get(2).getParam_name();
      String expected = "3, testPARAM_NAME3, testPARAM_NAME2, testPARAM_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Eniq_events_admin_properties objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Eniq_events_admin_properties testObject = new Eniq_events_admin_properties(rockFactory, "testPARAM_NAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Eniq_events_admin_properties objects. True is returned if the two vectors
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
   * Test comparing two Eniq_events_admin_properties objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Eniq_events_admin_properties objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Eniq_events_admin_properties testObject = new Eniq_events_admin_properties(rockFactory, "testPARAM_NAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Eniq_events_admin_properties objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Eniq_events_admin_properties testObject = new Eniq_events_admin_properties(rockFactory, "testPARAM_NAME" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Eniq_events_admin_properties");
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
    assertEquals(Eniq_events_admin_propertiesFactory.class, clonedObject.getClass());
  }
}