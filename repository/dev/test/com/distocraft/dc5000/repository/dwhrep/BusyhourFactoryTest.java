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
 * Test class for BusyhourFactory. Testing handling of all the objects in
 * Busyhour table.
 */
public class BusyhourFactoryTest {

  private static BusyhourFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Busyhour whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = BusyhourFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Busyhour ( VERSIONID VARCHAR(31)  ,BHLEVEL VARCHAR(31) ,BHTYPE VARCHAR(31) ,BHCRITERIA VARCHAR(31) ,WHERECLAUSE VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,TARGETVERSIONID VARCHAR(31) ,BHOBJECT VARCHAR(31) ,BHELEMENT INTEGER  ,ENABLE INTEGER  ,AGGREGATIONTYPE VARCHAR(31) ,OFFSET INTEGER  ,WINDOWSIZE INTEGER  ,LOOKBACK INTEGER  ,P_THRESHOLD INTEGER  ,N_THRESHOLD INTEGER  ,CLAUSE VARCHAR(31) ,PLACEHOLDERTYPE VARCHAR(31) ,GROUPING VARCHAR(31) ,REACTIVATEVIEWS INTEGER )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Busyhour(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Busyhour");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Busyhour VALUES( 'testVERSIONID3'  ,'testBHLEVEL3'  ,'testBHTYPE3'  ,'testBHCRITERIA3'  ,'testWHERECLAUSE3'  ,'testDESCRIPTION3'  ,'testTARGETVERSIONID3'  ,'testBHOBJECT3'  ,3  ,3  ,'testAGGREGATIONTYPE3'  ,3  ,3  ,3  ,3  ,3  ,'testCLAUSE3'  ,'testPLACEHOLDERTYPE3'  ,'testGROUPING3'  ,3 )");
	  stmt.executeUpdate("INSERT INTO Busyhour VALUES( 'testVERSIONID2'  ,'testBHLEVEL2'  ,'testBHTYPE2'  ,'testBHCRITERIA2'  ,'testWHERECLAUSE2'  ,'testDESCRIPTION2'  ,'testTARGETVERSIONID2'  ,'testBHOBJECT2'  ,2  ,2  ,'testAGGREGATIONTYPE2'  ,2  ,2  ,2  ,2  ,2  ,'testCLAUSE2'  ,'testPLACEHOLDERTYPE2'  ,'testGROUPING2'  ,2 )");
	  stmt.executeUpdate("INSERT INTO Busyhour VALUES( 'testVERSIONID1'  ,'testBHLEVEL1'  ,'testBHTYPE1'  ,'testBHCRITERIA1'  ,'testWHERECLAUSE1'  ,'testDESCRIPTION1'  ,'testTARGETVERSIONID1'  ,'testBHOBJECT1'  ,1  ,1  ,'testAGGREGATIONTYPE1'  ,1  ,1  ,1  ,1  ,1  ,'testCLAUSE1'  ,'testPLACEHOLDERTYPE1'  ,'testGROUPING1'  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new BusyhourFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Busyhour");
    objUnderTest = null;
  }
  
  /**
   * Testing BusyhourFactory constructor. All rows found from Busyhour
   * table are put into vector.
   */
  @Test
  public void testBusyhourFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new BusyhourFactory(rockFactory, whereObject);

    /* Asserting all Busyhours are found and put into vector */
    try {
      Vector<Busyhour> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Busyhours was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testBusyhourFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new BusyhourFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing BusyhourFactory constructor. All rows found from Busyhour
   * table are put into vector and data validation is on.
   */
  @Test
  public void testBusyhourFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new BusyhourFactory(rockFactory, whereObject, true);

    /* Asserting all Busyhours are found and put into vector */
    try {     
      Vector<Busyhour> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testBusyhourFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new BusyhourFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing BusyhourFactory constructor. All rows found from Busyhour
   * table are put into vector and data validation is on.
   */
  @Test
  public void testBusyhourFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new BusyhourFactory(rockFactory, whereObject, "ORDER BY VERSIONID");

    /* Asserting all Busyhours are found and put into vector */
    try {
      Vector<Busyhour> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID1, testVERSIONID2, testVERSIONID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Busyhours was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testBusyhourFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new BusyhourFactory(null, whereObject, "ORDER BY VERSIONID");
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

    assertEquals( "testVERSIONID2" , objUnderTest.getElementAt(1).getVersionid().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Busyhour objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Busyhour objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Busyhour> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Busyhour objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Busyhour testObject = new Busyhour(rockFactory, "testVERSIONID" + i, "testBHLEVEL" + i, "testBHTYPE" + i, "testTARGETVERSIONID" + i, "testBHOBJECT" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Busyhour objects. True is returned if the two vectors
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
   * Test comparing two Busyhour objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Busyhour objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Busyhour testObject = new Busyhour(rockFactory, "testVERSIONID1", "testBHLEVEL1", "testBHTYPE1", "testTARGETVERSIONID1", "testBHOBJECT1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Busyhour objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Busyhour testObject = new Busyhour(rockFactory, "testVERSIONID" + i, "testBHLEVEL" + i, "testBHTYPE" + i, "testTARGETVERSIONID" + i, "testBHOBJECT" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Busyhour");
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
    assertEquals(BusyhourFactory.class, clonedObject.getClass());
  }
}