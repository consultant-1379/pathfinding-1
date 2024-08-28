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
 * Test class for BusyhourmappingFactory. Testing handling of all the objects in
 * Busyhourmapping table.
 */
public class BusyhourmappingFactoryTest {

  private static BusyhourmappingFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Busyhourmapping whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = BusyhourmappingFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Busyhourmapping ( VERSIONID VARCHAR(31)  ,BHLEVEL VARCHAR(31) ,BHTYPE VARCHAR(31) ,TARGETVERSIONID VARCHAR(31) ,BHOBJECT VARCHAR(31) ,TYPEID VARCHAR(31) ,BHTARGETTYPE VARCHAR(31) ,BHTARGETLEVEL VARCHAR(31) ,ENABLE INTEGER )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Busyhourmapping(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Busyhourmapping");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Busyhourmapping VALUES( 'testVERSIONID3'  ,'testBHLEVEL3'  ,'testBHTYPE3'  ,'testTARGETVERSIONID3'  ,'testBHOBJECT3'  ,'testTYPEID3'  ,'testBHTARGETTYPE3'  ,'testBHTARGETLEVEL3'  ,3 )");
	  stmt.executeUpdate("INSERT INTO Busyhourmapping VALUES( 'testVERSIONID2'  ,'testBHLEVEL2'  ,'testBHTYPE2'  ,'testTARGETVERSIONID2'  ,'testBHOBJECT2'  ,'testTYPEID2'  ,'testBHTARGETTYPE2'  ,'testBHTARGETLEVEL2'  ,2 )");
	  stmt.executeUpdate("INSERT INTO Busyhourmapping VALUES( 'testVERSIONID1'  ,'testBHLEVEL1'  ,'testBHTYPE1'  ,'testTARGETVERSIONID1'  ,'testBHOBJECT1'  ,'testTYPEID1'  ,'testBHTARGETTYPE1'  ,'testBHTARGETLEVEL1'  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new BusyhourmappingFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Busyhourmapping");
    objUnderTest = null;
  }
  
  /**
   * Testing BusyhourmappingFactory constructor. All rows found from Busyhourmapping
   * table are put into vector.
   */
  @Test
  public void testBusyhourmappingFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new BusyhourmappingFactory(rockFactory, whereObject);

    /* Asserting all Busyhourmappings are found and put into vector */
    try {
      Vector<Busyhourmapping> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Busyhourmappings was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testBusyhourmappingFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new BusyhourmappingFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing BusyhourmappingFactory constructor. All rows found from Busyhourmapping
   * table are put into vector and data validation is on.
   */
  @Test
  public void testBusyhourmappingFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new BusyhourmappingFactory(rockFactory, whereObject, true);

    /* Asserting all Busyhourmappings are found and put into vector */
    try {     
      Vector<Busyhourmapping> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testBusyhourmappingFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new BusyhourmappingFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing BusyhourmappingFactory constructor. All rows found from Busyhourmapping
   * table are put into vector and data validation is on.
   */
  @Test
  public void testBusyhourmappingFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new BusyhourmappingFactory(rockFactory, whereObject, "ORDER BY VERSIONID");

    /* Asserting all Busyhourmappings are found and put into vector */
    try {
      Vector<Busyhourmapping> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID1, testVERSIONID2, testVERSIONID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Busyhourmappings was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testBusyhourmappingFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new BusyhourmappingFactory(null, whereObject, "ORDER BY VERSIONID");
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
   * Testing size retrieving of the vector containing Busyhourmapping objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Busyhourmapping objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Busyhourmapping> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Busyhourmapping objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Busyhourmapping testObject = new Busyhourmapping(rockFactory, "testVERSIONID" + i, "testBHLEVEL" + i, "testBHTYPE" + i, "testTARGETVERSIONID" + i, "testBHOBJECT" + i, "testTYPEID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Busyhourmapping objects. True is returned if the two vectors
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
   * Test comparing two Busyhourmapping objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Busyhourmapping objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Busyhourmapping testObject = new Busyhourmapping(rockFactory, "testVERSIONID1", "testBHLEVEL1", "testBHTYPE1", "testTARGETVERSIONID1", "testBHOBJECT1", "testTYPEID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Busyhourmapping objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Busyhourmapping testObject = new Busyhourmapping(rockFactory, "testVERSIONID" + i, "testBHLEVEL" + i, "testBHTYPE" + i, "testTARGETVERSIONID" + i, "testBHOBJECT" + i, "testTYPEID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Busyhourmapping");
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
    assertEquals(BusyhourmappingFactory.class, clonedObject.getClass());
  }
}