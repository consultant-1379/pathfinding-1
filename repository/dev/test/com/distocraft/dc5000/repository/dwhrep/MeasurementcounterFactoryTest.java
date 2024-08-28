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
 * Test class for MeasurementcounterFactory. Testing handling of all the objects in
 * Measurementcounter table.
 */
public class MeasurementcounterFactoryTest {

  private static MeasurementcounterFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Measurementcounter whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = MeasurementcounterFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Measurementcounter ( TYPEID VARCHAR(31)  ,DATANAME VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,TIMEAGGREGATION VARCHAR(31) ,GROUPAGGREGATION VARCHAR(31) ,COUNTAGGREGATION VARCHAR(31) ,COLNUMBER BIGINT  ,DATATYPE VARCHAR(31) ,DATASIZE INTEGER  ,DATASCALE INTEGER  ,INCLUDESQL INTEGER  ,UNIVOBJECT VARCHAR(31) ,UNIVCLASS VARCHAR(31) ,COUNTERTYPE VARCHAR(31) ,COUNTERPROCESS VARCHAR(31) ,DATAID VARCHAR(31) ,FOLLOWJOHN INTEGER )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Measurementcounter(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Measurementcounter");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Measurementcounter VALUES( 'testTYPEID3'  ,'testDATANAME3'  ,'testDESCRIPTION3'  ,'testTIMEAGGREGATION3'  ,'testGROUPAGGREGATION3'  ,'testCOUNTAGGREGATION3'  ,3  ,'testDATATYPE3'  ,3  ,3  ,3  ,'testUNIVOBJECT3'  ,'testUNIVCLASS3'  ,'testCOUNTERTYPE3'  ,'testCOUNTERPROCESS3'  ,'testDATAID3'  ,3 )");
	  stmt.executeUpdate("INSERT INTO Measurementcounter VALUES( 'testTYPEID2'  ,'testDATANAME2'  ,'testDESCRIPTION2'  ,'testTIMEAGGREGATION2'  ,'testGROUPAGGREGATION2'  ,'testCOUNTAGGREGATION2'  ,2  ,'testDATATYPE2'  ,2  ,2  ,2  ,'testUNIVOBJECT2'  ,'testUNIVCLASS2'  ,'testCOUNTERTYPE2'  ,'testCOUNTERPROCESS2'  ,'testDATAID2'  ,2 )");
	  stmt.executeUpdate("INSERT INTO Measurementcounter VALUES( 'testTYPEID1'  ,'testDATANAME1'  ,'testDESCRIPTION1'  ,'testTIMEAGGREGATION1'  ,'testGROUPAGGREGATION1'  ,'testCOUNTAGGREGATION1'  ,1  ,'testDATATYPE1'  ,1  ,1  ,1  ,'testUNIVOBJECT1'  ,'testUNIVCLASS1'  ,'testCOUNTERTYPE1'  ,'testCOUNTERPROCESS1'  ,'testDATAID1'  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new MeasurementcounterFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Measurementcounter");
    objUnderTest = null;
  }
  
  /**
   * Testing MeasurementcounterFactory constructor. All rows found from Measurementcounter
   * table are put into vector.
   */
  @Test
  public void testMeasurementcounterFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementcounterFactory(rockFactory, whereObject);

    /* Asserting all Measurementcounters are found and put into vector */
    try {
      Vector<Measurementcounter> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID3, testTYPEID2, testTYPEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Measurementcounters was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementcounterFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementcounterFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing MeasurementcounterFactory constructor. All rows found from Measurementcounter
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeasurementcounterFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementcounterFactory(rockFactory, whereObject, true);

    /* Asserting all Measurementcounters are found and put into vector */
    try {     
      Vector<Measurementcounter> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeasurementcounterFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementcounterFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing MeasurementcounterFactory constructor. All rows found from Measurementcounter
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeasurementcounterFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementcounterFactory(rockFactory, whereObject, "ORDER BY TYPEID");

    /* Asserting all Measurementcounters are found and put into vector */
    try {
      Vector<Measurementcounter> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID1, testTYPEID2, testTYPEID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Measurementcounters was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementcounterFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementcounterFactory(null, whereObject, "ORDER BY TYPEID");
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

    assertEquals( "testTYPEID2" , objUnderTest.getElementAt(1).getTypeid().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Measurementcounter objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Measurementcounter objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Measurementcounter> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID3, testTYPEID2, testTYPEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Measurementcounter objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Measurementcounter testObject = new Measurementcounter(rockFactory, "testTYPEID" + i, "testDATANAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementcounter objects. True is returned if the two vectors
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
   * Test comparing two Measurementcounter objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementcounter objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Measurementcounter testObject = new Measurementcounter(rockFactory, "testTYPEID1", "testDATANAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementcounter objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Measurementcounter testObject = new Measurementcounter(rockFactory, "testTYPEID" + i, "testDATANAME" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementcounter");
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
    assertEquals(MeasurementcounterFactory.class, clonedObject.getClass());
  }
}