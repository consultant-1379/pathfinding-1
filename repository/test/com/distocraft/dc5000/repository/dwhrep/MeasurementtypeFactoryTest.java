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
 * Test class for MeasurementtypeFactory. Testing handling of all the objects in
 * Measurementtype table.
 */
public class MeasurementtypeFactoryTest {

  private static MeasurementtypeFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Measurementtype whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = MeasurementtypeFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Measurementtype ( TYPEID VARCHAR(31)  ,TYPECLASSID VARCHAR(31) ,TYPENAME VARCHAR(31) ,VENDORID VARCHAR(31) ,FOLDERNAME VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,STATUS BIGINT  ,VERSIONID VARCHAR(31) ,OBJECTID VARCHAR(31) ,OBJECTNAME VARCHAR(31) ,OBJECTVERSION INTEGER  ,OBJECTTYPE VARCHAR(31) ,JOINABLE VARCHAR(31) ,SIZING VARCHAR(31) ,TOTALAGG INTEGER  ,ELEMENTBHSUPPORT INTEGER  ,RANKINGTABLE INTEGER  ,DELTACALCSUPPORT INTEGER  ,PLAINTABLE INTEGER  ,UNIVERSEEXTENSION VARCHAR(31) ,VECTORSUPPORT INTEGER  ,DATAFORMATSUPPORT INTEGER  ,FOLLOWJOHN INTEGER  ,ONEMINAGG INTEGER  ,FIFTEENMINAGG INTEGER  ,EVENTSCALCTABLE INTEGER  ,MIXEDPARTITIONSTABLE INTEGER  ,LOADFILE_DUP_CHECK INTEGER ,SONAGG INTEGER, SONFIFTEENMINAGG INTEGER ,ROPGRPCELL varchar(255))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Measurementtype(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Measurementtype");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Measurementtype VALUES( 'testTYPEID3'  ,'testTYPECLASSID3'  ,'testTYPENAME3'  ,'testVENDORID3'  ,'testFOLDERNAME3'  ,'testDESCRIPTION3'  ,3  ,'testVERSIONID3'  ,'testOBJECTID3'  ,'testOBJECTNAME3'  ,3  ,'testOBJECTTYPE3'  ,'testJOINABLE3'  ,'testSIZING3'  ,3  ,3  ,3  ,3  ,3  ,'testUNIVERSEEXTENSION3'  ,3  ,3  ,3  ,3  ,3  ,3  ,3  ,3 ,0 ,0 ,'test' )");
	  stmt.executeUpdate("INSERT INTO Measurementtype VALUES( 'testTYPEID2'  ,'testTYPECLASSID2'  ,'testTYPENAME2'  ,'testVENDORID2'  ,'testFOLDERNAME2'  ,'testDESCRIPTION2'  ,2  ,'testVERSIONID2'  ,'testOBJECTID2'  ,'testOBJECTNAME2'  ,2  ,'testOBJECTTYPE2'  ,'testJOINABLE2'  ,'testSIZING2'  ,2  ,2  ,2  ,2  ,2  ,'testUNIVERSEEXTENSION2'  ,2  ,2  ,2  ,2  ,2  ,2  ,2  ,2 ,0 ,0 ,'test' )");
	  stmt.executeUpdate("INSERT INTO Measurementtype VALUES( 'testTYPEID1'  ,'testTYPECLASSID1'  ,'testTYPENAME1'  ,'testVENDORID1'  ,'testFOLDERNAME1'  ,'testDESCRIPTION1'  ,1  ,'testVERSIONID1'  ,'testOBJECTID1'  ,'testOBJECTNAME1'  ,1  ,'testOBJECTTYPE1'  ,'testJOINABLE1'  ,'testSIZING1'  ,1  ,1  ,1  ,1  ,1  ,'testUNIVERSEEXTENSION1'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1 ,0 ,0 ,'test' )");

    /* Initializing tested object before each test */
    objUnderTest = new MeasurementtypeFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Measurementtype");
    objUnderTest = null;
  }
  
  /**
   * Testing MeasurementtypeFactory constructor. All rows found from Measurementtype
   * table are put into vector.
   */
  @Test
  public void testMeasurementtypeFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementtypeFactory(rockFactory, whereObject);

    /* Asserting all Measurementtypes are found and put into vector */
    try {
      Vector<Measurementtype> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID3, testTYPEID2, testTYPEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Measurementtypes was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementtypeFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementtypeFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing MeasurementtypeFactory constructor. All rows found from Measurementtype
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeasurementtypeFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementtypeFactory(rockFactory, whereObject, true);

    /* Asserting all Measurementtypes are found and put into vector */
    try {     
      Vector<Measurementtype> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeasurementtypeFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementtypeFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing MeasurementtypeFactory constructor. All rows found from Measurementtype
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeasurementtypeFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementtypeFactory(rockFactory, whereObject, "ORDER BY TYPEID");

    /* Asserting all Measurementtypes are found and put into vector */
    try {
      Vector<Measurementtype> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID1, testTYPEID2, testTYPEID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Measurementtypes was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementtypeFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementtypeFactory(null, whereObject, "ORDER BY TYPEID");
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
   * Testing size retrieving of the vector containing Measurementtype objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Measurementtype objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Measurementtype> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID3, testTYPEID2, testTYPEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Measurementtype objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Measurementtype testObject = new Measurementtype(rockFactory, "testTYPEID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementtype objects. True is returned if the two vectors
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
   * Test comparing two Measurementtype objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementtype objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Measurementtype testObject = new Measurementtype(rockFactory, "testTYPEID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementtype objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Measurementtype testObject = new Measurementtype(rockFactory, "testTYPEID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementtype");
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
    assertEquals(MeasurementtypeFactory.class, clonedObject.getClass());
  }
}