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
 * Test class for InterfacemeasurementFactory. Testing handling of all the objects in
 * Interfacemeasurement table.
 */
public class InterfacemeasurementFactoryTest {

  private static InterfacemeasurementFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Interfacemeasurement whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = InterfacemeasurementFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Interfacemeasurement ( TAGID VARCHAR(31)  ,DATAFORMATID VARCHAR(31) ,INTERFACENAME VARCHAR(31) ,TRANSFORMERID VARCHAR(31) ,STATUS BIGINT  ,MODIFTIME TIMESTAMP  ,DESCRIPTION VARCHAR(31) ,TECHPACKVERSION VARCHAR(31) ,INTERFACEVERSION VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Interfacemeasurement(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Interfacemeasurement");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Interfacemeasurement VALUES( 'testTAGID3'  ,'testDATAFORMATID3'  ,'testINTERFACENAME3'  ,'testTRANSFORMERID3'  ,3  ,'2003-03-03 00:00:00.0'  ,'testDESCRIPTION3'  ,'testTECHPACKVERSION3'  ,'testINTERFACEVERSION3' )");
	  stmt.executeUpdate("INSERT INTO Interfacemeasurement VALUES( 'testTAGID2'  ,'testDATAFORMATID2'  ,'testINTERFACENAME2'  ,'testTRANSFORMERID2'  ,2  ,'2002-02-02 00:00:00.0'  ,'testDESCRIPTION2'  ,'testTECHPACKVERSION2'  ,'testINTERFACEVERSION2' )");
	  stmt.executeUpdate("INSERT INTO Interfacemeasurement VALUES( 'testTAGID1'  ,'testDATAFORMATID1'  ,'testINTERFACENAME1'  ,'testTRANSFORMERID1'  ,1  ,'2001-01-01 00:00:00.0'  ,'testDESCRIPTION1'  ,'testTECHPACKVERSION1'  ,'testINTERFACEVERSION1' )");

    /* Initializing tested object before each test */
    objUnderTest = new InterfacemeasurementFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Interfacemeasurement");
    objUnderTest = null;
  }
  
  /**
   * Testing InterfacemeasurementFactory constructor. All rows found from Interfacemeasurement
   * table are put into vector.
   */
  @Test
  public void testInterfacemeasurementFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new InterfacemeasurementFactory(rockFactory, whereObject);

    /* Asserting all Interfacemeasurements are found and put into vector */
    try {
      Vector<Interfacemeasurement> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTagid() + ", " +  actualVector.get(1).getTagid() + ", " +  actualVector.get(2).getTagid();
      String expected = "3, testTAGID3, testTAGID2, testTAGID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Interfacemeasurements was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testInterfacemeasurementFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new InterfacemeasurementFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing InterfacemeasurementFactory constructor. All rows found from Interfacemeasurement
   * table are put into vector and data validation is on.
   */
  @Test
  public void testInterfacemeasurementFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new InterfacemeasurementFactory(rockFactory, whereObject, true);

    /* Asserting all Interfacemeasurements are found and put into vector */
    try {     
      Vector<Interfacemeasurement> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testInterfacemeasurementFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new InterfacemeasurementFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing InterfacemeasurementFactory constructor. All rows found from Interfacemeasurement
   * table are put into vector and data validation is on.
   */
  @Test
  public void testInterfacemeasurementFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new InterfacemeasurementFactory(rockFactory, whereObject, "ORDER BY TAGID");

    /* Asserting all Interfacemeasurements are found and put into vector */
    try {
      Vector<Interfacemeasurement> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTagid() + ", " +  actualVector.get(1).getTagid() + ", " +  actualVector.get(2).getTagid();
      String expected = "3, testTAGID1, testTAGID2, testTAGID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Interfacemeasurements was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testInterfacemeasurementFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new InterfacemeasurementFactory(null, whereObject, "ORDER BY TAGID");
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

    assertEquals( "testTAGID2" , objUnderTest.getElementAt(1).getTagid().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Interfacemeasurement objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Interfacemeasurement objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Interfacemeasurement> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getTagid() + ", " +  actualVector.get(1).getTagid() + ", " +  actualVector.get(2).getTagid();
      String expected = "3, testTAGID3, testTAGID2, testTAGID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Interfacemeasurement objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Interfacemeasurement testObject = new Interfacemeasurement(rockFactory, "testTAGID" + i, "testDATAFORMATID" + i, "testINTERFACENAME" + i, "testINTERFACEVERSION" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Interfacemeasurement objects. True is returned if the two vectors
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
   * Test comparing two Interfacemeasurement objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Interfacemeasurement objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Interfacemeasurement testObject = new Interfacemeasurement(rockFactory, "testTAGID1", "testDATAFORMATID1", "testINTERFACENAME1", "testINTERFACEVERSION1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Interfacemeasurement objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Interfacemeasurement testObject = new Interfacemeasurement(rockFactory, "testTAGID" + i, "testDATAFORMATID" + i, "testINTERFACENAME" + i, "testINTERFACEVERSION" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Interfacemeasurement");
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
    assertEquals(InterfacemeasurementFactory.class, clonedObject.getClass());
  }
}