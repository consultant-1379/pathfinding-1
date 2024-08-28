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
 * Test class for DataformatFactory. Testing handling of all the objects in
 * Dataformat table.
 */
public class DataformatFactoryTest {

  private static DataformatFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Dataformat whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = DataformatFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Dataformat ( DATAFORMATID VARCHAR(31)  ,TYPEID VARCHAR(31) ,VERSIONID VARCHAR(31) ,OBJECTTYPE VARCHAR(31) ,FOLDERNAME VARCHAR(31) ,DATAFORMATTYPE VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Dataformat(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Dataformat");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Dataformat VALUES( 'testDATAFORMATID3'  ,'testTYPEID3'  ,'testVERSIONID3'  ,'testOBJECTTYPE3'  ,'testFOLDERNAME3'  ,'testDATAFORMATTYPE3' )");
	  stmt.executeUpdate("INSERT INTO Dataformat VALUES( 'testDATAFORMATID2'  ,'testTYPEID2'  ,'testVERSIONID2'  ,'testOBJECTTYPE2'  ,'testFOLDERNAME2'  ,'testDATAFORMATTYPE2' )");
	  stmt.executeUpdate("INSERT INTO Dataformat VALUES( 'testDATAFORMATID1'  ,'testTYPEID1'  ,'testVERSIONID1'  ,'testOBJECTTYPE1'  ,'testFOLDERNAME1'  ,'testDATAFORMATTYPE1' )");

    /* Initializing tested object before each test */
    objUnderTest = new DataformatFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Dataformat");
    objUnderTest = null;
  }
  
  /**
   * Testing DataformatFactory constructor. All rows found from Dataformat
   * table are put into vector.
   */
  @Test
  public void testDataformatFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DataformatFactory(rockFactory, whereObject);

    /* Asserting all Dataformats are found and put into vector */
    try {
      Vector<Dataformat> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getDataformatid() + ", " +  actualVector.get(1).getDataformatid() + ", " +  actualVector.get(2).getDataformatid();
      String expected = "3, testDATAFORMATID3, testDATAFORMATID2, testDATAFORMATID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Dataformats was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDataformatFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DataformatFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing DataformatFactory constructor. All rows found from Dataformat
   * table are put into vector and data validation is on.
   */
  @Test
  public void testDataformatFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DataformatFactory(rockFactory, whereObject, true);

    /* Asserting all Dataformats are found and put into vector */
    try {     
      Vector<Dataformat> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testDataformatFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DataformatFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing DataformatFactory constructor. All rows found from Dataformat
   * table are put into vector and data validation is on.
   */
  @Test
  public void testDataformatFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DataformatFactory(rockFactory, whereObject, "ORDER BY DATAFORMATID");

    /* Asserting all Dataformats are found and put into vector */
    try {
      Vector<Dataformat> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getDataformatid() + ", " +  actualVector.get(1).getDataformatid() + ", " +  actualVector.get(2).getDataformatid();
      String expected = "3, testDATAFORMATID1, testDATAFORMATID2, testDATAFORMATID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Dataformats was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDataformatFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DataformatFactory(null, whereObject, "ORDER BY DATAFORMATID");
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

    assertEquals( "testDATAFORMATID2" , objUnderTest.getElementAt(1).getDataformatid().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Dataformat objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Dataformat objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Dataformat> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getDataformatid() + ", " +  actualVector.get(1).getDataformatid() + ", " +  actualVector.get(2).getDataformatid();
      String expected = "3, testDATAFORMATID3, testDATAFORMATID2, testDATAFORMATID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Dataformat objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Dataformat testObject = new Dataformat(rockFactory, "testDATAFORMATID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Dataformat objects. True is returned if the two vectors
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
   * Test comparing two Dataformat objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Dataformat objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Dataformat testObject = new Dataformat(rockFactory, "testDATAFORMATID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Dataformat objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Dataformat testObject = new Dataformat(rockFactory, "testDATAFORMATID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dataformat");
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
    assertEquals(DataformatFactory.class, clonedObject.getClass());
  }
}