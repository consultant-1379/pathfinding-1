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
 * Test class for AggregationruleFactory. Testing handling of all the objects in
 * Aggregationrule table.
 */
public class AggregationruleFactoryTest {

  private static AggregationruleFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Aggregationrule whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = AggregationruleFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Aggregationrule ( AGGREGATION VARCHAR(31)  ,VERSIONID VARCHAR(31) ,RULEID INTEGER  ,TARGET_TYPE VARCHAR(31) ,TARGET_LEVEL VARCHAR(31) ,TARGET_TABLE VARCHAR(31) ,TARGET_MTABLEID VARCHAR(31) ,SOURCE_TYPE VARCHAR(31) ,SOURCE_LEVEL VARCHAR(31) ,SOURCE_TABLE VARCHAR(31) ,SOURCE_MTABLEID VARCHAR(31) ,RULETYPE VARCHAR(31) ,AGGREGATIONSCOPE VARCHAR(31) ,BHTYPE VARCHAR(31) ,ENABLE INTEGER )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Aggregationrule(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Aggregationrule");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Aggregationrule VALUES( 'testAGGREGATION3'  ,'testVERSIONID3'  ,3  ,'testTARGET_TYPE3'  ,'testTARGET_LEVEL3'  ,'testTARGET_TABLE3'  ,'testTARGET_MTABLEID3'  ,'testSOURCE_TYPE3'  ,'testSOURCE_LEVEL3'  ,'testSOURCE_TABLE3'  ,'testSOURCE_MTABLEID3'  ,'testRULETYPE3'  ,'testAGGREGATIONSCOPE3'  ,'testBHTYPE3'  ,3 )");
	  stmt.executeUpdate("INSERT INTO Aggregationrule VALUES( 'testAGGREGATION2'  ,'testVERSIONID2'  ,2  ,'testTARGET_TYPE2'  ,'testTARGET_LEVEL2'  ,'testTARGET_TABLE2'  ,'testTARGET_MTABLEID2'  ,'testSOURCE_TYPE2'  ,'testSOURCE_LEVEL2'  ,'testSOURCE_TABLE2'  ,'testSOURCE_MTABLEID2'  ,'testRULETYPE2'  ,'testAGGREGATIONSCOPE2'  ,'testBHTYPE2'  ,2 )");
	  stmt.executeUpdate("INSERT INTO Aggregationrule VALUES( 'testAGGREGATION1'  ,'testVERSIONID1'  ,1  ,'testTARGET_TYPE1'  ,'testTARGET_LEVEL1'  ,'testTARGET_TABLE1'  ,'testTARGET_MTABLEID1'  ,'testSOURCE_TYPE1'  ,'testSOURCE_LEVEL1'  ,'testSOURCE_TABLE1'  ,'testSOURCE_MTABLEID1'  ,'testRULETYPE1'  ,'testAGGREGATIONSCOPE1'  ,'testBHTYPE1'  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new AggregationruleFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Aggregationrule");
    objUnderTest = null;
  }
  
  /**
   * Testing AggregationruleFactory constructor. All rows found from Aggregationrule
   * table are put into vector.
   */
  @Test
  public void testAggregationruleFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AggregationruleFactory(rockFactory, whereObject);

    /* Asserting all Aggregationrules are found and put into vector */
    try {
      Vector<Aggregationrule> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getAggregation() + ", " +  actualVector.get(1).getAggregation() + ", " +  actualVector.get(2).getAggregation();
      String expected = "3, testAGGREGATION3, testAGGREGATION2, testAGGREGATION1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Aggregationrules was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAggregationruleFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AggregationruleFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing AggregationruleFactory constructor. All rows found from Aggregationrule
   * table are put into vector and data validation is on.
   */
  @Test
  public void testAggregationruleFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AggregationruleFactory(rockFactory, whereObject, true);

    /* Asserting all Aggregationrules are found and put into vector */
    try {     
      Vector<Aggregationrule> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testAggregationruleFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AggregationruleFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing AggregationruleFactory constructor. All rows found from Aggregationrule
   * table are put into vector and data validation is on.
   */
  @Test
  public void testAggregationruleFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AggregationruleFactory(rockFactory, whereObject, "ORDER BY AGGREGATION");

    /* Asserting all Aggregationrules are found and put into vector */
    try {
      Vector<Aggregationrule> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getAggregation() + ", " +  actualVector.get(1).getAggregation() + ", " +  actualVector.get(2).getAggregation();
      String expected = "3, testAGGREGATION1, testAGGREGATION2, testAGGREGATION3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Aggregationrules was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAggregationruleFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AggregationruleFactory(null, whereObject, "ORDER BY AGGREGATION");
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

    assertEquals( "testAGGREGATION2" , objUnderTest.getElementAt(1).getAggregation().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Aggregationrule objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Aggregationrule objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Aggregationrule> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getAggregation() + ", " +  actualVector.get(1).getAggregation() + ", " +  actualVector.get(2).getAggregation();
      String expected = "3, testAGGREGATION3, testAGGREGATION2, testAGGREGATION1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Aggregationrule objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Aggregationrule testObject = new Aggregationrule(rockFactory, "testAGGREGATION" + i, "testVERSIONID" + i, i, "testRULETYPE" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Aggregationrule objects. True is returned if the two vectors
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
   * Test comparing two Aggregationrule objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Aggregationrule objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Aggregationrule testObject = new Aggregationrule(rockFactory, "testAGGREGATION1", "testVERSIONID1", 1, "testRULETYPE1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Aggregationrule objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Aggregationrule testObject = new Aggregationrule(rockFactory, "testAGGREGATION" + i, "testVERSIONID" + i, i, "testRULETYPE" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Aggregationrule");
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
    assertEquals(AggregationruleFactory.class, clonedObject.getClass());
  }
}