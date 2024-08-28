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
 * Test class for AggregationFactory. Testing handling of all the objects in
 * Aggregation table.
 */
public class AggregationFactoryTest {

  private static AggregationFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Aggregation whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = AggregationFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Aggregation ( AGGREGATION VARCHAR(31)  ,VERSIONID VARCHAR(31) ,AGGREGATIONSET VARCHAR(31) ,AGGREGATIONGROUP VARCHAR(31) ,REAGGREGATIONSET VARCHAR(31) ,REAGGREGATIONGROUP VARCHAR(31) ,GROUPORDER INTEGER  ,AGGREGATIONORDER INTEGER  ,AGGREGATIONTYPE VARCHAR(31) ,AGGREGATIONSCOPE VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Aggregation(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Aggregation");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Aggregation VALUES( 'testAGGREGATION3'  ,'testVERSIONID3'  ,'testAGGREGATIONSET3'  ,'testAGGREGATIONGROUP3'  ,'testREAGGREGATIONSET3'  ,'testREAGGREGATIONGROUP3'  ,3  ,3  ,'testAGGREGATIONTYPE3'  ,'testAGGREGATIONSCOPE3' )");
	  stmt.executeUpdate("INSERT INTO Aggregation VALUES( 'testAGGREGATION2'  ,'testVERSIONID2'  ,'testAGGREGATIONSET2'  ,'testAGGREGATIONGROUP2'  ,'testREAGGREGATIONSET2'  ,'testREAGGREGATIONGROUP2'  ,2  ,2  ,'testAGGREGATIONTYPE2'  ,'testAGGREGATIONSCOPE2' )");
	  stmt.executeUpdate("INSERT INTO Aggregation VALUES( 'testAGGREGATION1'  ,'testVERSIONID1'  ,'testAGGREGATIONSET1'  ,'testAGGREGATIONGROUP1'  ,'testREAGGREGATIONSET1'  ,'testREAGGREGATIONGROUP1'  ,1  ,1  ,'testAGGREGATIONTYPE1'  ,'testAGGREGATIONSCOPE1' )");

    /* Initializing tested object before each test */
    objUnderTest = new AggregationFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Aggregation");
    objUnderTest = null;
  }
  
  /**
   * Testing AggregationFactory constructor. All rows found from Aggregation
   * table are put into vector.
   */
  @Test
  public void testAggregationFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AggregationFactory(rockFactory, whereObject);

    /* Asserting all Aggregations are found and put into vector */
    try {
      Vector<Aggregation> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getAggregation() + ", " +  actualVector.get(1).getAggregation() + ", " +  actualVector.get(2).getAggregation();
      String expected = "3, testAGGREGATION3, testAGGREGATION2, testAGGREGATION1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAggregationFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AggregationFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing AggregationFactory constructor. All rows found from Aggregation
   * table are put into vector and data validation is on.
   */
  @Test
  public void testAggregationFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AggregationFactory(rockFactory, whereObject, true);

    /* Asserting all Aggregations are found and put into vector */
    try {     
      Vector<Aggregation> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testAggregationFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AggregationFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing AggregationFactory constructor. All rows found from Aggregation
   * table are put into vector and data validation is on.
   */
  @Test
  public void testAggregationFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AggregationFactory(rockFactory, whereObject, "ORDER BY AGGREGATION");

    /* Asserting all Aggregations are found and put into vector */
    try {
      Vector<Aggregation> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getAggregation() + ", " +  actualVector.get(1).getAggregation() + ", " +  actualVector.get(2).getAggregation();
      String expected = "3, testAGGREGATION1, testAGGREGATION2, testAGGREGATION3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAggregationFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AggregationFactory(null, whereObject, "ORDER BY AGGREGATION");
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
   * Testing size retrieving of the vector containing Aggregation objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Aggregation objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Aggregation> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getAggregation() + ", " +  actualVector.get(1).getAggregation() + ", " +  actualVector.get(2).getAggregation();
      String expected = "3, testAGGREGATION3, testAGGREGATION2, testAGGREGATION1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Aggregation objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Aggregation testObject = new Aggregation(rockFactory, "testAGGREGATION" + i, "testVERSIONID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Aggregation objects. True is returned if the two vectors
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
   * Test comparing two Aggregation objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Aggregation objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Aggregation testObject = new Aggregation(rockFactory, "testAGGREGATION1", "testVERSIONID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Aggregation objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Aggregation testObject = new Aggregation(rockFactory, "testAGGREGATION" + i, "testVERSIONID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Aggregation");
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
    assertEquals(AggregationFactory.class, clonedObject.getClass());
  }
}