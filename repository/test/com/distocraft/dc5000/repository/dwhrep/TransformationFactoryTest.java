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
 * Test class for TransformationFactory. Testing handling of all the objects in
 * Transformation table.
 */
public class TransformationFactoryTest {

  private static TransformationFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Transformation whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = TransformationFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Transformation ( TRANSFORMERID VARCHAR(31)  ,ORDERNO BIGINT  ,TYPE VARCHAR(31) ,SOURCE VARCHAR(31) ,TARGET VARCHAR(31) ,CONFIG VARCHAR(31) ,DESCRIPTION VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Transformation(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Transformation");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Transformation VALUES( 'testTRANSFORMERID3'  ,3  ,'testTYPE3'  ,'testSOURCE3'  ,'testTARGET3'  ,'testCONFIG3'  ,'testDESCRIPTION3' )");
	  stmt.executeUpdate("INSERT INTO Transformation VALUES( 'testTRANSFORMERID2'  ,2  ,'testTYPE2'  ,'testSOURCE2'  ,'testTARGET2'  ,'testCONFIG2'  ,'testDESCRIPTION2' )");
	  stmt.executeUpdate("INSERT INTO Transformation VALUES( 'testTRANSFORMERID1'  ,1  ,'testTYPE1'  ,'testSOURCE1'  ,'testTARGET1'  ,'testCONFIG1'  ,'testDESCRIPTION1' )");

    /* Initializing tested object before each test */
    objUnderTest = new TransformationFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Transformation");
    objUnderTest = null;
  }
  
  /**
   * Testing TransformationFactory constructor. All rows found from Transformation
   * table are put into vector.
   */
  @Test
  public void testTransformationFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new TransformationFactory(rockFactory, whereObject);

    /* Asserting all Transformations are found and put into vector */
    try {
      Vector<Transformation> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTransformerid() + ", " +  actualVector.get(1).getTransformerid() + ", " +  actualVector.get(2).getTransformerid();
      String expected = "3, testTRANSFORMERID3, testTRANSFORMERID2, testTRANSFORMERID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Transformations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testTransformationFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new TransformationFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing TransformationFactory constructor. All rows found from Transformation
   * table are put into vector and data validation is on.
   */
  @Test
  public void testTransformationFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new TransformationFactory(rockFactory, whereObject, true);

    /* Asserting all Transformations are found and put into vector */
    try {     
      Vector<Transformation> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testTransformationFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new TransformationFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing TransformationFactory constructor. All rows found from Transformation
   * table are put into vector and data validation is on.
   */
  @Test
  public void testTransformationFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new TransformationFactory(rockFactory, whereObject, "ORDER BY TRANSFORMERID");

    /* Asserting all Transformations are found and put into vector */
    try {
      Vector<Transformation> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTransformerid() + ", " +  actualVector.get(1).getTransformerid() + ", " +  actualVector.get(2).getTransformerid();
      String expected = "3, testTRANSFORMERID1, testTRANSFORMERID2, testTRANSFORMERID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Transformations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testTransformationFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new TransformationFactory(null, whereObject, "ORDER BY TRANSFORMERID");
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

    assertEquals( "testTRANSFORMERID2" , objUnderTest.getElementAt(1).getTransformerid().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Transformation objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Transformation objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Transformation> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getTransformerid() + ", " +  actualVector.get(1).getTransformerid() + ", " +  actualVector.get(2).getTransformerid();
      String expected = "3, testTRANSFORMERID3, testTRANSFORMERID2, testTRANSFORMERID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Transformation objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Transformation testObject = new Transformation(rockFactory, "testTRANSFORMERID" + i, Long.parseLong(i + ""));
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Transformation objects. True is returned if the two vectors
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
   * Test comparing two Transformation objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Transformation objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Transformation testObject = new Transformation(rockFactory, "testTRANSFORMERID1", 1L);
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Transformation objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Transformation testObject = new Transformation(rockFactory, "testTRANSFORMERID" + i, Long.parseLong(i + ""));
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Transformation");
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
    assertEquals(TransformationFactory.class, clonedObject.getClass());
  }
}