package com.distocraft.dc5000.etl.rock;

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
 * Test class for Meta_connection_typesFactory. Testing handling of all the objects in
 * Meta_connection_types table.
 */
public class Meta_connection_typesFactoryTest {

  private static Meta_connection_typesFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_connection_types whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_connection_typesFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_connection_types ( TYPE_NAME VARCHAR(31)  ,DATABASE_TYPE_FLAG VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_connection_types(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_connection_types");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_connection_types VALUES( 'testTYPE_NAME3'  ,'testDATABASE_TYPE_FLAG3' )");
	  stmt.executeUpdate("INSERT INTO Meta_connection_types VALUES( 'testTYPE_NAME2'  ,'testDATABASE_TYPE_FLAG2' )");
	  stmt.executeUpdate("INSERT INTO Meta_connection_types VALUES( 'testTYPE_NAME1'  ,'testDATABASE_TYPE_FLAG1' )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_connection_typesFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_connection_types");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_connection_typesFactory constructor. All rows found from Meta_connection_types
   * table are put into vector.
   */
  @Test
  public void testMeta_connection_typesFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_connection_typesFactory(rockFactory, whereObject);

    /* Asserting all Meta_connection_typess are found and put into vector */
    try {
      Vector<Meta_connection_types> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getType_name() + ", " +  actualVector.get(1).getType_name() + ", " +  actualVector.get(2).getType_name();
      String expected = "3, testTYPE_NAME3, testTYPE_NAME2, testTYPE_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_connection_typess was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_connection_typesFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_connection_typesFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_connection_typesFactory constructor. All rows found from Meta_connection_types
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_connection_typesFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_connection_typesFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_connection_typess are found and put into vector */
    try {     
      Vector<Meta_connection_types> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_connection_typesFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_connection_typesFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_connection_typesFactory constructor. All rows found from Meta_connection_types
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_connection_typesFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_connection_typesFactory(rockFactory, whereObject, "ORDER BY TYPE_NAME");

    /* Asserting all Meta_connection_typess are found and put into vector */
    try {
      Vector<Meta_connection_types> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getType_name() + ", " +  actualVector.get(1).getType_name() + ", " +  actualVector.get(2).getType_name();
      String expected = "3, testTYPE_NAME1, testTYPE_NAME2, testTYPE_NAME3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_connection_typess was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_connection_typesFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_connection_typesFactory(null, whereObject, "ORDER BY TYPE_NAME");
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

    assertEquals( "testTYPE_NAME2" , objUnderTest.getElementAt(1).getType_name().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Meta_connection_types objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_connection_types objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_connection_types> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getType_name() + ", " +  actualVector.get(1).getType_name() + ", " +  actualVector.get(2).getType_name();
      String expected = "3, testTYPE_NAME3, testTYPE_NAME2, testTYPE_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_connection_types objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_connection_types testObject = new Meta_connection_types(rockFactory, "testTYPE_NAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_connection_types objects. True is returned if the two vectors
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
   * Test comparing two Meta_connection_types objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_connection_types objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_connection_types testObject = new Meta_connection_types(rockFactory, "testTYPE_NAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_connection_types objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_connection_types testObject = new Meta_connection_types(rockFactory, "testTYPE_NAME" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_connection_types");
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
    assertEquals(Meta_connection_typesFactory.class, clonedObject.getClass());
  }
}