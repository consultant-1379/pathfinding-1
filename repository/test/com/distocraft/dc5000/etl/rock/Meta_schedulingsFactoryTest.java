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
 * Test class for Meta_schedulingsFactory. Testing handling of all the objects in
 * Meta_schedulings table.
 */
public class Meta_schedulingsFactoryTest {

  private static Meta_schedulingsFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_schedulings whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_schedulingsFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_schedulings ( VERSION_NUMBER VARCHAR(31)  ,ID BIGINT  ,EXECUTION_TYPE VARCHAR(31) ,OS_COMMAND VARCHAR(31) ,SCHEDULING_MONTH BIGINT  ,SCHEDULING_DAY BIGINT  ,SCHEDULING_HOUR BIGINT  ,SCHEDULING_MIN BIGINT  ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,MON_FLAG VARCHAR(31) ,TUE_FLAG VARCHAR(31) ,WED_FLAG VARCHAR(31) ,THU_FLAG VARCHAR(31) ,FRI_FLAG VARCHAR(31) ,SAT_FLAG VARCHAR(31) ,SUN_FLAG VARCHAR(31) ,STATUS VARCHAR(31) ,LAST_EXECUTION_TIME TIMESTAMP  ,INTERVAL_HOUR BIGINT  ,INTERVAL_MIN BIGINT  ,NAME VARCHAR(31) ,HOLD_FLAG VARCHAR(31) ,PRIORITY BIGINT  ,SCHEDULING_YEAR BIGINT  ,TRIGGER_COMMAND VARCHAR(31) ,LAST_EXEC_TIME_MS BIGINT )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_schedulings(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_schedulings");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_schedulings VALUES( 'testVERSION_NUMBER3'  ,3  ,'testEXECUTION_TYPE3'  ,'testOS_COMMAND3'  ,3  ,3  ,3  ,3  ,3  ,3  ,'testMON_FLAG3'  ,'testTUE_FLAG3'  ,'testWED_FLAG3'  ,'testTHU_FLAG3'  ,'testFRI_FLAG3'  ,'testSAT_FLAG3'  ,'testSUN_FLAG3'  ,'testSTATUS3'  ,'2003-03-03 00:00:00.0'  ,3  ,3  ,'testNAME3'  ,'testHOLD_FLAG3'  ,3  ,3  ,'testTRIGGER_COMMAND3'  ,3 )");
	  stmt.executeUpdate("INSERT INTO Meta_schedulings VALUES( 'testVERSION_NUMBER2'  ,2  ,'testEXECUTION_TYPE2'  ,'testOS_COMMAND2'  ,2  ,2  ,2  ,2  ,2  ,2  ,'testMON_FLAG2'  ,'testTUE_FLAG2'  ,'testWED_FLAG2'  ,'testTHU_FLAG2'  ,'testFRI_FLAG2'  ,'testSAT_FLAG2'  ,'testSUN_FLAG2'  ,'testSTATUS2'  ,'2002-02-02 00:00:00.0'  ,2  ,2  ,'testNAME2'  ,'testHOLD_FLAG2'  ,2  ,2  ,'testTRIGGER_COMMAND2'  ,2 )");
	  stmt.executeUpdate("INSERT INTO Meta_schedulings VALUES( 'testVERSION_NUMBER1'  ,1  ,'testEXECUTION_TYPE1'  ,'testOS_COMMAND1'  ,1  ,1  ,1  ,1  ,1  ,1  ,'testMON_FLAG1'  ,'testTUE_FLAG1'  ,'testWED_FLAG1'  ,'testTHU_FLAG1'  ,'testFRI_FLAG1'  ,'testSAT_FLAG1'  ,'testSUN_FLAG1'  ,'testSTATUS1'  ,'2001-01-01 00:00:00.0'  ,1  ,1  ,'testNAME1'  ,'testHOLD_FLAG1'  ,1  ,1  ,'testTRIGGER_COMMAND1'  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_schedulingsFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_schedulings");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_schedulingsFactory constructor. All rows found from Meta_schedulings
   * table are put into vector.
   */
  @Test
  public void testMeta_schedulingsFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_schedulingsFactory(rockFactory, whereObject);

    /* Asserting all Meta_schedulingss are found and put into vector */
    try {
      Vector<Meta_schedulings> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersion_number() + ", " +  actualVector.get(1).getVersion_number() + ", " +  actualVector.get(2).getVersion_number();
      String expected = "3, testVERSION_NUMBER3, testVERSION_NUMBER2, testVERSION_NUMBER1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_schedulingss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_schedulingsFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_schedulingsFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_schedulingsFactory constructor. All rows found from Meta_schedulings
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_schedulingsFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_schedulingsFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_schedulingss are found and put into vector */
    try {     
      Vector<Meta_schedulings> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_schedulingsFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_schedulingsFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_schedulingsFactory constructor. All rows found from Meta_schedulings
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_schedulingsFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_schedulingsFactory(rockFactory, whereObject, "ORDER BY VERSION_NUMBER");

    /* Asserting all Meta_schedulingss are found and put into vector */
    try {
      Vector<Meta_schedulings> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersion_number() + ", " +  actualVector.get(1).getVersion_number() + ", " +  actualVector.get(2).getVersion_number();
      String expected = "3, testVERSION_NUMBER1, testVERSION_NUMBER2, testVERSION_NUMBER3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_schedulingss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_schedulingsFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_schedulingsFactory(null, whereObject, "ORDER BY VERSION_NUMBER");
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

    assertEquals( "testVERSION_NUMBER2" , objUnderTest.getElementAt(1).getVersion_number().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Meta_schedulings objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_schedulings objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_schedulings> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersion_number() + ", " +  actualVector.get(1).getVersion_number() + ", " +  actualVector.get(2).getVersion_number();
      String expected = "3, testVERSION_NUMBER3, testVERSION_NUMBER2, testVERSION_NUMBER1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_schedulings objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_schedulings testObject = new Meta_schedulings(rockFactory, Long.parseLong(i + ""));
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_schedulings objects. True is returned if the two vectors
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
   * Test comparing two Meta_schedulings objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_schedulings objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_schedulings testObject = new Meta_schedulings(rockFactory, 1L);
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_schedulings objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_schedulings testObject = new Meta_schedulings(rockFactory, Long.parseLong(i + ""));
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_schedulings");
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
    assertEquals(Meta_schedulingsFactory.class, clonedObject.getClass());
  }
}