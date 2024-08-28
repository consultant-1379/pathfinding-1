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
 * Test class for Meta_transfer_batchesFactory. Testing handling of all the objects in
 * Meta_transfer_batches table.
 */
public class Meta_transfer_batchesFactoryTest {

  private static Meta_transfer_batchesFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_transfer_batches whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_transfer_batchesFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_transfer_batches ( ID BIGINT  ,START_DATE TIMESTAMP  ,END_DATE TIMESTAMP  ,FAIL_FLAG VARCHAR(1) ,STATUS VARCHAR(10) ,VERSION_NUMBER VARCHAR(32) ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,META_COLLECTION_NAME VARCHAR(128) ,META_COLLECTION_SET_NAME VARCHAR(128) ,SETTYPE VARCHAR(10) ,SLOT_ID INTEGER  ,SCHEDULING_INFO VARCHAR(16000) ,SERVICE_NODE VARCHAR(64))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_transfer_batches(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_transfer_batches");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_transfer_batches VALUES( 3  ,'2003-03-03 00:00:00.0'  ,'2003-03-03 00:00:00.0'  ,'t'  ,'testSTATUS'  ,'testVERSION_NUMBER3'  ,3  ,3  ,'testMETA_COLLECTION_NAME3'  ,'testMETA_COLLECTION_SET_NAME3'  ,'testSETTYP'  ,3  ,'testSCHEDULING_INFO3'  ,'testSERVICE_NODE3' )");
	  stmt.executeUpdate("INSERT INTO Meta_transfer_batches VALUES( 2  ,'2002-02-02 00:00:00.0'  ,'2002-02-02 00:00:00.0'  ,'t'  ,'testSTATUS'  ,'testVERSION_NUMBER2'  ,2  ,2  ,'testMETA_COLLECTION_NAME2'  ,'testMETA_COLLECTION_SET_NAME2'  ,'testSETTYP'  ,2  ,'testSCHEDULING_INFO2'  ,'testSERVICE_NODE2' )");
	  stmt.executeUpdate("INSERT INTO Meta_transfer_batches VALUES( 1  ,'2001-01-01 00:00:00.0'  ,'2001-01-01 00:00:00.0'  ,'t'  ,'testSTATUS'  ,'testVERSION_NUMBER1'  ,1  ,1  ,'testMETA_COLLECTION_NAME1'  ,'testMETA_COLLECTION_SET_NAME1'  ,'testSETTYP'  ,1  ,'testSCHEDULING_INFO1'  ,'testSERVICE_NODE1' )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_transfer_batchesFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_transfer_batches");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_transfer_batchesFactory constructor. All rows found from Meta_transfer_batches
   * table are put into vector.
   */
  @Test
  public void testMeta_transfer_batchesFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transfer_batchesFactory(rockFactory, whereObject);

    /* Asserting all Meta_transfer_batchess are found and put into vector */
    try {
      Vector<Meta_transfer_batches> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getId() + ", " +  actualVector.get(1).getId() + ", " +  actualVector.get(2).getId();
      String expected = "3, 3, 2, 1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_transfer_batchess was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_transfer_batchesFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_transfer_batchesFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_transfer_batchesFactory constructor. All rows found from Meta_transfer_batches
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_transfer_batchesFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transfer_batchesFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_transfer_batchess are found and put into vector */
    try {     
      Vector<Meta_transfer_batches> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_transfer_batchesFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_transfer_batchesFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_transfer_batchesFactory constructor. All rows found from Meta_transfer_batches
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_transfer_batchesFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transfer_batchesFactory(rockFactory, whereObject, "ORDER BY ID");

    /* Asserting all Meta_transfer_batchess are found and put into vector */
    try {
      Vector<Meta_transfer_batches> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getId() + ", " +  actualVector.get(1).getId() + ", " +  actualVector.get(2).getId();
      String expected = "3, 1, 2, 3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_transfer_batchess was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_transfer_batchesFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_transfer_batchesFactory(null, whereObject, "ORDER BY ID");
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

    assertEquals("2", objUnderTest.getElementAt(1).getId().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Meta_transfer_batches objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_transfer_batches objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_transfer_batches> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getId() + ", " +  actualVector.get(1).getId() + ", " +  actualVector.get(2).getId();
      String expected = "3, 3, 2, 1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_transfer_batches objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_transfer_batches testObject = new Meta_transfer_batches(rockFactory, Long.parseLong(i + ""), "testVERSION_NUMBER" + i, "testMETA_COLLECTION_NAME" + i, "testMETA_COLLECTION_SET_NAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_transfer_batches objects. True is returned if the two vectors
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
   * Test comparing two Meta_transfer_batches objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_transfer_batches objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_transfer_batches testObject = new Meta_transfer_batches(rockFactory, 1L, "testVERSION_NUMBER1", "testMETA_COLLECTION_NAME1", "testMETA_COLLECTION_SET_NAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_transfer_batches objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_transfer_batches testObject = new Meta_transfer_batches(rockFactory, Long.parseLong(i + ""), "testVERSION_NUMBER" + i, "testMETA_COLLECTION_NAME" + i, "testMETA_COLLECTION_SET_NAME" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_transfer_batches");
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
    assertEquals(Meta_transfer_batchesFactory.class, clonedObject.getClass());
  }
}