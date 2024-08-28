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
 * Test class for Meta_collectionsFactory. Testing handling of all the objects in
 * Meta_collections table.
 */
public class Meta_collectionsFactoryTest {

  private static Meta_collectionsFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_collections whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_collectionsFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_collections ( COLLECTION_ID BIGINT  ,COLLECTION_NAME VARCHAR(31) ,COLLECTION VARCHAR(31) ,MAIL_ERROR_ADDR VARCHAR(31) ,MAIL_FAIL_ADDR VARCHAR(31) ,MAIL_BUG_ADDR VARCHAR(31) ,MAX_ERRORS BIGINT  ,MAX_FK_ERRORS BIGINT  ,MAX_COL_LIMIT_ERRORS BIGINT  ,CHECK_FK_ERROR_FLAG VARCHAR(31) ,CHECK_COL_LIMITS_FLAG VARCHAR(31) ,LAST_TRANSFER_DATE TIMESTAMP  ,VERSION_NUMBER VARCHAR(31) ,COLLECTION_SET_ID BIGINT  ,USE_BATCH_ID VARCHAR(31) ,PRIORITY BIGINT  ,QUEUE_TIME_LIMIT BIGINT  ,ENABLED_FLAG VARCHAR(31) ,SETTYPE VARCHAR(31) ,FOLDABLE_FLAG VARCHAR(31) ,MEASTYPE VARCHAR(31) ,HOLD_FLAG VARCHAR(31) ,SCHEDULING_INFO VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_collections(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_collections");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_collections VALUES( 3  ,'testCOLLECTION_NAME3'  ,'testCOLLECTION3'  ,'testMAIL_ERROR_ADDR3'  ,'testMAIL_FAIL_ADDR3'  ,'testMAIL_BUG_ADDR3'  ,3  ,3  ,3  ,'testCHECK_FK_ERROR_FLAG3'  ,'testCHECK_COL_LIMITS_FLAG3'  ,'2003-03-03 00:00:00.0'  ,'testVERSION_NUMBER3'  ,3  ,'testUSE_BATCH_ID3'  ,3  ,3  ,'testENABLED_FLAG3'  ,'testSETTYPE3'  ,'testFOLDABLE_FLAG3'  ,'testMEASTYPE3'  ,'testHOLD_FLAG3'  ,'testSCHEDULING_INFO3' )");
	  stmt.executeUpdate("INSERT INTO Meta_collections VALUES( 2  ,'testCOLLECTION_NAME2'  ,'testCOLLECTION2'  ,'testMAIL_ERROR_ADDR2'  ,'testMAIL_FAIL_ADDR2'  ,'testMAIL_BUG_ADDR2'  ,2  ,2  ,2  ,'testCHECK_FK_ERROR_FLAG2'  ,'testCHECK_COL_LIMITS_FLAG2'  ,'2002-02-02 00:00:00.0'  ,'testVERSION_NUMBER2'  ,2  ,'testUSE_BATCH_ID2'  ,2  ,2  ,'testENABLED_FLAG2'  ,'testSETTYPE2'  ,'testFOLDABLE_FLAG2'  ,'testMEASTYPE2'  ,'testHOLD_FLAG2'  ,'testSCHEDULING_INFO2' )");
	  stmt.executeUpdate("INSERT INTO Meta_collections VALUES( 1  ,'testCOLLECTION_NAME1'  ,'testCOLLECTION1'  ,'testMAIL_ERROR_ADDR1'  ,'testMAIL_FAIL_ADDR1'  ,'testMAIL_BUG_ADDR1'  ,1  ,1  ,1  ,'testCHECK_FK_ERROR_FLAG1'  ,'testCHECK_COL_LIMITS_FLAG1'  ,'2001-01-01 00:00:00.0'  ,'testVERSION_NUMBER1'  ,1  ,'testUSE_BATCH_ID1'  ,1  ,1  ,'testENABLED_FLAG1'  ,'testSETTYPE1'  ,'testFOLDABLE_FLAG1'  ,'testMEASTYPE1'  ,'testHOLD_FLAG1'  ,'testSCHEDULING_INFO1' )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_collectionsFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_collections");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_collectionsFactory constructor. All rows found from Meta_collections
   * table are put into vector.
   */
  @Test
  public void testMeta_collectionsFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_collectionsFactory(rockFactory, whereObject);

    /* Asserting all Meta_collectionss are found and put into vector */
    try {
      Vector<Meta_collections> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getCollection_id() + ", " +  actualVector.get(1).getCollection_id() + ", " +  actualVector.get(2).getCollection_id();
      String expected = "3, 3, 2, 1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_collectionss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_collectionsFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_collectionsFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_collectionsFactory constructor. All rows found from Meta_collections
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_collectionsFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_collectionsFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_collectionss are found and put into vector */
    try {     
      Vector<Meta_collections> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_collectionsFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_collectionsFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_collectionsFactory constructor. All rows found from Meta_collections
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_collectionsFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_collectionsFactory(rockFactory, whereObject, "ORDER BY COLLECTION_ID");

    /* Asserting all Meta_collectionss are found and put into vector */
    try {
      Vector<Meta_collections> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getCollection_id() + ", " +  actualVector.get(1).getCollection_id() + ", " +  actualVector.get(2).getCollection_id();
      String expected = "3, 1, 2, 3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_collectionss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_collectionsFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_collectionsFactory(null, whereObject, "ORDER BY COLLECTION_ID");
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

    assertEquals("2", objUnderTest.getElementAt(1).getCollection_id().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Meta_collections objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_collections objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_collections> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getCollection_id() + ", " +  actualVector.get(1).getCollection_id() + ", " +  actualVector.get(2).getCollection_id();
      String expected = "3, 3, 2, 1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_collections objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_collections testObject = new Meta_collections(rockFactory, Long.parseLong(i + ""), "testVERSION_NUMBER" + i, Long.parseLong(i + ""));
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_collections objects. True is returned if the two vectors
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
   * Test comparing two Meta_collections objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_collections objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_collections testObject = new Meta_collections(rockFactory, 1L, "testVERSION_NUMBER1", 1L);
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_collections objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_collections testObject = new Meta_collections(rockFactory, Long.parseLong(i + ""), "testVERSION_NUMBER" + i, Long.parseLong(i + ""));
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_collections");
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
    assertEquals(Meta_collectionsFactory.class, clonedObject.getClass());
  }
}