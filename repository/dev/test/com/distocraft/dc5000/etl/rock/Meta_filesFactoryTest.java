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
 * Test class for Meta_filesFactory. Testing handling of all the objects in
 * Meta_files table.
 */
public class Meta_filesFactoryTest {

  private static Meta_filesFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_files whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_filesFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_files ( FILE_ID BIGINT  ,FILE_NAME VARCHAR(31) ,FILE_CONTENT_TYPE VARCHAR(31) ,ROW_DELIM VARCHAR(31) ,COLUMN_DELIM VARCHAR(31) ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,COMMIT_AFTER_N_ROWS BIGINT  ,IS_SOURCE VARCHAR(31) ,VERSION_NUMBER VARCHAR(31) ,TRANSFER_ACTION_ID BIGINT )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_files(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_files");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_files VALUES( 3  ,'testFILE_NAME3'  ,'testFILE_CONTENT_TYPE3'  ,'testROW_DELIM3'  ,'testCOLUMN_DELIM3'  ,3  ,3  ,3  ,'testIS_SOURCE3'  ,'testVERSION_NUMBER3'  ,3 )");
	  stmt.executeUpdate("INSERT INTO Meta_files VALUES( 2  ,'testFILE_NAME2'  ,'testFILE_CONTENT_TYPE2'  ,'testROW_DELIM2'  ,'testCOLUMN_DELIM2'  ,2  ,2  ,2  ,'testIS_SOURCE2'  ,'testVERSION_NUMBER2'  ,2 )");
	  stmt.executeUpdate("INSERT INTO Meta_files VALUES( 1  ,'testFILE_NAME1'  ,'testFILE_CONTENT_TYPE1'  ,'testROW_DELIM1'  ,'testCOLUMN_DELIM1'  ,1  ,1  ,1  ,'testIS_SOURCE1'  ,'testVERSION_NUMBER1'  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_filesFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_files");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_filesFactory constructor. All rows found from Meta_files
   * table are put into vector.
   */
  @Test
  public void testMeta_filesFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_filesFactory(rockFactory, whereObject);

    /* Asserting all Meta_filess are found and put into vector */
    try {
      Vector<Meta_files> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getFile_id() + ", " +  actualVector.get(1).getFile_id() + ", " +  actualVector.get(2).getFile_id();
      String expected = "3, 3, 2, 1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_filess was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_filesFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_filesFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_filesFactory constructor. All rows found from Meta_files
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_filesFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_filesFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_filess are found and put into vector */
    try {     
      Vector<Meta_files> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_filesFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_filesFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_filesFactory constructor. All rows found from Meta_files
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_filesFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_filesFactory(rockFactory, whereObject, "ORDER BY FILE_ID");

    /* Asserting all Meta_filess are found and put into vector */
    try {
      Vector<Meta_files> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getFile_id() + ", " +  actualVector.get(1).getFile_id() + ", " +  actualVector.get(2).getFile_id();
      String expected = "3, 1, 2, 3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_filess was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_filesFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_filesFactory(null, whereObject, "ORDER BY FILE_ID");
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

    assertEquals("2", objUnderTest.getElementAt(1).getFile_id().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Meta_files objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_files objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_files> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getFile_id() + ", " +  actualVector.get(1).getFile_id() + ", " +  actualVector.get(2).getFile_id();
      String expected = "3, 3, 2, 1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_files objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_files testObject = new Meta_files(rockFactory, Long.parseLong(i + ""), Long.parseLong(i + ""), Long.parseLong(i + ""), "testVERSION_NUMBER" + i, Long.parseLong(i + ""));
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_files objects. True is returned if the two vectors
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
   * Test comparing two Meta_files objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_files objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_files testObject = new Meta_files(rockFactory, 1L, 1L, 1L, "testVERSION_NUMBER1", 1L);
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_files objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_files testObject = new Meta_files(rockFactory, Long.parseLong(i + ""), Long.parseLong(i + ""), Long.parseLong(i + ""), "testVERSION_NUMBER" + i, Long.parseLong(i + ""));
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_files");
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
    assertEquals(Meta_filesFactory.class, clonedObject.getClass());
  }
}