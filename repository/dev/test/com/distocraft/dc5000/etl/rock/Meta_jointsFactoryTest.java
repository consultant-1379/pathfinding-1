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
 * Test class for Meta_jointsFactory. Testing handling of all the objects in
 * Meta_joints table.
 */
public class Meta_jointsFactoryTest {

  private static Meta_jointsFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_joints whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_jointsFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_joints ( ID BIGINT  ,IS_PK_COLUMN VARCHAR(31) ,IS_SUM_COLUMN VARCHAR(31) ,IS_GROUP_BY_COLUMN VARCHAR(31) ,COLUMN_SPACE_AT_FILE BIGINT  ,FILE_ORDER_BY BIGINT  ,PLUGIN_METHOD_NAME VARCHAR(31) ,VERSION_NUMBER VARCHAR(31) ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,TRANSFER_ACTION_ID BIGINT  ,TARGET_CONNECTION_ID BIGINT  ,TARGET_TABLE_ID BIGINT  ,COLUMN_ID_TARGET_COLUMN BIGINT  ,SOURCE_CONNECTION_ID BIGINT  ,SOURCE_TABLE_ID BIGINT  ,COLUMN_ID_SOURCE_COLUMN BIGINT  ,TRANSFORMATION_ID BIGINT  ,TRANSF_TABLE_ID BIGINT  ,PAR_NAME VARCHAR(31) ,FILE_ID BIGINT  ,PLUGIN_ID BIGINT  ,FREE_FORMAT_TRANSFORMAT VARCHAR(31) ,METHOD_PARAMETER VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_joints(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_joints");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_joints VALUES( 3  ,'testIS_PK_COLUMN3'  ,'testIS_SUM_COLUMN3'  ,'testIS_GROUP_BY_COLUMN3'  ,3  ,3  ,'testPLUGIN_METHOD_NAME3'  ,'testVERSION_NUMBER3'  ,3  ,3  ,3  ,3  ,3  ,3  ,3  ,3  ,3  ,3  ,3  ,'testPAR_NAME3'  ,3  ,3  ,'testFREE_FORMAT_TRANSFORMAT3'  ,'testMETHOD_PARAMETER3' )");
	  stmt.executeUpdate("INSERT INTO Meta_joints VALUES( 2  ,'testIS_PK_COLUMN2'  ,'testIS_SUM_COLUMN2'  ,'testIS_GROUP_BY_COLUMN2'  ,2  ,2  ,'testPLUGIN_METHOD_NAME2'  ,'testVERSION_NUMBER2'  ,2  ,2  ,2  ,2  ,2  ,2  ,2  ,2  ,2  ,2  ,2  ,'testPAR_NAME2'  ,2  ,2  ,'testFREE_FORMAT_TRANSFORMAT2'  ,'testMETHOD_PARAMETER2' )");
	  stmt.executeUpdate("INSERT INTO Meta_joints VALUES( 1  ,'testIS_PK_COLUMN1'  ,'testIS_SUM_COLUMN1'  ,'testIS_GROUP_BY_COLUMN1'  ,1  ,1  ,'testPLUGIN_METHOD_NAME1'  ,'testVERSION_NUMBER1'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,'testPAR_NAME1'  ,1  ,1  ,'testFREE_FORMAT_TRANSFORMAT1'  ,'testMETHOD_PARAMETER1' )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_jointsFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_joints");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_jointsFactory constructor. All rows found from Meta_joints
   * table are put into vector.
   */
  @Test
  public void testMeta_jointsFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_jointsFactory(rockFactory, whereObject);

    /* Asserting all Meta_jointss are found and put into vector */
    try {
      Vector<Meta_joints> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getId() + ", " +  actualVector.get(1).getId() + ", " +  actualVector.get(2).getId();
      String expected = "3, 3, 2, 1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_jointss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_jointsFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_jointsFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_jointsFactory constructor. All rows found from Meta_joints
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_jointsFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_jointsFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_jointss are found and put into vector */
    try {     
      Vector<Meta_joints> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_jointsFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_jointsFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_jointsFactory constructor. All rows found from Meta_joints
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_jointsFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_jointsFactory(rockFactory, whereObject, "ORDER BY ID");

    /* Asserting all Meta_jointss are found and put into vector */
    try {
      Vector<Meta_joints> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getId() + ", " +  actualVector.get(1).getId() + ", " +  actualVector.get(2).getId();
      String expected = "3, 1, 2, 3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_jointss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_jointsFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_jointsFactory(null, whereObject, "ORDER BY ID");
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
   * Testing size retrieving of the vector containing Meta_joints objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_joints objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_joints> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getId() + ", " +  actualVector.get(1).getId() + ", " +  actualVector.get(2).getId();
      String expected = "3, 3, 2, 1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_joints objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_joints testObject = new Meta_joints(rockFactory, Long.parseLong(i + ""));
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_joints objects. True is returned if the two vectors
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
   * Test comparing two Meta_joints objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_joints objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_joints testObject = new Meta_joints(rockFactory, 1L);
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_joints objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_joints testObject = new Meta_joints(rockFactory, Long.parseLong(i + ""));
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_joints");
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
    assertEquals(Meta_jointsFactory.class, clonedObject.getClass());
  }
}