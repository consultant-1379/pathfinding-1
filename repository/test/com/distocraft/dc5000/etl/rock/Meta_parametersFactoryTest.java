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
 * Test class for Meta_parametersFactory. Testing handling of all the objects in
 * Meta_parameters table.
 */
public class Meta_parametersFactoryTest {

  private static Meta_parametersFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_parameters whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_parametersFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_parameters ( RB_SEGMENT_NAME VARCHAR(30)  ,USE_RB_SEGMENT_FLAG VARCHAR(1) ,DEFAULT_ERROR_MAIL_ADDR VARCHAR(100) ,DEFAULT_FAIL_MAIL_ADDR VARCHAR(100) ,DEFAULT_BUG_ERROR_MAIL_ADDR VARCHAR(100) ,DEFAULT_MAX_ERROR_VALUE BIGINT  ,DEFAULT_MAX_FK_ERROR_VALUE BIGINT  ,DEFAULT_MAX_COL_LIMIT_VALUE BIGINT  ,TEMP_SUM_TABLESPACE VARCHAR(30) ,USE_TEMP_SUM_TABLESPACE_FLAG VARCHAR(1) ,BATCH_COLUMN_NAME VARCHAR(30) ,VERSION_NUMBER VARCHAR(32))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_parameters(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_parameters");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_parameters VALUES( 'testRB_SEGMENT_NAME3'  ,'t'  ,'testDEFAULT_ERROR_MAIL_ADDR3'  ,'testDEFAULT_FAIL_MAIL_ADDR3'  ,'testDEFAULT_BUG_ERROR_MAIL_ADDR3'  ,3  ,3  ,3  ,'testTEMP_SUM_TABLESPACE3'  ,'t'  ,'testBATCH_COLUMN_NAME3'  ,'testVERSION_NUMBER3' )");
	  stmt.executeUpdate("INSERT INTO Meta_parameters VALUES( 'testRB_SEGMENT_NAME2'  ,'t'  ,'testDEFAULT_ERROR_MAIL_ADDR2'  ,'testDEFAULT_FAIL_MAIL_ADDR2'  ,'testDEFAULT_BUG_ERROR_MAIL_ADDR2'  ,2  ,2  ,2  ,'testTEMP_SUM_TABLESPACE2'  ,'t'  ,'testBATCH_COLUMN_NAME2'  ,'testVERSION_NUMBER2' )");
	  stmt.executeUpdate("INSERT INTO Meta_parameters VALUES( 'testRB_SEGMENT_NAME1'  ,'t'  ,'testDEFAULT_ERROR_MAIL_ADDR1'  ,'testDEFAULT_FAIL_MAIL_ADDR1'  ,'testDEFAULT_BUG_ERROR_MAIL_ADDR1'  ,1  ,1  ,1  ,'testTEMP_SUM_TABLESPACE1'  ,'t'  ,'testBATCH_COLUMN_NAME1'  ,'testVERSION_NUMBER1' )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_parametersFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_parameters");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_parametersFactory constructor. All rows found from Meta_parameters
   * table are put into vector.
   */
  @Test
  public void testMeta_parametersFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_parametersFactory(rockFactory, whereObject);

    /* Asserting all Meta_parameterss are found and put into vector */
    try {
      Vector<Meta_parameters> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getRb_segment_name() + ", " +  actualVector.get(1).getRb_segment_name() + ", " +  actualVector.get(2).getRb_segment_name();
      String expected = "3, testRB_SEGMENT_NAME3, testRB_SEGMENT_NAME2, testRB_SEGMENT_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_parameterss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_parametersFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_parametersFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_parametersFactory constructor. All rows found from Meta_parameters
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_parametersFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_parametersFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_parameterss are found and put into vector */
    try {     
      Vector<Meta_parameters> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_parametersFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_parametersFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_parametersFactory constructor. All rows found from Meta_parameters
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_parametersFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_parametersFactory(rockFactory, whereObject, "ORDER BY RB_SEGMENT_NAME");

    /* Asserting all Meta_parameterss are found and put into vector */
    try {
      Vector<Meta_parameters> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getRb_segment_name() + ", " +  actualVector.get(1).getRb_segment_name() + ", " +  actualVector.get(2).getRb_segment_name();
      String expected = "3, testRB_SEGMENT_NAME1, testRB_SEGMENT_NAME2, testRB_SEGMENT_NAME3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_parameterss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_parametersFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_parametersFactory(null, whereObject, "ORDER BY RB_SEGMENT_NAME");
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

    assertEquals( "testRB_SEGMENT_NAME2" , objUnderTest.getElementAt(1).getRb_segment_name().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Meta_parameters objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_parameters objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_parameters> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getRb_segment_name() + ", " +  actualVector.get(1).getRb_segment_name() + ", " +  actualVector.get(2).getRb_segment_name();
      String expected = "3, testRB_SEGMENT_NAME3, testRB_SEGMENT_NAME2, testRB_SEGMENT_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_parameters objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_parameters testObject = new Meta_parameters(rockFactory, "testVERSION_NUMBER" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_parameters objects. True is returned if the two vectors
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
   * Test comparing two Meta_parameters objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_parameters objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_parameters testObject = new Meta_parameters(rockFactory, "testVERSION_NUMBER1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_parameters objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_parameters testObject = new Meta_parameters(rockFactory, "testVERSION_NUMBER" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_parameters");
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
    assertEquals(Meta_parametersFactory.class, clonedObject.getClass());
  }
}