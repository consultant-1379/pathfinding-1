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
 * Test class for Meta_execution_slotFactory. Testing handling of all the objects in
 * Meta_execution_slot table.
 */
public class Meta_execution_slotFactoryTest {

  private static Meta_execution_slotFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_execution_slot whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_execution_slotFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_execution_slot ( PROFILE_ID VARCHAR(38)  ,SLOT_NAME VARCHAR(15) ,SLOT_ID VARCHAR(38) ,ACCEPTED_SET_TYPES VARCHAR(2000) ,SERVICE_NODE VARCHAR(64))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_execution_slot(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_execution_slot");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_execution_slot VALUES( 'testPROFILE_ID3'  ,'testSLOT_NAME3'  ,'testSLOT_ID3'  ,'testACCEPTED_SET_TYPES3'  ,'testSERVICE_NODE3' )");
	  stmt.executeUpdate("INSERT INTO Meta_execution_slot VALUES( 'testPROFILE_ID2'  ,'testSLOT_NAME2'  ,'testSLOT_ID2'  ,'testACCEPTED_SET_TYPES2'  ,'testSERVICE_NODE2' )");
	  stmt.executeUpdate("INSERT INTO Meta_execution_slot VALUES( 'testPROFILE_ID1'  ,'testSLOT_NAME1'  ,'testSLOT_ID1'  ,'testACCEPTED_SET_TYPES1'  ,'testSERVICE_NODE1' )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_execution_slotFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_execution_slot");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_execution_slotFactory constructor. All rows found from Meta_execution_slot
   * table are put into vector.
   */
  @Test
  public void testMeta_execution_slotFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_execution_slotFactory(rockFactory, whereObject);

    /* Asserting all Meta_execution_slots are found and put into vector */
    try {
      Vector<Meta_execution_slot> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getProfile_id() + ", " +  actualVector.get(1).getProfile_id() + ", " +  actualVector.get(2).getProfile_id();
      String expected = "3, testPROFILE_ID3, testPROFILE_ID2, testPROFILE_ID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_execution_slots was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_execution_slotFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_execution_slotFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_execution_slotFactory constructor. All rows found from Meta_execution_slot
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_execution_slotFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_execution_slotFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_execution_slots are found and put into vector */
    try {     
      Vector<Meta_execution_slot> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_execution_slotFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_execution_slotFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_execution_slotFactory constructor. All rows found from Meta_execution_slot
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_execution_slotFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_execution_slotFactory(rockFactory, whereObject, "ORDER BY PROFILE_ID");

    /* Asserting all Meta_execution_slots are found and put into vector */
    try {
      Vector<Meta_execution_slot> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getProfile_id() + ", " +  actualVector.get(1).getProfile_id() + ", " +  actualVector.get(2).getProfile_id();
      String expected = "3, testPROFILE_ID1, testPROFILE_ID2, testPROFILE_ID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_execution_slots was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_execution_slotFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_execution_slotFactory(null, whereObject, "ORDER BY PROFILE_ID");
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

    assertEquals( "testPROFILE_ID2" , objUnderTest.getElementAt(1).getProfile_id().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Meta_execution_slot objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_execution_slot objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_execution_slot> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getProfile_id() + ", " +  actualVector.get(1).getProfile_id() + ", " +  actualVector.get(2).getProfile_id();
      String expected = "3, testPROFILE_ID3, testPROFILE_ID2, testPROFILE_ID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_execution_slot objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_execution_slot testObject = new Meta_execution_slot(rockFactory, "testSLOT_ID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_execution_slot objects. True is returned if the two vectors
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
   * Test comparing two Meta_execution_slot objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_execution_slot objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_execution_slot testObject = new Meta_execution_slot(rockFactory, "testSLOT_ID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_execution_slot objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_execution_slot testObject = new Meta_execution_slot(rockFactory, "testSLOT_ID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_execution_slot");
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
    assertEquals(Meta_execution_slotFactory.class, clonedObject.getClass());
  }
}