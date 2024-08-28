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
 * Test class for Meta_execution_slot_profileFactory. Testing handling of all the objects in
 * Meta_execution_slot_profile table.
 */
public class Meta_execution_slot_profileFactoryTest {

  private static Meta_execution_slot_profileFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_execution_slot_profile whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_execution_slot_profileFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_execution_slot_profile ( PROFILE_NAME VARCHAR(31)  ,PROFILE_ID VARCHAR(31) ,ACTIVE_FLAG VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_execution_slot_profile(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_execution_slot_profile");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_execution_slot_profile VALUES( 'testPROFILE_NAME3'  ,'testPROFILE_ID3'  ,'testACTIVE_FLAG3' )");
	  stmt.executeUpdate("INSERT INTO Meta_execution_slot_profile VALUES( 'testPROFILE_NAME2'  ,'testPROFILE_ID2'  ,'testACTIVE_FLAG2' )");
	  stmt.executeUpdate("INSERT INTO Meta_execution_slot_profile VALUES( 'testPROFILE_NAME1'  ,'testPROFILE_ID1'  ,'testACTIVE_FLAG1' )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_execution_slot_profileFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_execution_slot_profile");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_execution_slot_profileFactory constructor. All rows found from Meta_execution_slot_profile
   * table are put into vector.
   */
  @Test
  public void testMeta_execution_slot_profileFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_execution_slot_profileFactory(rockFactory, whereObject);

    /* Asserting all Meta_execution_slot_profiles are found and put into vector */
    try {
      Vector<Meta_execution_slot_profile> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getProfile_name() + ", " +  actualVector.get(1).getProfile_name() + ", " +  actualVector.get(2).getProfile_name();
      String expected = "3, testPROFILE_NAME3, testPROFILE_NAME2, testPROFILE_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_execution_slot_profiles was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_execution_slot_profileFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_execution_slot_profileFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_execution_slot_profileFactory constructor. All rows found from Meta_execution_slot_profile
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_execution_slot_profileFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_execution_slot_profileFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_execution_slot_profiles are found and put into vector */
    try {     
      Vector<Meta_execution_slot_profile> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_execution_slot_profileFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_execution_slot_profileFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_execution_slot_profileFactory constructor. All rows found from Meta_execution_slot_profile
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_execution_slot_profileFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_execution_slot_profileFactory(rockFactory, whereObject, "ORDER BY PROFILE_NAME");

    /* Asserting all Meta_execution_slot_profiles are found and put into vector */
    try {
      Vector<Meta_execution_slot_profile> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getProfile_name() + ", " +  actualVector.get(1).getProfile_name() + ", " +  actualVector.get(2).getProfile_name();
      String expected = "3, testPROFILE_NAME1, testPROFILE_NAME2, testPROFILE_NAME3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_execution_slot_profiles was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_execution_slot_profileFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_execution_slot_profileFactory(null, whereObject, "ORDER BY PROFILE_NAME");
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

    assertEquals( "testPROFILE_NAME2" , objUnderTest.getElementAt(1).getProfile_name().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Meta_execution_slot_profile objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_execution_slot_profile objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_execution_slot_profile> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getProfile_name() + ", " +  actualVector.get(1).getProfile_name() + ", " +  actualVector.get(2).getProfile_name();
      String expected = "3, testPROFILE_NAME3, testPROFILE_NAME2, testPROFILE_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_execution_slot_profile objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_execution_slot_profile testObject = new Meta_execution_slot_profile(rockFactory, "testPROFILE_ID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_execution_slot_profile objects. True is returned if the two vectors
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
   * Test comparing two Meta_execution_slot_profile objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_execution_slot_profile objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_execution_slot_profile testObject = new Meta_execution_slot_profile(rockFactory, "testPROFILE_ID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_execution_slot_profile objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_execution_slot_profile testObject = new Meta_execution_slot_profile(rockFactory, "testPROFILE_ID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_execution_slot_profile");
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
    assertEquals(Meta_execution_slot_profileFactory.class, clonedObject.getClass());
  }
}