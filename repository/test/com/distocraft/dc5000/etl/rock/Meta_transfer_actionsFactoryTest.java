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
 * Test class for Meta_transfer_actionsFactory. Testing handling of all the objects in
 * Meta_transfer_actions table.
 */
public class Meta_transfer_actionsFactoryTest {

  private static Meta_transfer_actionsFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_transfer_actions whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_transfer_actionsFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_transfer_actions ( VERSION_NUMBER VARCHAR(31)  ,TRANSFER_ACTION_ID BIGINT  ,COLLECTION_ID BIGINT  ,COLLECTION_SET_ID BIGINT  ,ACTION_TYPE VARCHAR(31) ,TRANSFER_ACTION_NAME VARCHAR(31) ,ORDER_BY_NO BIGINT  ,DESCRIPTION VARCHAR(31) ,WHERE_CLAUSE_01 VARCHAR(31) ,ACTION_CONTENTS_01 VARCHAR(31) ,ENABLED_FLAG VARCHAR(31) ,CONNECTION_ID BIGINT  ,WHERE_CLAUSE_02 VARCHAR(31) ,WHERE_CLAUSE_03 VARCHAR(31) ,ACTION_CONTENTS_02 VARCHAR(31) ,ACTION_CONTENTS_03 VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_transfer_actions(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_transfer_actions");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_transfer_actions VALUES( 'testVERSION_NUMBER3'  ,3  ,3  ,3  ,'testACTION_TYPE3'  ,'testTRANSFER_ACTION_NAME3'  ,3  ,'testDESCRIPTION3'  ,'testWHERE_CLAUSE_013'  ,'testACTION_CONTENTS_013'  ,'testENABLED_FLAG3'  ,3  ,'testWHERE_CLAUSE_023'  ,'testWHERE_CLAUSE_033'  ,'testACTION_CONTENTS_023'  ,'testACTION_CONTENTS_033' )");
	  stmt.executeUpdate("INSERT INTO Meta_transfer_actions VALUES( 'testVERSION_NUMBER2'  ,2  ,2  ,2  ,'testACTION_TYPE2'  ,'testTRANSFER_ACTION_NAME2'  ,2  ,'testDESCRIPTION2'  ,'testWHERE_CLAUSE_012'  ,'testACTION_CONTENTS_012'  ,'testENABLED_FLAG2'  ,2  ,'testWHERE_CLAUSE_022'  ,'testWHERE_CLAUSE_032'  ,'testACTION_CONTENTS_022'  ,'testACTION_CONTENTS_032' )");
	  stmt.executeUpdate("INSERT INTO Meta_transfer_actions VALUES( 'testVERSION_NUMBER1'  ,1  ,1  ,1  ,'testACTION_TYPE1'  ,'testTRANSFER_ACTION_NAME1'  ,1  ,'testDESCRIPTION1'  ,'testWHERE_CLAUSE_011'  ,'testACTION_CONTENTS_011'  ,'testENABLED_FLAG1'  ,1  ,'testWHERE_CLAUSE_021'  ,'testWHERE_CLAUSE_031'  ,'testACTION_CONTENTS_021'  ,'testACTION_CONTENTS_031' )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_transfer_actionsFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_transfer_actions");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_transfer_actionsFactory constructor. All rows found from Meta_transfer_actions
   * table are put into vector.
   */
  @Test
  public void testMeta_transfer_actionsFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transfer_actionsFactory(rockFactory, whereObject);

    /* Asserting all Meta_transfer_actionss are found and put into vector */
    try {
      Vector<Meta_transfer_actions> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersion_number() + ", " +  actualVector.get(1).getVersion_number() + ", " +  actualVector.get(2).getVersion_number();
      String expected = "3, testVERSION_NUMBER3, testVERSION_NUMBER2, testVERSION_NUMBER1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_transfer_actionss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_transfer_actionsFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_transfer_actionsFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_transfer_actionsFactory constructor. All rows found from Meta_transfer_actions
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_transfer_actionsFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transfer_actionsFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_transfer_actionss are found and put into vector */
    try {     
      Vector<Meta_transfer_actions> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_transfer_actionsFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_transfer_actionsFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_transfer_actionsFactory constructor. All rows found from Meta_transfer_actions
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_transfer_actionsFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transfer_actionsFactory(rockFactory, whereObject, "ORDER BY VERSION_NUMBER");

    /* Asserting all Meta_transfer_actionss are found and put into vector */
    try {
      Vector<Meta_transfer_actions> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersion_number() + ", " +  actualVector.get(1).getVersion_number() + ", " +  actualVector.get(2).getVersion_number();
      String expected = "3, testVERSION_NUMBER1, testVERSION_NUMBER2, testVERSION_NUMBER3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_transfer_actionss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_transfer_actionsFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_transfer_actionsFactory(null, whereObject, "ORDER BY VERSION_NUMBER");
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
   * Testing size retrieving of the vector containing Meta_transfer_actions objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_transfer_actions objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_transfer_actions> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersion_number() + ", " +  actualVector.get(1).getVersion_number() + ", " +  actualVector.get(2).getVersion_number();
      String expected = "3, testVERSION_NUMBER3, testVERSION_NUMBER2, testVERSION_NUMBER1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_transfer_actions objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_transfer_actions testObject = new Meta_transfer_actions(rockFactory, "testVERSION_NUMBER" + i, Long.parseLong(i + ""), Long.parseLong(i + ""), Long.parseLong(i + ""));
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_transfer_actions objects. True is returned if the two vectors
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
   * Test comparing two Meta_transfer_actions objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_transfer_actions objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_transfer_actions testObject = new Meta_transfer_actions(rockFactory, "testVERSION_NUMBER1", 1L, 1L, 1L);
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_transfer_actions objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_transfer_actions testObject = new Meta_transfer_actions(rockFactory, "testVERSION_NUMBER" + i, Long.parseLong(i + ""), Long.parseLong(i + ""), Long.parseLong(i + ""));
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_transfer_actions");
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
    assertEquals(Meta_transfer_actionsFactory.class, clonedObject.getClass());
  }
}