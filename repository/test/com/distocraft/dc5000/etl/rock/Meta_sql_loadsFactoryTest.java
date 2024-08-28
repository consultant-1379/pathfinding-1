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
 * Test class for Meta_sql_loadsFactory. Testing handling of all the objects in
 * Meta_sql_loads table.
 */
public class Meta_sql_loadsFactoryTest {

  private static Meta_sql_loadsFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Meta_sql_loads whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = Meta_sql_loadsFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_sql_loads ( INPUT_FILE VARCHAR(31)  ,CTL_FILE VARCHAR(31) ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,CONNECTION_ID BIGINT  ,DIS_FILE VARCHAR(31) ,BAD_FILE VARCHAR(31) ,LOAD_TYPE VARCHAR(31) ,TEXT VARCHAR(31) ,DELIM VARCHAR(31) ,SQLLDR_CMD VARCHAR(31) ,LOAD_OPTION VARCHAR(31) ,VERSION_NUMBER VARCHAR(31) ,TRANSFER_ACTION_ID BIGINT  ,TABLE_ID BIGINT )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Meta_sql_loads(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_sql_loads");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Meta_sql_loads VALUES( 'testINPUT_FILE3'  ,'testCTL_FILE3'  ,3  ,3  ,3  ,'testDIS_FILE3'  ,'testBAD_FILE3'  ,'testLOAD_TYPE3'  ,'testTEXT3'  ,'testDELIM3'  ,'testSQLLDR_CMD3'  ,'testLOAD_OPTION3'  ,'testVERSION_NUMBER3'  ,3  ,3 )");
	  stmt.executeUpdate("INSERT INTO Meta_sql_loads VALUES( 'testINPUT_FILE2'  ,'testCTL_FILE2'  ,2  ,2  ,2  ,'testDIS_FILE2'  ,'testBAD_FILE2'  ,'testLOAD_TYPE2'  ,'testTEXT2'  ,'testDELIM2'  ,'testSQLLDR_CMD2'  ,'testLOAD_OPTION2'  ,'testVERSION_NUMBER2'  ,2  ,2 )");
	  stmt.executeUpdate("INSERT INTO Meta_sql_loads VALUES( 'testINPUT_FILE1'  ,'testCTL_FILE1'  ,1  ,1  ,1  ,'testDIS_FILE1'  ,'testBAD_FILE1'  ,'testLOAD_TYPE1'  ,'testTEXT1'  ,'testDELIM1'  ,'testSQLLDR_CMD1'  ,'testLOAD_OPTION1'  ,'testVERSION_NUMBER1'  ,1  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new Meta_sql_loadsFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_sql_loads");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_sql_loadsFactory constructor. All rows found from Meta_sql_loads
   * table are put into vector.
   */
  @Test
  public void testMeta_sql_loadsFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_sql_loadsFactory(rockFactory, whereObject);

    /* Asserting all Meta_sql_loadss are found and put into vector */
    try {
      Vector<Meta_sql_loads> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInput_file() + ", " +  actualVector.get(1).getInput_file() + ", " +  actualVector.get(2).getInput_file();
      String expected = "3, testINPUT_FILE3, testINPUT_FILE2, testINPUT_FILE1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_sql_loadss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_sql_loadsFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_sql_loadsFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_sql_loadsFactory constructor. All rows found from Meta_sql_loads
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_sql_loadsFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_sql_loadsFactory(rockFactory, whereObject, true);

    /* Asserting all Meta_sql_loadss are found and put into vector */
    try {     
      Vector<Meta_sql_loads> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeta_sql_loadsFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_sql_loadsFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_sql_loadsFactory constructor. All rows found from Meta_sql_loads
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeta_sql_loadsFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_sql_loadsFactory(rockFactory, whereObject, "ORDER BY INPUT_FILE");

    /* Asserting all Meta_sql_loadss are found and put into vector */
    try {
      Vector<Meta_sql_loads> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInput_file() + ", " +  actualVector.get(1).getInput_file() + ", " +  actualVector.get(2).getInput_file();
      String expected = "3, testINPUT_FILE1, testINPUT_FILE2, testINPUT_FILE3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Meta_sql_loadss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_sql_loadsFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_sql_loadsFactory(null, whereObject, "ORDER BY INPUT_FILE");
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

    assertEquals( "testINPUT_FILE2" , objUnderTest.getElementAt(1).getInput_file().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Meta_sql_loads objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Meta_sql_loads objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Meta_sql_loads> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getInput_file() + ", " +  actualVector.get(1).getInput_file() + ", " +  actualVector.get(2).getInput_file();
      String expected = "3, testINPUT_FILE3, testINPUT_FILE2, testINPUT_FILE1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Meta_sql_loads objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Meta_sql_loads testObject = new Meta_sql_loads(rockFactory, Long.parseLong(i + ""), Long.parseLong(i + ""), Long.parseLong(i + ""), "testVERSION_NUMBER" + i, Long.parseLong(i + ""), Long.parseLong(i + ""));
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_sql_loads objects. True is returned if the two vectors
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
   * Test comparing two Meta_sql_loads objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_sql_loads objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Meta_sql_loads testObject = new Meta_sql_loads(rockFactory, 1L, 1L, 1L, "testVERSION_NUMBER1", 1L, 1L);
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Meta_sql_loads objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Meta_sql_loads testObject = new Meta_sql_loads(rockFactory, Long.parseLong(i + ""), Long.parseLong(i + ""), Long.parseLong(i + ""), "testVERSION_NUMBER" + i, Long.parseLong(i + ""), Long.parseLong(i + ""));
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_sql_loads");
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
    assertEquals(Meta_sql_loadsFactory.class, clonedObject.getClass());
  }
}