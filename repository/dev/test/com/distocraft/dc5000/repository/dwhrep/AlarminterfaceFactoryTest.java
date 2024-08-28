package com.distocraft.dc5000.repository.dwhrep;

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
 * Test class for AlarminterfaceFactory. Testing handling of all the objects in
 * Alarminterface table.
 */
public class AlarminterfaceFactoryTest {

  private static AlarminterfaceFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Alarminterface whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = AlarminterfaceFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Alarminterface ( INTERFACEID VARCHAR(31)  ,DESCRIPTION VARCHAR(31) ,STATUS VARCHAR(31) ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,QUEUE_NUMBER BIGINT )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Alarminterface(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Alarminterface");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Alarminterface VALUES( 'testINTERFACEID3'  ,'testDESCRIPTION3'  ,'testSTATUS3'  ,3  ,3  ,3 )");
	  stmt.executeUpdate("INSERT INTO Alarminterface VALUES( 'testINTERFACEID2'  ,'testDESCRIPTION2'  ,'testSTATUS2'  ,2  ,2  ,2 )");
	  stmt.executeUpdate("INSERT INTO Alarminterface VALUES( 'testINTERFACEID1'  ,'testDESCRIPTION1'  ,'testSTATUS1'  ,1  ,1  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new AlarminterfaceFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Alarminterface");
    objUnderTest = null;
  }
  
  /**
   * Testing AlarminterfaceFactory constructor. All rows found from Alarminterface
   * table are put into vector.
   */
  @Test
  public void testAlarminterfaceFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AlarminterfaceFactory(rockFactory, whereObject);

    /* Asserting all Alarminterfaces are found and put into vector */
    try {
      Vector<Alarminterface> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfaceid() + ", " +  actualVector.get(1).getInterfaceid() + ", " +  actualVector.get(2).getInterfaceid();
      String expected = "3, testINTERFACEID3, testINTERFACEID2, testINTERFACEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Alarminterfaces was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAlarminterfaceFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AlarminterfaceFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing AlarminterfaceFactory constructor. All rows found from Alarminterface
   * table are put into vector and data validation is on.
   */
  @Test
  public void testAlarminterfaceFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AlarminterfaceFactory(rockFactory, whereObject, true);

    /* Asserting all Alarminterfaces are found and put into vector */
    try {     
      Vector<Alarminterface> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testAlarminterfaceFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AlarminterfaceFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing AlarminterfaceFactory constructor. All rows found from Alarminterface
   * table are put into vector and data validation is on.
   */
  @Test
  public void testAlarminterfaceFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AlarminterfaceFactory(rockFactory, whereObject, "ORDER BY INTERFACEID");

    /* Asserting all Alarminterfaces are found and put into vector */
    try {
      Vector<Alarminterface> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfaceid() + ", " +  actualVector.get(1).getInterfaceid() + ", " +  actualVector.get(2).getInterfaceid();
      String expected = "3, testINTERFACEID1, testINTERFACEID2, testINTERFACEID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Alarminterfaces was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAlarminterfaceFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AlarminterfaceFactory(null, whereObject, "ORDER BY INTERFACEID");
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

    assertEquals( "testINTERFACEID2" , objUnderTest.getElementAt(1).getInterfaceid().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Alarminterface objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Alarminterface objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Alarminterface> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfaceid() + ", " +  actualVector.get(1).getInterfaceid() + ", " +  actualVector.get(2).getInterfaceid();
      String expected = "3, testINTERFACEID3, testINTERFACEID2, testINTERFACEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Alarminterface objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Alarminterface testObject = new Alarminterface(rockFactory, "testINTERFACEID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Alarminterface objects. True is returned if the two vectors
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
   * Test comparing two Alarminterface objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Alarminterface objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Alarminterface testObject = new Alarminterface(rockFactory, "testINTERFACEID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Alarminterface objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Alarminterface testObject = new Alarminterface(rockFactory, "testINTERFACEID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Alarminterface");
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
    assertEquals(AlarminterfaceFactory.class, clonedObject.getClass());
  }
}