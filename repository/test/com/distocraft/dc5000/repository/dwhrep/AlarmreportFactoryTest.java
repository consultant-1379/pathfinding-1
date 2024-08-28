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
 * Test class for AlarmreportFactory. Testing handling of all the objects in
 * Alarmreport table.
 */
public class AlarmreportFactoryTest {

  private static AlarmreportFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Alarmreport whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = AlarmreportFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Alarmreport ( INTERFACEID VARCHAR(31)  ,REPORTID VARCHAR(31) ,REPORTNAME VARCHAR(31) ,URL VARCHAR(31) ,STATUS VARCHAR(31) ,SIMULTANEOUS INTEGER )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Alarmreport(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Alarmreport");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Alarmreport VALUES( 'testINTERFACEID3'  ,'testREPORTID3'  ,'testREPORTNAME3'  ,'testURL3'  ,'testSTATUS3'  ,3 )");
	  stmt.executeUpdate("INSERT INTO Alarmreport VALUES( 'testINTERFACEID2'  ,'testREPORTID2'  ,'testREPORTNAME2'  ,'testURL2'  ,'testSTATUS2'  ,2 )");
	  stmt.executeUpdate("INSERT INTO Alarmreport VALUES( 'testINTERFACEID1'  ,'testREPORTID1'  ,'testREPORTNAME1'  ,'testURL1'  ,'testSTATUS1'  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new AlarmreportFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Alarmreport");
    objUnderTest = null;
  }
  
  /**
   * Testing AlarmreportFactory constructor. All rows found from Alarmreport
   * table are put into vector.
   */
  @Test
  public void testAlarmreportFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AlarmreportFactory(rockFactory, whereObject);

    /* Asserting all Alarmreports are found and put into vector */
    try {
      Vector<Alarmreport> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfaceid() + ", " +  actualVector.get(1).getInterfaceid() + ", " +  actualVector.get(2).getInterfaceid();
      String expected = "3, testINTERFACEID3, testINTERFACEID2, testINTERFACEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Alarmreports was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAlarmreportFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AlarmreportFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing AlarmreportFactory constructor. All rows found from Alarmreport
   * table are put into vector and data validation is on.
   */
  @Test
  public void testAlarmreportFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AlarmreportFactory(rockFactory, whereObject, true);

    /* Asserting all Alarmreports are found and put into vector */
    try {     
      Vector<Alarmreport> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testAlarmreportFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AlarmreportFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing AlarmreportFactory constructor. All rows found from Alarmreport
   * table are put into vector and data validation is on.
   */
  @Test
  public void testAlarmreportFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new AlarmreportFactory(rockFactory, whereObject, "ORDER BY INTERFACEID");

    /* Asserting all Alarmreports are found and put into vector */
    try {
      Vector<Alarmreport> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfaceid() + ", " +  actualVector.get(1).getInterfaceid() + ", " +  actualVector.get(2).getInterfaceid();
      String expected = "3, testINTERFACEID1, testINTERFACEID2, testINTERFACEID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Alarmreports was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAlarmreportFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new AlarmreportFactory(null, whereObject, "ORDER BY INTERFACEID");
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
   * Testing size retrieving of the vector containing Alarmreport objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Alarmreport objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Alarmreport> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfaceid() + ", " +  actualVector.get(1).getInterfaceid() + ", " +  actualVector.get(2).getInterfaceid();
      String expected = "3, testINTERFACEID3, testINTERFACEID2, testINTERFACEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Alarmreport objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Alarmreport testObject = new Alarmreport(rockFactory, "testREPORTID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Alarmreport objects. True is returned if the two vectors
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
   * Test comparing two Alarmreport objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Alarmreport objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Alarmreport testObject = new Alarmreport(rockFactory, "testREPORTID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Alarmreport objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Alarmreport testObject = new Alarmreport(rockFactory, "testREPORTID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Alarmreport");
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
    assertEquals(AlarmreportFactory.class, clonedObject.getClass());
  }
}