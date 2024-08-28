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
 * Test class for DatainterfaceFactory. Testing handling of all the objects in
 * Datainterface table.
 */
public class DatainterfaceFactoryTest {

  private static DatainterfaceFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Datainterface whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = DatainterfaceFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Datainterface ( INTERFACENAME VARCHAR(31)  ,STATUS BIGINT  ,INTERFACETYPE VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,DATAFORMATTYPE VARCHAR(31) ,INTERFACEVERSION VARCHAR(31) ,LOCKEDBY VARCHAR(31) ,LOCKDATE TIMESTAMP  ,PRODUCTNUMBER VARCHAR(31) ,ENIQ_LEVEL VARCHAR(31) ,RSTATE VARCHAR(31) ,INSTALLDESCRIPTION VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Datainterface(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Datainterface");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Datainterface VALUES( 'testINTERFACENAME3'  ,3  ,'testINTERFACETYPE3'  ,'testDESCRIPTION3'  ,'testDATAFORMATTYPE3'  ,'testINTERFACEVERSION3'  ,'testLOCKEDBY3'  ,'2003-03-03 00:00:00.0'  ,'testPRODUCTNUMBER3'  ,'testENIQ_LEVEL3'  ,'testRSTATE3'  ,'testINSTALLDESCRIPTION3' )");
	  stmt.executeUpdate("INSERT INTO Datainterface VALUES( 'testINTERFACENAME2'  ,2  ,'testINTERFACETYPE2'  ,'testDESCRIPTION2'  ,'testDATAFORMATTYPE2'  ,'testINTERFACEVERSION2'  ,'testLOCKEDBY2'  ,'2002-02-02 00:00:00.0'  ,'testPRODUCTNUMBER2'  ,'testENIQ_LEVEL2'  ,'testRSTATE2'  ,'testINSTALLDESCRIPTION2' )");
	  stmt.executeUpdate("INSERT INTO Datainterface VALUES( 'testINTERFACENAME1'  ,1  ,'testINTERFACETYPE1'  ,'testDESCRIPTION1'  ,'testDATAFORMATTYPE1'  ,'testINTERFACEVERSION1'  ,'testLOCKEDBY1'  ,'2001-01-01 00:00:00.0'  ,'testPRODUCTNUMBER1'  ,'testENIQ_LEVEL1'  ,'testRSTATE1'  ,'testINSTALLDESCRIPTION1' )");

    /* Initializing tested object before each test */
    objUnderTest = new DatainterfaceFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Datainterface");
    objUnderTest = null;
  }
  
  /**
   * Testing DatainterfaceFactory constructor. All rows found from Datainterface
   * table are put into vector.
   */
  @Test
  public void testDatainterfaceFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DatainterfaceFactory(rockFactory, whereObject);

    /* Asserting all Datainterfaces are found and put into vector */
    try {
      Vector<Datainterface> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfacename() + ", " +  actualVector.get(1).getInterfacename() + ", " +  actualVector.get(2).getInterfacename();
      String expected = "3, testINTERFACENAME3, testINTERFACENAME2, testINTERFACENAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Datainterfaces was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDatainterfaceFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DatainterfaceFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing DatainterfaceFactory constructor. All rows found from Datainterface
   * table are put into vector and data validation is on.
   */
  @Test
  public void testDatainterfaceFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DatainterfaceFactory(rockFactory, whereObject, true);

    /* Asserting all Datainterfaces are found and put into vector */
    try {     
      Vector<Datainterface> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testDatainterfaceFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DatainterfaceFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing DatainterfaceFactory constructor. All rows found from Datainterface
   * table are put into vector and data validation is on.
   */
  @Test
  public void testDatainterfaceFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DatainterfaceFactory(rockFactory, whereObject, "ORDER BY INTERFACENAME");

    /* Asserting all Datainterfaces are found and put into vector */
    try {
      Vector<Datainterface> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfacename() + ", " +  actualVector.get(1).getInterfacename() + ", " +  actualVector.get(2).getInterfacename();
      String expected = "3, testINTERFACENAME1, testINTERFACENAME2, testINTERFACENAME3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Datainterfaces was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDatainterfaceFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DatainterfaceFactory(null, whereObject, "ORDER BY INTERFACENAME");
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

    assertEquals( "testINTERFACENAME2" , objUnderTest.getElementAt(1).getInterfacename().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Datainterface objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Datainterface objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Datainterface> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfacename() + ", " +  actualVector.get(1).getInterfacename() + ", " +  actualVector.get(2).getInterfacename();
      String expected = "3, testINTERFACENAME3, testINTERFACENAME2, testINTERFACENAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Datainterface objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Datainterface testObject = new Datainterface(rockFactory, "testINTERFACENAME" + i, "testINTERFACEVERSION" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Datainterface objects. True is returned if the two vectors
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
   * Test comparing two Datainterface objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Datainterface objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Datainterface testObject = new Datainterface(rockFactory, "testINTERFACENAME1", "testINTERFACEVERSION1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Datainterface objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Datainterface testObject = new Datainterface(rockFactory, "testINTERFACENAME" + i, "testINTERFACEVERSION" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Datainterface");
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
    assertEquals(DatainterfaceFactory.class, clonedObject.getClass());
  }
}