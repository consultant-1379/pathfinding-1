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
 * Test class for InterfacetechpacksFactory. Testing handling of all the objects in
 * Interfacetechpacks table.
 */
public class InterfacetechpacksFactoryTest {

  private static InterfacetechpacksFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Interfacetechpacks whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = InterfacetechpacksFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Interfacetechpacks ( INTERFACENAME VARCHAR(31)  ,TECHPACKNAME VARCHAR(31) ,TECHPACKVERSION VARCHAR(31) ,INTERFACEVERSION VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Interfacetechpacks(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Interfacetechpacks");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Interfacetechpacks VALUES( 'testINTERFACENAME3'  ,'testTECHPACKNAME3'  ,'testTECHPACKVERSION3'  ,'testINTERFACEVERSION3' )");
	  stmt.executeUpdate("INSERT INTO Interfacetechpacks VALUES( 'testINTERFACENAME2'  ,'testTECHPACKNAME2'  ,'testTECHPACKVERSION2'  ,'testINTERFACEVERSION2' )");
	  stmt.executeUpdate("INSERT INTO Interfacetechpacks VALUES( 'testINTERFACENAME1'  ,'testTECHPACKNAME1'  ,'testTECHPACKVERSION1'  ,'testINTERFACEVERSION1' )");

    /* Initializing tested object before each test */
    objUnderTest = new InterfacetechpacksFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Interfacetechpacks");
    objUnderTest = null;
  }
  
  /**
   * Testing InterfacetechpacksFactory constructor. All rows found from Interfacetechpacks
   * table are put into vector.
   */
  @Test
  public void testInterfacetechpacksFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new InterfacetechpacksFactory(rockFactory, whereObject);

    /* Asserting all Interfacetechpackss are found and put into vector */
    try {
      Vector<Interfacetechpacks> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfacename() + ", " +  actualVector.get(1).getInterfacename() + ", " +  actualVector.get(2).getInterfacename();
      String expected = "3, testINTERFACENAME3, testINTERFACENAME2, testINTERFACENAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Interfacetechpackss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testInterfacetechpacksFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new InterfacetechpacksFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing InterfacetechpacksFactory constructor. All rows found from Interfacetechpacks
   * table are put into vector and data validation is on.
   */
  @Test
  public void testInterfacetechpacksFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new InterfacetechpacksFactory(rockFactory, whereObject, true);

    /* Asserting all Interfacetechpackss are found and put into vector */
    try {     
      Vector<Interfacetechpacks> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testInterfacetechpacksFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new InterfacetechpacksFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing InterfacetechpacksFactory constructor. All rows found from Interfacetechpacks
   * table are put into vector and data validation is on.
   */
  @Test
  public void testInterfacetechpacksFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new InterfacetechpacksFactory(rockFactory, whereObject, "ORDER BY INTERFACENAME");

    /* Asserting all Interfacetechpackss are found and put into vector */
    try {
      Vector<Interfacetechpacks> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfacename() + ", " +  actualVector.get(1).getInterfacename() + ", " +  actualVector.get(2).getInterfacename();
      String expected = "3, testINTERFACENAME1, testINTERFACENAME2, testINTERFACENAME3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Interfacetechpackss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testInterfacetechpacksFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new InterfacetechpacksFactory(null, whereObject, "ORDER BY INTERFACENAME");
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
   * Testing size retrieving of the vector containing Interfacetechpacks objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Interfacetechpacks objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Interfacetechpacks> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfacename() + ", " +  actualVector.get(1).getInterfacename() + ", " +  actualVector.get(2).getInterfacename();
      String expected = "3, testINTERFACENAME3, testINTERFACENAME2, testINTERFACENAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Interfacetechpacks objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Interfacetechpacks testObject = new Interfacetechpacks(rockFactory, "testINTERFACENAME" + i, "testTECHPACKNAME" + i, "testTECHPACKVERSION" + i, "testINTERFACEVERSION" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Interfacetechpacks objects. True is returned if the two vectors
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
   * Test comparing two Interfacetechpacks objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Interfacetechpacks objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Interfacetechpacks testObject = new Interfacetechpacks(rockFactory, "testINTERFACENAME1", "testTECHPACKNAME1", "testTECHPACKVERSION1", "testINTERFACEVERSION1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Interfacetechpacks objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Interfacetechpacks testObject = new Interfacetechpacks(rockFactory, "testINTERFACENAME" + i, "testTECHPACKNAME" + i, "testTECHPACKVERSION" + i, "testINTERFACEVERSION" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Interfacetechpacks");
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
    assertEquals(InterfacetechpacksFactory.class, clonedObject.getClass());
  }
}