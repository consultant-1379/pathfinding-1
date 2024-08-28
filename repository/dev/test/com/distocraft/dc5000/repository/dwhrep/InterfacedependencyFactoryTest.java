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
 * Test class for InterfacedependencyFactory. Testing handling of all the objects in
 * Interfacedependency table.
 */
public class InterfacedependencyFactoryTest {

  private static InterfacedependencyFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Interfacedependency whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = InterfacedependencyFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Interfacedependency ( INTERFACEVERSION VARCHAR(31)  ,INTERFACENAME VARCHAR(31) ,TECHPACKNAME VARCHAR(31) ,TECHPACKVERSION VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Interfacedependency(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Interfacedependency");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Interfacedependency VALUES( 'testINTERFACEVERSION3'  ,'testINTERFACENAME3'  ,'testTECHPACKNAME3'  ,'testTECHPACKVERSION3' )");
	  stmt.executeUpdate("INSERT INTO Interfacedependency VALUES( 'testINTERFACEVERSION2'  ,'testINTERFACENAME2'  ,'testTECHPACKNAME2'  ,'testTECHPACKVERSION2' )");
	  stmt.executeUpdate("INSERT INTO Interfacedependency VALUES( 'testINTERFACEVERSION1'  ,'testINTERFACENAME1'  ,'testTECHPACKNAME1'  ,'testTECHPACKVERSION1' )");

    /* Initializing tested object before each test */
    objUnderTest = new InterfacedependencyFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Interfacedependency");
    objUnderTest = null;
  }
  
  /**
   * Testing InterfacedependencyFactory constructor. All rows found from Interfacedependency
   * table are put into vector.
   */
  @Test
  public void testInterfacedependencyFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new InterfacedependencyFactory(rockFactory, whereObject);

    /* Asserting all Interfacedependencys are found and put into vector */
    try {
      Vector<Interfacedependency> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfaceversion() + ", " +  actualVector.get(1).getInterfaceversion() + ", " +  actualVector.get(2).getInterfaceversion();
      String expected = "3, testINTERFACEVERSION3, testINTERFACEVERSION2, testINTERFACEVERSION1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Interfacedependencys was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testInterfacedependencyFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new InterfacedependencyFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing InterfacedependencyFactory constructor. All rows found from Interfacedependency
   * table are put into vector and data validation is on.
   */
  @Test
  public void testInterfacedependencyFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new InterfacedependencyFactory(rockFactory, whereObject, true);

    /* Asserting all Interfacedependencys are found and put into vector */
    try {     
      Vector<Interfacedependency> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testInterfacedependencyFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new InterfacedependencyFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing InterfacedependencyFactory constructor. All rows found from Interfacedependency
   * table are put into vector and data validation is on.
   */
  @Test
  public void testInterfacedependencyFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new InterfacedependencyFactory(rockFactory, whereObject, "ORDER BY INTERFACEVERSION");

    /* Asserting all Interfacedependencys are found and put into vector */
    try {
      Vector<Interfacedependency> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfaceversion() + ", " +  actualVector.get(1).getInterfaceversion() + ", " +  actualVector.get(2).getInterfaceversion();
      String expected = "3, testINTERFACEVERSION1, testINTERFACEVERSION2, testINTERFACEVERSION3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Interfacedependencys was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testInterfacedependencyFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new InterfacedependencyFactory(null, whereObject, "ORDER BY INTERFACEVERSION");
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

    assertEquals( "testINTERFACEVERSION2" , objUnderTest.getElementAt(1).getInterfaceversion().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Interfacedependency objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Interfacedependency objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Interfacedependency> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getInterfaceversion() + ", " +  actualVector.get(1).getInterfaceversion() + ", " +  actualVector.get(2).getInterfaceversion();
      String expected = "3, testINTERFACEVERSION3, testINTERFACEVERSION2, testINTERFACEVERSION1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Interfacedependency objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Interfacedependency testObject = new Interfacedependency(rockFactory, "testINTERFACEVERSION" + i, "testINTERFACENAME" + i, "testTECHPACKNAME" + i, "testTECHPACKVERSION" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Interfacedependency objects. True is returned if the two vectors
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
   * Test comparing two Interfacedependency objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Interfacedependency objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Interfacedependency testObject = new Interfacedependency(rockFactory, "testINTERFACEVERSION1", "testINTERFACENAME1", "testTECHPACKNAME1", "testTECHPACKVERSION1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Interfacedependency objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Interfacedependency testObject = new Interfacedependency(rockFactory, "testINTERFACEVERSION" + i, "testINTERFACENAME" + i, "testTECHPACKNAME" + i, "testTECHPACKVERSION" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Interfacedependency");
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
    assertEquals(InterfacedependencyFactory.class, clonedObject.getClass());
  }
}