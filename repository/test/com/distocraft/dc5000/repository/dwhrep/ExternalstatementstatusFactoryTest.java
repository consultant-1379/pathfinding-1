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
 * Test class for ExternalstatementstatusFactory. Testing handling of all the objects in
 * Externalstatementstatus table.
 */
public class ExternalstatementstatusFactoryTest {

  private static ExternalstatementstatusFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Externalstatementstatus whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = ExternalstatementstatusFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Externalstatementstatus ( TECHPACK_NAME VARCHAR(31)  ,STATEMENTNAME VARCHAR(31) ,VERSIONID VARCHAR(31) ,STATUS VARCHAR(31) ,EXECTIME TIMESTAMP  ,EXECSTATEMENT VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Externalstatementstatus(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Externalstatementstatus");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Externalstatementstatus VALUES( 'testTECHPACK_NAME3'  ,'testSTATEMENTNAME3'  ,'testVERSIONID3'  ,'testSTATUS3'  ,'2003-03-03 00:00:00.0'  ,'testEXECSTATEMENT3' )");
	  stmt.executeUpdate("INSERT INTO Externalstatementstatus VALUES( 'testTECHPACK_NAME2'  ,'testSTATEMENTNAME2'  ,'testVERSIONID2'  ,'testSTATUS2'  ,'2002-02-02 00:00:00.0'  ,'testEXECSTATEMENT2' )");
	  stmt.executeUpdate("INSERT INTO Externalstatementstatus VALUES( 'testTECHPACK_NAME1'  ,'testSTATEMENTNAME1'  ,'testVERSIONID1'  ,'testSTATUS1'  ,'2001-01-01 00:00:00.0'  ,'testEXECSTATEMENT1' )");

    /* Initializing tested object before each test */
    objUnderTest = new ExternalstatementstatusFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Externalstatementstatus");
    objUnderTest = null;
  }
  
  /**
   * Testing ExternalstatementstatusFactory constructor. All rows found from Externalstatementstatus
   * table are put into vector.
   */
  @Test
  public void testExternalstatementstatusFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new ExternalstatementstatusFactory(rockFactory, whereObject);

    /* Asserting all Externalstatementstatuss are found and put into vector */
    try {
      Vector<Externalstatementstatus> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTechpack_name() + ", " +  actualVector.get(1).getTechpack_name() + ", " +  actualVector.get(2).getTechpack_name();
      String expected = "3, testTECHPACK_NAME3, testTECHPACK_NAME2, testTECHPACK_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Externalstatementstatuss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testExternalstatementstatusFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new ExternalstatementstatusFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing ExternalstatementstatusFactory constructor. All rows found from Externalstatementstatus
   * table are put into vector and data validation is on.
   */
  @Test
  public void testExternalstatementstatusFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new ExternalstatementstatusFactory(rockFactory, whereObject, true);

    /* Asserting all Externalstatementstatuss are found and put into vector */
    try {     
      Vector<Externalstatementstatus> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testExternalstatementstatusFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new ExternalstatementstatusFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing ExternalstatementstatusFactory constructor. All rows found from Externalstatementstatus
   * table are put into vector and data validation is on.
   */
  @Test
  public void testExternalstatementstatusFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new ExternalstatementstatusFactory(rockFactory, whereObject, "ORDER BY TECHPACK_NAME");

    /* Asserting all Externalstatementstatuss are found and put into vector */
    try {
      Vector<Externalstatementstatus> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTechpack_name() + ", " +  actualVector.get(1).getTechpack_name() + ", " +  actualVector.get(2).getTechpack_name();
      String expected = "3, testTECHPACK_NAME1, testTECHPACK_NAME2, testTECHPACK_NAME3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Externalstatementstatuss was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testExternalstatementstatusFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new ExternalstatementstatusFactory(null, whereObject, "ORDER BY TECHPACK_NAME");
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

    assertEquals( "testTECHPACK_NAME2" , objUnderTest.getElementAt(1).getTechpack_name().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Externalstatementstatus objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Externalstatementstatus objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Externalstatementstatus> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getTechpack_name() + ", " +  actualVector.get(1).getTechpack_name() + ", " +  actualVector.get(2).getTechpack_name();
      String expected = "3, testTECHPACK_NAME3, testTECHPACK_NAME2, testTECHPACK_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Externalstatementstatus objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Externalstatementstatus testObject = new Externalstatementstatus(rockFactory, "testTECHPACK_NAME" + i, "testSTATEMENTNAME" + i, "testVERSIONID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Externalstatementstatus objects. True is returned if the two vectors
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
   * Test comparing two Externalstatementstatus objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Externalstatementstatus objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Externalstatementstatus testObject = new Externalstatementstatus(rockFactory, "testTECHPACK_NAME1", "testSTATEMENTNAME1", "testVERSIONID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Externalstatementstatus objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Externalstatementstatus testObject = new Externalstatementstatus(rockFactory, "testTECHPACK_NAME" + i, "testSTATEMENTNAME" + i, "testVERSIONID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Externalstatementstatus");
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
    assertEquals(ExternalstatementstatusFactory.class, clonedObject.getClass());
  }
}