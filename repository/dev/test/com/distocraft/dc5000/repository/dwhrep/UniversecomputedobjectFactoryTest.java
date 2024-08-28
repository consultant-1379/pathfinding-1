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
 * Test class for UniversecomputedobjectFactory. Testing handling of all the objects in
 * Universecomputedobject table.
 */
public class UniversecomputedobjectFactoryTest {

  private static UniversecomputedobjectFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Universecomputedobject whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = UniversecomputedobjectFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Universecomputedobject ( VERSIONID VARCHAR(31)  ,CLASSNAME VARCHAR(31) ,UNIVERSEEXTENSION VARCHAR(31) ,OBJECTNAME VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,OBJECTTYPE VARCHAR(31) ,QUALIFICATION VARCHAR(31) ,AGGREGATION VARCHAR(31) ,OBJSELECT VARCHAR(31) ,OBJWHERE VARCHAR(31) ,PROMPTHIERARCHY VARCHAR(31) ,OBJ_BH_REL INTEGER  ,ELEM_BH_REL INTEGER  ,INHERITANCE INTEGER  ,ORDERNRO BIGINT )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Universecomputedobject(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Universecomputedobject");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Universecomputedobject VALUES( 'testVERSIONID3'  ,'testCLASSNAME3'  ,'testUNIVERSEEXTENSION3'  ,'testOBJECTNAME3'  ,'testDESCRIPTION3'  ,'testOBJECTTYPE3'  ,'testQUALIFICATION3'  ,'testAGGREGATION3'  ,'testOBJSELECT3'  ,'testOBJWHERE3'  ,'testPROMPTHIERARCHY3'  ,3  ,3  ,3  ,3 )");
	  stmt.executeUpdate("INSERT INTO Universecomputedobject VALUES( 'testVERSIONID2'  ,'testCLASSNAME2'  ,'testUNIVERSEEXTENSION2'  ,'testOBJECTNAME2'  ,'testDESCRIPTION2'  ,'testOBJECTTYPE2'  ,'testQUALIFICATION2'  ,'testAGGREGATION2'  ,'testOBJSELECT2'  ,'testOBJWHERE2'  ,'testPROMPTHIERARCHY2'  ,2  ,2  ,2  ,2 )");
	  stmt.executeUpdate("INSERT INTO Universecomputedobject VALUES( 'testVERSIONID1'  ,'testCLASSNAME1'  ,'testUNIVERSEEXTENSION1'  ,'testOBJECTNAME1'  ,'testDESCRIPTION1'  ,'testOBJECTTYPE1'  ,'testQUALIFICATION1'  ,'testAGGREGATION1'  ,'testOBJSELECT1'  ,'testOBJWHERE1'  ,'testPROMPTHIERARCHY1'  ,1  ,1  ,1  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new UniversecomputedobjectFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Universecomputedobject");
    objUnderTest = null;
  }
  
  /**
   * Testing UniversecomputedobjectFactory constructor. All rows found from Universecomputedobject
   * table are put into vector.
   */
  @Test
  public void testUniversecomputedobjectFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new UniversecomputedobjectFactory(rockFactory, whereObject);

    /* Asserting all Universecomputedobjects are found and put into vector */
    try {
      Vector<Universecomputedobject> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Universecomputedobjects was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniversecomputedobjectFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new UniversecomputedobjectFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing UniversecomputedobjectFactory constructor. All rows found from Universecomputedobject
   * table are put into vector and data validation is on.
   */
  @Test
  public void testUniversecomputedobjectFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new UniversecomputedobjectFactory(rockFactory, whereObject, true);

    /* Asserting all Universecomputedobjects are found and put into vector */
    try {     
      Vector<Universecomputedobject> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testUniversecomputedobjectFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new UniversecomputedobjectFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing UniversecomputedobjectFactory constructor. All rows found from Universecomputedobject
   * table are put into vector and data validation is on.
   */
  @Test
  public void testUniversecomputedobjectFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new UniversecomputedobjectFactory(rockFactory, whereObject, "ORDER BY VERSIONID");

    /* Asserting all Universecomputedobjects are found and put into vector */
    try {
      Vector<Universecomputedobject> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID1, testVERSIONID2, testVERSIONID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Universecomputedobjects was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniversecomputedobjectFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new UniversecomputedobjectFactory(null, whereObject, "ORDER BY VERSIONID");
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

    assertEquals( "testVERSIONID2" , objUnderTest.getElementAt(1).getVersionid().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Universecomputedobject objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Universecomputedobject objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Universecomputedobject> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Universecomputedobject objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Universecomputedobject testObject = new Universecomputedobject(rockFactory, "testVERSIONID" + i, "testCLASSNAME" + i, "testUNIVERSEEXTENSION" + i, "testOBJECTNAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Universecomputedobject objects. True is returned if the two vectors
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
   * Test comparing two Universecomputedobject objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Universecomputedobject objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Universecomputedobject testObject = new Universecomputedobject(rockFactory, "testVERSIONID1", "testCLASSNAME1", "testUNIVERSEEXTENSION1", "testOBJECTNAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Universecomputedobject objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Universecomputedobject testObject = new Universecomputedobject(rockFactory, "testVERSIONID" + i, "testCLASSNAME" + i, "testUNIVERSEEXTENSION" + i, "testOBJECTNAME" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universecomputedobject");
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
    assertEquals(UniversecomputedobjectFactory.class, clonedObject.getClass());
  }
}