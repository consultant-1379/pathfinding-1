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
 * Test class for UniverseobjectFactory. Testing handling of all the objects in
 * Universeobject table.
 */
public class UniverseobjectFactoryTest {

  private static UniverseobjectFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Universeobject whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = UniverseobjectFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Universeobject ( VERSIONID VARCHAR(31)  ,CLASSNAME VARCHAR(31) ,UNIVERSEEXTENSION VARCHAR(31) ,OBJECTNAME VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,OBJECTTYPE VARCHAR(31) ,QUALIFICATION VARCHAR(31) ,AGGREGATION VARCHAR(31) ,OBJSELECT VARCHAR(31) ,OBJWHERE VARCHAR(31) ,PROMPTHIERARCHY VARCHAR(31) ,OBJ_BH_REL INTEGER  ,ELEM_BH_REL INTEGER  ,INHERITANCE INTEGER  ,ORDERNRO BIGINT )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Universeobject(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Universeobject");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Universeobject VALUES( 'testVERSIONID3'  ,'testCLASSNAME3'  ,'testUNIVERSEEXTENSION3'  ,'testOBJECTNAME3'  ,'testDESCRIPTION3'  ,'testOBJECTTYPE3'  ,'testQUALIFICATION3'  ,'testAGGREGATION3'  ,'testOBJSELECT3'  ,'testOBJWHERE3'  ,'testPROMPTHIERARCHY3'  ,3  ,3  ,3  ,3 )");
	  stmt.executeUpdate("INSERT INTO Universeobject VALUES( 'testVERSIONID2'  ,'testCLASSNAME2'  ,'testUNIVERSEEXTENSION2'  ,'testOBJECTNAME2'  ,'testDESCRIPTION2'  ,'testOBJECTTYPE2'  ,'testQUALIFICATION2'  ,'testAGGREGATION2'  ,'testOBJSELECT2'  ,'testOBJWHERE2'  ,'testPROMPTHIERARCHY2'  ,2  ,2  ,2  ,2 )");
	  stmt.executeUpdate("INSERT INTO Universeobject VALUES( 'testVERSIONID1'  ,'testCLASSNAME1'  ,'testUNIVERSEEXTENSION1'  ,'testOBJECTNAME1'  ,'testDESCRIPTION1'  ,'testOBJECTTYPE1'  ,'testQUALIFICATION1'  ,'testAGGREGATION1'  ,'testOBJSELECT1'  ,'testOBJWHERE1'  ,'testPROMPTHIERARCHY1'  ,1  ,1  ,1  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new UniverseobjectFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Universeobject");
    objUnderTest = null;
  }
  
  /**
   * Testing UniverseobjectFactory constructor. All rows found from Universeobject
   * table are put into vector.
   */
  @Test
  public void testUniverseobjectFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new UniverseobjectFactory(rockFactory, whereObject);

    /* Asserting all Universeobjects are found and put into vector */
    try {
      Vector<Universeobject> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Universeobjects was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniverseobjectFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new UniverseobjectFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing UniverseobjectFactory constructor. All rows found from Universeobject
   * table are put into vector and data validation is on.
   */
  @Test
  public void testUniverseobjectFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new UniverseobjectFactory(rockFactory, whereObject, true);

    /* Asserting all Universeobjects are found and put into vector */
    try {     
      Vector<Universeobject> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testUniverseobjectFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new UniverseobjectFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing UniverseobjectFactory constructor. All rows found from Universeobject
   * table are put into vector and data validation is on.
   */
  @Test
  public void testUniverseobjectFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new UniverseobjectFactory(rockFactory, whereObject, "ORDER BY VERSIONID");

    /* Asserting all Universeobjects are found and put into vector */
    try {
      Vector<Universeobject> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID1, testVERSIONID2, testVERSIONID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Universeobjects was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniverseobjectFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new UniverseobjectFactory(null, whereObject, "ORDER BY VERSIONID");
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
   * Testing size retrieving of the vector containing Universeobject objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Universeobject objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Universeobject> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Universeobject objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Universeobject testObject = new Universeobject(rockFactory, "testVERSIONID" + i, "testCLASSNAME" + i, "testUNIVERSEEXTENSION" + i, "testOBJECTNAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Universeobject objects. True is returned if the two vectors
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
   * Test comparing two Universeobject objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Universeobject objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Universeobject testObject = new Universeobject(rockFactory, "testVERSIONID1", "testCLASSNAME1", "testUNIVERSEEXTENSION1", "testOBJECTNAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Universeobject objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Universeobject testObject = new Universeobject(rockFactory, "testVERSIONID" + i, "testCLASSNAME" + i, "testUNIVERSEEXTENSION" + i, "testOBJECTNAME" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universeobject");
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
    assertEquals(UniverseobjectFactory.class, clonedObject.getClass());
  }
}