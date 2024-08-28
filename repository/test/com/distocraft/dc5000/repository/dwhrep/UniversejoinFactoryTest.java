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
 * Test class for UniversejoinFactory. Testing handling of all the objects in
 * Universejoin table.
 */
public class UniversejoinFactoryTest {

  private static UniversejoinFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Universejoin whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = UniversejoinFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Universejoin ( VERSIONID VARCHAR(31)  ,SOURCETABLE VARCHAR(31) ,SOURCELEVEL VARCHAR(31) ,SOURCECOLUMN VARCHAR(31) ,TARGETTABLE VARCHAR(31) ,TARGETLEVEL VARCHAR(31) ,TARGETCOLUMN VARCHAR(31) ,EXPRESSION VARCHAR(31) ,CARDINALITY VARCHAR(31) ,CONTEXT VARCHAR(31) ,EXCLUDEDCONTEXTS VARCHAR(31) ,TMPCOUNTER INTEGER  ,ORDERNRO BIGINT, UNIVERSEEXTENSION VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Universejoin(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Universejoin");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Universejoin VALUES( 'testVERSIONID3'  ,'testSOURCETABLE3'  ,'testSOURCELEVEL3'  ,'testSOURCECOLUMN3'  ,'testTARGETTABLE3'  ,'testTARGETLEVEL3'  ,'testTARGETCOLUMN3'  ,'testEXPRESSION3'  ,'testCARDINALITY3'  ,'testCONTEXT3'  ,'testEXCLUDEDCONTEXTS3'  ,3  ,3, '' )");
	  stmt.executeUpdate("INSERT INTO Universejoin VALUES( 'testVERSIONID2'  ,'testSOURCETABLE2'  ,'testSOURCELEVEL2'  ,'testSOURCECOLUMN2'  ,'testTARGETTABLE2'  ,'testTARGETLEVEL2'  ,'testTARGETCOLUMN2'  ,'testEXPRESSION2'  ,'testCARDINALITY2'  ,'testCONTEXT2'  ,'testEXCLUDEDCONTEXTS2'  ,2  ,2, '' )");
	  stmt.executeUpdate("INSERT INTO Universejoin VALUES( 'testVERSIONID1'  ,'testSOURCETABLE1'  ,'testSOURCELEVEL1'  ,'testSOURCECOLUMN1'  ,'testTARGETTABLE1'  ,'testTARGETLEVEL1'  ,'testTARGETCOLUMN1'  ,'testEXPRESSION1'  ,'testCARDINALITY1'  ,'testCONTEXT1'  ,'testEXCLUDEDCONTEXTS1'  ,1  ,1, '' )");

    /* Initializing tested object before each test */
    objUnderTest = new UniversejoinFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Universejoin");
    objUnderTest = null;
  }
  
  /**
   * Testing UniversejoinFactory constructor. All rows found from Universejoin
   * table are put into vector.
   */
  @Test
  public void testUniversejoinFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new UniversejoinFactory(rockFactory, whereObject);

    /* Asserting all Universejoins are found and put into vector */
    try {
      Vector<Universejoin> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Universejoins was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniversejoinFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new UniversejoinFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing UniversejoinFactory constructor. All rows found from Universejoin
   * table are put into vector and data validation is on.
   */
  @Test
  public void testUniversejoinFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new UniversejoinFactory(rockFactory, whereObject, true);

    /* Asserting all Universejoins are found and put into vector */
    try {     
      Vector<Universejoin> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testUniversejoinFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new UniversejoinFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing UniversejoinFactory constructor. All rows found from Universejoin
   * table are put into vector and data validation is on.
   */
  @Test
  public void testUniversejoinFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new UniversejoinFactory(rockFactory, whereObject, "ORDER BY VERSIONID");

    /* Asserting all Universejoins are found and put into vector */
    try {
      Vector<Universejoin> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID1, testVERSIONID2, testVERSIONID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Universejoins was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniversejoinFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new UniversejoinFactory(null, whereObject, "ORDER BY VERSIONID");
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
   * Testing size retrieving of the vector containing Universejoin objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Universejoin objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Universejoin> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Universejoin objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Universejoin testObject = new Universejoin(rockFactory, "testVERSIONID" + i, "testSOURCETABLE" + i, "testSOURCECOLUMN" + i, "testTARGETTABLE" + i, "testTARGETCOLUMN" + i, i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Universejoin objects. True is returned if the two vectors
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
   * Test comparing two Universejoin objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Universejoin objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Universejoin testObject = new Universejoin(rockFactory, "testVERSIONID1", "testSOURCETABLE1", "testSOURCECOLUMN1", "testTARGETTABLE1", "testTARGETCOLUMN1", 1);
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Universejoin objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Universejoin testObject = new Universejoin(rockFactory, "testVERSIONID" + i, "testSOURCETABLE" + i, "testSOURCECOLUMN" + i, "testTARGETTABLE" + i, "testTARGETCOLUMN" + i, i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universejoin");
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
    assertEquals(UniversejoinFactory.class, clonedObject.getClass());
  }
}