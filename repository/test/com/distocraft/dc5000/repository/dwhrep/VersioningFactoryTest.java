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
 * Test class for VersioningFactory. Testing handling of all the objects in
 * Versioning table.
 */
public class VersioningFactoryTest {

  private static VersioningFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Versioning whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = VersioningFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Versioning ( VERSIONID VARCHAR(31)  ,DESCRIPTION VARCHAR(31) ,STATUS BIGINT  ,TECHPACK_NAME VARCHAR(31) ,TECHPACK_VERSION VARCHAR(31) ,TECHPACK_TYPE VARCHAR(31) ,PRODUCT_NUMBER VARCHAR(31) ,LOCKEDBY VARCHAR(31) ,LOCKDATE TIMESTAMP  ,BASEDEFINITION VARCHAR(31) ,BASEVERSION VARCHAR(31) ,INSTALLDESCRIPTION VARCHAR(31) ,UNIVERSENAME VARCHAR(31) ,UNIVERSEEXTENSION VARCHAR(31) ,ENIQ_LEVEL VARCHAR(31) ,LICENSENAME VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Versioning(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Versioning");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Versioning VALUES( 'testVERSIONID3'  ,'testDESCRIPTION3'  ,3  ,'testTECHPACK_NAME3'  ,'testTECHPACK_VERSION3'  ,'testTECHPACK_TYPE3'  ,'testPRODUCT_NUMBER3'  ,'testLOCKEDBY3'  ,'2003-03-03 00:00:00.0'  ,'testBASEDEFINITION3'  ,'testBASEVERSION3'  ,'testINSTALLDESCRIPTION3'  ,'testUNIVERSENAME3'  ,'testUNIVERSEEXTENSION3'  ,'testENIQ_LEVEL3'  ,'testLICENSENAME3' )");
	  stmt.executeUpdate("INSERT INTO Versioning VALUES( 'testVERSIONID2'  ,'testDESCRIPTION2'  ,2  ,'testTECHPACK_NAME2'  ,'testTECHPACK_VERSION2'  ,'testTECHPACK_TYPE2'  ,'testPRODUCT_NUMBER2'  ,'testLOCKEDBY2'  ,'2002-02-02 00:00:00.0'  ,'testBASEDEFINITION2'  ,'testBASEVERSION2'  ,'testINSTALLDESCRIPTION2'  ,'testUNIVERSENAME2'  ,'testUNIVERSEEXTENSION2'  ,'testENIQ_LEVEL2'  ,'testLICENSENAME2' )");
	  stmt.executeUpdate("INSERT INTO Versioning VALUES( 'testVERSIONID1'  ,'testDESCRIPTION1'  ,1  ,'testTECHPACK_NAME1'  ,'testTECHPACK_VERSION1'  ,'testTECHPACK_TYPE1'  ,'testPRODUCT_NUMBER1'  ,'testLOCKEDBY1'  ,'2001-01-01 00:00:00.0'  ,'testBASEDEFINITION1'  ,'testBASEVERSION1'  ,'testINSTALLDESCRIPTION1'  ,'testUNIVERSENAME1'  ,'testUNIVERSEEXTENSION1'  ,'testENIQ_LEVEL1'  ,'testLICENSENAME1' )");

    /* Initializing tested object before each test */
    objUnderTest = new VersioningFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Versioning");
    objUnderTest = null;
  }
  
  /**
   * Testing VersioningFactory constructor. All rows found from Versioning
   * table are put into vector.
   */
  @Test
  public void testVersioningFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new VersioningFactory(rockFactory, whereObject);

    /* Asserting all Versionings are found and put into vector */
    try {
      Vector<Versioning> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Versionings was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testVersioningFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new VersioningFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing VersioningFactory constructor. All rows found from Versioning
   * table are put into vector and data validation is on.
   */
  @Test
  public void testVersioningFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new VersioningFactory(rockFactory, whereObject, true);

    /* Asserting all Versionings are found and put into vector */
    try {     
      Vector<Versioning> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testVersioningFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new VersioningFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing VersioningFactory constructor. All rows found from Versioning
   * table are put into vector and data validation is on.
   */
  @Test
  public void testVersioningFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new VersioningFactory(rockFactory, whereObject, "ORDER BY VERSIONID");

    /* Asserting all Versionings are found and put into vector */
    try {
      Vector<Versioning> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID1, testVERSIONID2, testVERSIONID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Versionings was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testVersioningFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new VersioningFactory(null, whereObject, "ORDER BY VERSIONID");
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
   * Testing size retrieving of the vector containing Versioning objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Versioning objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Versioning> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Versioning objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Versioning testObject = new Versioning(rockFactory, "testVERSIONID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Versioning objects. True is returned if the two vectors
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
   * Test comparing two Versioning objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Versioning objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Versioning testObject = new Versioning(rockFactory, "testVERSIONID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Versioning objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Versioning testObject = new Versioning(rockFactory, "testVERSIONID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Versioning");
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
    assertEquals(VersioningFactory.class, clonedObject.getClass());
  }
}