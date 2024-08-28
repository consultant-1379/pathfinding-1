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
 * Test class for VerificationconditionFactory. Testing handling of all the objects in
 * Verificationcondition table.
 */
public class VerificationconditionFactoryTest {

  private static VerificationconditionFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Verificationcondition whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = VerificationconditionFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Verificationcondition ( VERSIONID VARCHAR(31)  ,FACTTABLE VARCHAR(31) ,VERLEVEL VARCHAR(31) ,CONDITIONCLASS VARCHAR(31) ,VERCONDITION VARCHAR(31) ,PROMPTNAME1 VARCHAR(31) ,PROMPTVALUE1 VARCHAR(31) ,PROMPTNAME2 VARCHAR(31) ,PROMPTVALUE2 VARCHAR(31) ,OBJECTCONDITION VARCHAR(31) ,PROMPTNAME3 VARCHAR(31) ,PROMPTVALUE3 VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Verificationcondition(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Verificationcondition");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Verificationcondition VALUES( 'testVERSIONID3'  ,'testFACTTABLE3'  ,'testVERLEVEL3'  ,'testCONDITIONCLASS3'  ,'testVERCONDITION3'  ,'testPROMPTNAME13'  ,'testPROMPTVALUE13'  ,'testPROMPTNAME23'  ,'testPROMPTVALUE23'  ,'testOBJECTCONDITION3'  ,'testPROMPTNAME33'  ,'testPROMPTVALUE33' )");
	  stmt.executeUpdate("INSERT INTO Verificationcondition VALUES( 'testVERSIONID2'  ,'testFACTTABLE2'  ,'testVERLEVEL2'  ,'testCONDITIONCLASS2'  ,'testVERCONDITION2'  ,'testPROMPTNAME12'  ,'testPROMPTVALUE12'  ,'testPROMPTNAME22'  ,'testPROMPTVALUE22'  ,'testOBJECTCONDITION2'  ,'testPROMPTNAME32'  ,'testPROMPTVALUE32' )");
	  stmt.executeUpdate("INSERT INTO Verificationcondition VALUES( 'testVERSIONID1'  ,'testFACTTABLE1'  ,'testVERLEVEL1'  ,'testCONDITIONCLASS1'  ,'testVERCONDITION1'  ,'testPROMPTNAME11'  ,'testPROMPTVALUE11'  ,'testPROMPTNAME21'  ,'testPROMPTVALUE21'  ,'testOBJECTCONDITION1'  ,'testPROMPTNAME31'  ,'testPROMPTVALUE31' )");

    /* Initializing tested object before each test */
    objUnderTest = new VerificationconditionFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Verificationcondition");
    objUnderTest = null;
  }
  
  /**
   * Testing VerificationconditionFactory constructor. All rows found from Verificationcondition
   * table are put into vector.
   */
  @Test
  public void testVerificationconditionFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new VerificationconditionFactory(rockFactory, whereObject);

    /* Asserting all Verificationconditions are found and put into vector */
    try {
      Vector<Verificationcondition> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Verificationconditions was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testVerificationconditionFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new VerificationconditionFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing VerificationconditionFactory constructor. All rows found from Verificationcondition
   * table are put into vector and data validation is on.
   */
  @Test
  public void testVerificationconditionFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new VerificationconditionFactory(rockFactory, whereObject, true);

    /* Asserting all Verificationconditions are found and put into vector */
    try {     
      Vector<Verificationcondition> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testVerificationconditionFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new VerificationconditionFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing VerificationconditionFactory constructor. All rows found from Verificationcondition
   * table are put into vector and data validation is on.
   */
  @Test
  public void testVerificationconditionFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new VerificationconditionFactory(rockFactory, whereObject, "ORDER BY VERSIONID");

    /* Asserting all Verificationconditions are found and put into vector */
    try {
      Vector<Verificationcondition> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID1, testVERSIONID2, testVERSIONID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Verificationconditions was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testVerificationconditionFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new VerificationconditionFactory(null, whereObject, "ORDER BY VERSIONID");
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
   * Testing size retrieving of the vector containing Verificationcondition objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Verificationcondition objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Verificationcondition> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Verificationcondition objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Verificationcondition testObject = new Verificationcondition(rockFactory, "testVERSIONID" + i, "testVERLEVEL" + i, "testCONDITIONCLASS" + i, "testVERCONDITION" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Verificationcondition objects. True is returned if the two vectors
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
   * Test comparing two Verificationcondition objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Verificationcondition objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Verificationcondition testObject = new Verificationcondition(rockFactory, "testVERSIONID1", "testVERLEVEL1", "testCONDITIONCLASS1", "testVERCONDITION1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Verificationcondition objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Verificationcondition testObject = new Verificationcondition(rockFactory, "testVERSIONID" + i, "testVERLEVEL" + i, "testCONDITIONCLASS" + i, "testVERCONDITION" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Verificationcondition");
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
    assertEquals(VerificationconditionFactory.class, clonedObject.getClass());
  }
}