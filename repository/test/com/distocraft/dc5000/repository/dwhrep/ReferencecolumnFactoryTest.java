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
 * Test class for ReferencecolumnFactory. Testing handling of all the objects in
 * Referencecolumn table.
 */
public class ReferencecolumnFactoryTest {

  private static ReferencecolumnFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Referencecolumn whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = ReferencecolumnFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Referencecolumn ( TYPEID VARCHAR(31)  ,DATANAME VARCHAR(31) ,COLNUMBER BIGINT  ,DATATYPE VARCHAR(31) ,DATASIZE INTEGER  ,DATASCALE INTEGER  ,UNIQUEVALUE BIGINT  ,NULLABLE INTEGER  ,INDEXES VARCHAR(31) ,UNIQUEKEY INTEGER  ,INCLUDESQL INTEGER  ,INCLUDEUPD INTEGER  ,COLTYPE VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,UNIVERSECLASS VARCHAR(31) ,UNIVERSEOBJECT VARCHAR(31) ,UNIVERSECONDITION VARCHAR(31) ,DATAID VARCHAR(31) ,BASEDEF INTEGER )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Referencecolumn(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Referencecolumn");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Referencecolumn VALUES( 'testTYPEID3'  ,'testDATANAME3'  ,3  ,'testDATATYPE3'  ,3  ,3  ,3  ,3  ,'testINDEXES3'  ,3  ,3  ,3  ,'testCOLTYPE3'  ,'testDESCRIPTION3'  ,'testUNIVERSECLASS3'  ,'testUNIVERSEOBJECT3'  ,'testUNIVERSECONDITION3'  ,'testDATAID3'  ,3 )");
	  stmt.executeUpdate("INSERT INTO Referencecolumn VALUES( 'testTYPEID2'  ,'testDATANAME2'  ,2  ,'testDATATYPE2'  ,2  ,2  ,2  ,2  ,'testINDEXES2'  ,2  ,2  ,2  ,'testCOLTYPE2'  ,'testDESCRIPTION2'  ,'testUNIVERSECLASS2'  ,'testUNIVERSEOBJECT2'  ,'testUNIVERSECONDITION2'  ,'testDATAID2'  ,2 )");
	  stmt.executeUpdate("INSERT INTO Referencecolumn VALUES( 'testTYPEID1'  ,'testDATANAME1'  ,1  ,'testDATATYPE1'  ,1  ,1  ,1  ,1  ,'testINDEXES1'  ,1  ,1  ,1  ,'testCOLTYPE1'  ,'testDESCRIPTION1'  ,'testUNIVERSECLASS1'  ,'testUNIVERSEOBJECT1'  ,'testUNIVERSECONDITION1'  ,'testDATAID1'  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new ReferencecolumnFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Referencecolumn");
    objUnderTest = null;
  }
  
  /**
   * Testing ReferencecolumnFactory constructor. All rows found from Referencecolumn
   * table are put into vector.
   */
  @Test
  public void testReferencecolumnFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new ReferencecolumnFactory(rockFactory, whereObject);

    /* Asserting all Referencecolumns are found and put into vector */
    try {
      Vector<Referencecolumn> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID3, testTYPEID2, testTYPEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Referencecolumns was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testReferencecolumnFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new ReferencecolumnFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing ReferencecolumnFactory constructor. All rows found from Referencecolumn
   * table are put into vector and data validation is on.
   */
  @Test
  public void testReferencecolumnFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new ReferencecolumnFactory(rockFactory, whereObject, true);

    /* Asserting all Referencecolumns are found and put into vector */
    try {     
      Vector<Referencecolumn> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testReferencecolumnFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new ReferencecolumnFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing ReferencecolumnFactory constructor. All rows found from Referencecolumn
   * table are put into vector and data validation is on.
   */
  @Test
  public void testReferencecolumnFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new ReferencecolumnFactory(rockFactory, whereObject, "ORDER BY TYPEID");

    /* Asserting all Referencecolumns are found and put into vector */
    try {
      Vector<Referencecolumn> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID1, testTYPEID2, testTYPEID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Referencecolumns was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testReferencecolumnFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new ReferencecolumnFactory(null, whereObject, "ORDER BY TYPEID");
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

    assertEquals( "testTYPEID2" , objUnderTest.getElementAt(1).getTypeid().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Referencecolumn objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Referencecolumn objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Referencecolumn> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID3, testTYPEID2, testTYPEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Referencecolumn objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Referencecolumn testObject = new Referencecolumn(rockFactory, "testTYPEID" + i, "testDATANAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Referencecolumn objects. True is returned if the two vectors
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
   * Test comparing two Referencecolumn objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Referencecolumn objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Referencecolumn testObject = new Referencecolumn(rockFactory, "testTYPEID1", "testDATANAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Referencecolumn objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Referencecolumn testObject = new Referencecolumn(rockFactory, "testTYPEID" + i, "testDATANAME" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Referencecolumn");
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
    assertEquals(ReferencecolumnFactory.class, clonedObject.getClass());
  }
}