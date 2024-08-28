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
 * Test class for MeasurementcolumnFactory. Testing handling of all the objects in
 * Measurementcolumn table.
 */
public class MeasurementcolumnFactoryTest {

  private static MeasurementcolumnFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Measurementcolumn whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = MeasurementcolumnFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Measurementcolumn ( MTABLEID VARCHAR(31)  ,DATANAME VARCHAR(31) ,COLNUMBER BIGINT  ,DATATYPE VARCHAR(31) ,DATASIZE INTEGER  ,DATASCALE INTEGER  ,UNIQUEVALUE BIGINT  ,NULLABLE INTEGER  ,INDEXES VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,DATAID VARCHAR(31) ,RELEASEID VARCHAR(31) ,UNIQUEKEY INTEGER  ,INCLUDESQL INTEGER  ,COLTYPE VARCHAR(31) ,FOLLOWJOHN INTEGER )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Measurementcolumn(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Measurementcolumn");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Measurementcolumn VALUES( 'testMTABLEID3'  ,'testDATANAME3'  ,3  ,'testDATATYPE3'  ,3  ,3  ,3  ,3  ,'testINDEXES3'  ,'testDESCRIPTION3'  ,'testDATAID3'  ,'testRELEASEID3'  ,3  ,3  ,'testCOLTYPE3'  ,3 )");
	  stmt.executeUpdate("INSERT INTO Measurementcolumn VALUES( 'testMTABLEID2'  ,'testDATANAME2'  ,2  ,'testDATATYPE2'  ,2  ,2  ,2  ,2  ,'testINDEXES2'  ,'testDESCRIPTION2'  ,'testDATAID2'  ,'testRELEASEID2'  ,2  ,2  ,'testCOLTYPE2'  ,2 )");
	  stmt.executeUpdate("INSERT INTO Measurementcolumn VALUES( 'testMTABLEID1'  ,'testDATANAME1'  ,1  ,'testDATATYPE1'  ,1  ,1  ,1  ,1  ,'testINDEXES1'  ,'testDESCRIPTION1'  ,'testDATAID1'  ,'testRELEASEID1'  ,1  ,1  ,'testCOLTYPE1'  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new MeasurementcolumnFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Measurementcolumn");
    objUnderTest = null;
  }
  
  /**
   * Testing MeasurementcolumnFactory constructor. All rows found from Measurementcolumn
   * table are put into vector.
   */
  @Test
  public void testMeasurementcolumnFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementcolumnFactory(rockFactory, whereObject);

    /* Asserting all Measurementcolumns are found and put into vector */
    try {
      Vector<Measurementcolumn> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getMtableid() + ", " +  actualVector.get(1).getMtableid() + ", " +  actualVector.get(2).getMtableid();
      String expected = "3, testMTABLEID3, testMTABLEID2, testMTABLEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Measurementcolumns was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementcolumnFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementcolumnFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing MeasurementcolumnFactory constructor. All rows found from Measurementcolumn
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeasurementcolumnFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementcolumnFactory(rockFactory, whereObject, true);

    /* Asserting all Measurementcolumns are found and put into vector */
    try {     
      Vector<Measurementcolumn> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeasurementcolumnFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementcolumnFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing MeasurementcolumnFactory constructor. All rows found from Measurementcolumn
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeasurementcolumnFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementcolumnFactory(rockFactory, whereObject, "ORDER BY MTABLEID");

    /* Asserting all Measurementcolumns are found and put into vector */
    try {
      Vector<Measurementcolumn> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getMtableid() + ", " +  actualVector.get(1).getMtableid() + ", " +  actualVector.get(2).getMtableid();
      String expected = "3, testMTABLEID1, testMTABLEID2, testMTABLEID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Measurementcolumns was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementcolumnFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementcolumnFactory(null, whereObject, "ORDER BY MTABLEID");
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

    assertEquals( "testMTABLEID2" , objUnderTest.getElementAt(1).getMtableid().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Measurementcolumn objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Measurementcolumn objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Measurementcolumn> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getMtableid() + ", " +  actualVector.get(1).getMtableid() + ", " +  actualVector.get(2).getMtableid();
      String expected = "3, testMTABLEID3, testMTABLEID2, testMTABLEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Measurementcolumn objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Measurementcolumn testObject = new Measurementcolumn(rockFactory, "testMTABLEID" + i, "testDATANAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementcolumn objects. True is returned if the two vectors
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
   * Test comparing two Measurementcolumn objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementcolumn objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Measurementcolumn testObject = new Measurementcolumn(rockFactory, "testMTABLEID1", "testDATANAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementcolumn objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Measurementcolumn testObject = new Measurementcolumn(rockFactory, "testMTABLEID" + i, "testDATANAME" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementcolumn");
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
    assertEquals(MeasurementcolumnFactory.class, clonedObject.getClass());
  }
}