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
 * Test class for DwhpartitionFactory. Testing handling of all the objects in
 * Dwhpartition table.
 */
public class DwhpartitionFactoryTest {

  private static DwhpartitionFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Dwhpartition whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = DwhpartitionFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Dwhpartition ( STORAGEID VARCHAR(31)  ,TABLENAME VARCHAR(31) ,STARTTIME TIMESTAMP  ,ENDTIME TIMESTAMP  ,STATUS VARCHAR(31) ,LOADORDER INTEGER )");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Dwhpartition(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Dwhpartition");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Dwhpartition VALUES( 'testSTORAGEID3'  ,'testTABLENAME3'  ,'2003-03-03 00:00:00.0'  ,'2003-03-03 00:00:00.0'  ,'testSTATUS3'  ,3 )");
	  stmt.executeUpdate("INSERT INTO Dwhpartition VALUES( 'testSTORAGEID2'  ,'testTABLENAME2'  ,'2002-02-02 00:00:00.0'  ,'2002-02-02 00:00:00.0'  ,'testSTATUS2'  ,2 )");
	  stmt.executeUpdate("INSERT INTO Dwhpartition VALUES( 'testSTORAGEID1'  ,'testTABLENAME1'  ,'2001-01-01 00:00:00.0'  ,'2001-01-01 00:00:00.0'  ,'testSTATUS1'  ,1 )");

    /* Initializing tested object before each test */
    objUnderTest = new DwhpartitionFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Dwhpartition");
    objUnderTest = null;
  }
  
  /**
   * Testing DwhpartitionFactory constructor. All rows found from Dwhpartition
   * table are put into vector.
   */
  @Test
  public void testDwhpartitionFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DwhpartitionFactory(rockFactory, whereObject);

    /* Asserting all Dwhpartitions are found and put into vector */
    try {
      Vector<Dwhpartition> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getStorageid() + ", " +  actualVector.get(1).getStorageid() + ", " +  actualVector.get(2).getStorageid();
      String expected = "3, testSTORAGEID3, testSTORAGEID2, testSTORAGEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Dwhpartitions was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDwhpartitionFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DwhpartitionFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing DwhpartitionFactory constructor. All rows found from Dwhpartition
   * table are put into vector and data validation is on.
   */
  @Test
  public void testDwhpartitionFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DwhpartitionFactory(rockFactory, whereObject, true);

    /* Asserting all Dwhpartitions are found and put into vector */
    try {     
      Vector<Dwhpartition> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testDwhpartitionFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DwhpartitionFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing DwhpartitionFactory constructor. All rows found from Dwhpartition
   * table are put into vector and data validation is on.
   */
  @Test
  public void testDwhpartitionFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DwhpartitionFactory(rockFactory, whereObject, "ORDER BY STORAGEID");

    /* Asserting all Dwhpartitions are found and put into vector */
    try {
      Vector<Dwhpartition> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getStorageid() + ", " +  actualVector.get(1).getStorageid() + ", " +  actualVector.get(2).getStorageid();
      String expected = "3, testSTORAGEID1, testSTORAGEID2, testSTORAGEID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Dwhpartitions was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDwhpartitionFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DwhpartitionFactory(null, whereObject, "ORDER BY STORAGEID");
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

    assertEquals( "testSTORAGEID2" , objUnderTest.getElementAt(1).getStorageid().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Dwhpartition objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Dwhpartition objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Dwhpartition> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getStorageid() + ", " +  actualVector.get(1).getStorageid() + ", " +  actualVector.get(2).getStorageid();
      String expected = "3, testSTORAGEID3, testSTORAGEID2, testSTORAGEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Dwhpartition objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Dwhpartition testObject = new Dwhpartition(rockFactory, "testTABLENAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Dwhpartition objects. True is returned if the two vectors
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
   * Test comparing two Dwhpartition objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Dwhpartition objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Dwhpartition testObject = new Dwhpartition(rockFactory, "testTABLENAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Dwhpartition objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Dwhpartition testObject = new Dwhpartition(rockFactory, "testTABLENAME" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dwhpartition");
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
    assertEquals(DwhpartitionFactory.class, clonedObject.getClass());
  }
}