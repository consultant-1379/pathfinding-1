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
 * Test class for MeasurementobjbhsupportFactory. Testing handling of all the objects in
 * Measurementobjbhsupport table.
 */
public class MeasurementobjbhsupportFactoryTest {

  private static MeasurementobjbhsupportFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Measurementobjbhsupport whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = MeasurementobjbhsupportFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Measurementobjbhsupport ( TYPEID VARCHAR(31)  ,OBJBHSUPPORT VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Measurementobjbhsupport(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Measurementobjbhsupport");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Measurementobjbhsupport VALUES( 'testTYPEID3'  ,'testOBJBHSUPPORT3' )");
	  stmt.executeUpdate("INSERT INTO Measurementobjbhsupport VALUES( 'testTYPEID2'  ,'testOBJBHSUPPORT2' )");
	  stmt.executeUpdate("INSERT INTO Measurementobjbhsupport VALUES( 'testTYPEID1'  ,'testOBJBHSUPPORT1' )");

    /* Initializing tested object before each test */
    objUnderTest = new MeasurementobjbhsupportFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Measurementobjbhsupport");
    objUnderTest = null;
  }
  
  /**
   * Testing MeasurementobjbhsupportFactory constructor. All rows found from Measurementobjbhsupport
   * table are put into vector.
   */
  @Test
  public void testMeasurementobjbhsupportFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementobjbhsupportFactory(rockFactory, whereObject);

    /* Asserting all Measurementobjbhsupports are found and put into vector */
    try {
      Vector<Measurementobjbhsupport> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID3, testTYPEID2, testTYPEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Measurementobjbhsupports was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementobjbhsupportFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementobjbhsupportFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing MeasurementobjbhsupportFactory constructor. All rows found from Measurementobjbhsupport
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeasurementobjbhsupportFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementobjbhsupportFactory(rockFactory, whereObject, true);

    /* Asserting all Measurementobjbhsupports are found and put into vector */
    try {     
      Vector<Measurementobjbhsupport> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testMeasurementobjbhsupportFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementobjbhsupportFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing MeasurementobjbhsupportFactory constructor. All rows found from Measurementobjbhsupport
   * table are put into vector and data validation is on.
   */
  @Test
  public void testMeasurementobjbhsupportFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new MeasurementobjbhsupportFactory(rockFactory, whereObject, "ORDER BY TYPEID");

    /* Asserting all Measurementobjbhsupports are found and put into vector */
    try {
      Vector<Measurementobjbhsupport> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID1, testTYPEID2, testTYPEID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Measurementobjbhsupports was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementobjbhsupportFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new MeasurementobjbhsupportFactory(null, whereObject, "ORDER BY TYPEID");
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
   * Testing size retrieving of the vector containing Measurementobjbhsupport objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Measurementobjbhsupport objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Measurementobjbhsupport> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getTypeid() + ", " +  actualVector.get(1).getTypeid() + ", " +  actualVector.get(2).getTypeid();
      String expected = "3, testTYPEID3, testTYPEID2, testTYPEID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Measurementobjbhsupport objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Measurementobjbhsupport testObject = new Measurementobjbhsupport(rockFactory, "testTYPEID" + i, "testOBJBHSUPPORT" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementobjbhsupport objects. True is returned if the two vectors
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
   * Test comparing two Measurementobjbhsupport objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementobjbhsupport objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Measurementobjbhsupport testObject = new Measurementobjbhsupport(rockFactory, "testTYPEID1", "testOBJBHSUPPORT1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Measurementobjbhsupport objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Measurementobjbhsupport testObject = new Measurementobjbhsupport(rockFactory, "testTYPEID" + i, "testOBJBHSUPPORT" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementobjbhsupport");
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
    assertEquals(MeasurementobjbhsupportFactory.class, clonedObject.getClass());
  }
}