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
 * Test class for TechpackdependencyFactory. Testing handling of all the objects in
 * Techpackdependency table.
 */
public class TechpackdependencyFactoryTest {

  private static TechpackdependencyFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Techpackdependency whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = TechpackdependencyFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Techpackdependency ( VERSIONID VARCHAR(31)  ,TECHPACKNAME VARCHAR(31) ,VERSION VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Techpackdependency(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Techpackdependency");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Techpackdependency VALUES( 'testVERSIONID3'  ,'testTECHPACKNAME3'  ,'testVERSION3' )");
	  stmt.executeUpdate("INSERT INTO Techpackdependency VALUES( 'testVERSIONID2'  ,'testTECHPACKNAME2'  ,'testVERSION2' )");
	  stmt.executeUpdate("INSERT INTO Techpackdependency VALUES( 'testVERSIONID1'  ,'testTECHPACKNAME1'  ,'testVERSION1' )");

    /* Initializing tested object before each test */
    objUnderTest = new TechpackdependencyFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Techpackdependency");
    objUnderTest = null;
  }
  
  /**
   * Testing TechpackdependencyFactory constructor. All rows found from Techpackdependency
   * table are put into vector.
   */
  @Test
  public void testTechpackdependencyFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new TechpackdependencyFactory(rockFactory, whereObject);

    /* Asserting all Techpackdependencys are found and put into vector */
    try {
      Vector<Techpackdependency> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Techpackdependencys was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testTechpackdependencyFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new TechpackdependencyFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing TechpackdependencyFactory constructor. All rows found from Techpackdependency
   * table are put into vector and data validation is on.
   */
  @Test
  public void testTechpackdependencyFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new TechpackdependencyFactory(rockFactory, whereObject, true);

    /* Asserting all Techpackdependencys are found and put into vector */
    try {     
      Vector<Techpackdependency> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testTechpackdependencyFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new TechpackdependencyFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing TechpackdependencyFactory constructor. All rows found from Techpackdependency
   * table are put into vector and data validation is on.
   */
  @Test
  public void testTechpackdependencyFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new TechpackdependencyFactory(rockFactory, whereObject, "ORDER BY VERSIONID");

    /* Asserting all Techpackdependencys are found and put into vector */
    try {
      Vector<Techpackdependency> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID1, testVERSIONID2, testVERSIONID3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Techpackdependencys was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testTechpackdependencyFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new TechpackdependencyFactory(null, whereObject, "ORDER BY VERSIONID");
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
   * Testing size retrieving of the vector containing Techpackdependency objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Techpackdependency objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Techpackdependency> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getVersionid() + ", " +  actualVector.get(1).getVersionid() + ", " +  actualVector.get(2).getVersionid();
      String expected = "3, testVERSIONID3, testVERSIONID2, testVERSIONID1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Techpackdependency objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Techpackdependency testObject = new Techpackdependency(rockFactory, "testVERSIONID" + i, "testTECHPACKNAME" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Techpackdependency objects. True is returned if the two vectors
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
   * Test comparing two Techpackdependency objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Techpackdependency objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Techpackdependency testObject = new Techpackdependency(rockFactory, "testVERSIONID1", "testTECHPACKNAME1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Techpackdependency objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Techpackdependency testObject = new Techpackdependency(rockFactory, "testVERSIONID" + i, "testTECHPACKNAME" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Techpackdependency");
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
    assertEquals(TechpackdependencyFactory.class, clonedObject.getClass());
  }
}