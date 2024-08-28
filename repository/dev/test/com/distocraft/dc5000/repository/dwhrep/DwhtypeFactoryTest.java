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
 * Test class for DwhtypeFactory. Testing handling of all the objects in
 * Dwhtype table.
 */
public class DwhtypeFactoryTest {

  private static DwhtypeFactory objUnderTest;

  private static RockFactory rockFactory;

  private static Dwhtype whereObject;

  private static Connection con = null;

  private static Statement stmt;

  private static Field vec;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    /* Reflecting the private fields */
    vec = DwhtypeFactory.class.getDeclaredField("vec");
    vec.setAccessible(true);

    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Dwhtype ( TECHPACK_NAME VARCHAR(31)  ,TYPENAME VARCHAR(31) ,TABLELEVEL VARCHAR(31) ,STORAGEID VARCHAR(31) ,PARTITIONSIZE BIGINT  ,PARTITIONCOUNT BIGINT  ,STATUS VARCHAR(31) ,TYPE VARCHAR(31) ,OWNER VARCHAR(31) ,VIEWTEMPLATE VARCHAR(31) ,CREATETEMPLATE VARCHAR(31) ,NEXTPARTITIONTIME TIMESTAMP  ,BASETABLENAME VARCHAR(31) ,DATADATECOLUMN VARCHAR(31) ,PUBLICVIEWTEMPLATE VARCHAR(31) ,PARTITIONPLAN VARCHAR(31))");
    
    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

    /* Creating where object which tells what sort of query is to be done */
    whereObject = new Dwhtype(rockFactory);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Dwhtype");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
	  stmt.executeUpdate("INSERT INTO Dwhtype VALUES( 'testTECHPACK_NAME3'  ,'testTYPENAME3'  ,'testTABLELEVEL3'  ,'testSTORAGEID3'  ,3  ,3  ,'testSTATUS3'  ,'testTYPE3'  ,'testOWNER3'  ,'testVIEWTEMPLATE3'  ,'testCREATETEMPLATE3'  ,'2003-03-03 00:00:00.0'  ,'testBASETABLENAME3'  ,'testDATADATECOLUMN3'  ,'testPUBLICVIEWTEMPLATE3'  ,'testPARTITIONPLAN3' )");
	  stmt.executeUpdate("INSERT INTO Dwhtype VALUES( 'testTECHPACK_NAME2'  ,'testTYPENAME2'  ,'testTABLELEVEL2'  ,'testSTORAGEID2'  ,2  ,2  ,'testSTATUS2'  ,'testTYPE2'  ,'testOWNER2'  ,'testVIEWTEMPLATE2'  ,'testCREATETEMPLATE2'  ,'2002-02-02 00:00:00.0'  ,'testBASETABLENAME2'  ,'testDATADATECOLUMN2'  ,'testPUBLICVIEWTEMPLATE2'  ,'testPARTITIONPLAN2' )");
	  stmt.executeUpdate("INSERT INTO Dwhtype VALUES( 'testTECHPACK_NAME1'  ,'testTYPENAME1'  ,'testTABLELEVEL1'  ,'testSTORAGEID1'  ,1  ,1  ,'testSTATUS1'  ,'testTYPE1'  ,'testOWNER1'  ,'testVIEWTEMPLATE1'  ,'testCREATETEMPLATE1'  ,'2001-01-01 00:00:00.0'  ,'testBASETABLENAME1'  ,'testDATADATECOLUMN1'  ,'testPUBLICVIEWTEMPLATE1'  ,'testPARTITIONPLAN1' )");

    /* Initializing tested object before each test */
    objUnderTest = new DwhtypeFactory(rockFactory, whereObject);
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Dwhtype");
    objUnderTest = null;
  }
  
  /**
   * Testing DwhtypeFactory constructor. All rows found from Dwhtype
   * table are put into vector.
   */
  @Test
  public void testDwhtypeFactoryConstructorWithWhereObject() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DwhtypeFactory(rockFactory, whereObject);

    /* Asserting all Dwhtypes are found and put into vector */
    try {
      Vector<Dwhtype> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTechpack_name() + ", " +  actualVector.get(1).getTechpack_name() + ", " +  actualVector.get(2).getTechpack_name();
      String expected = "3, testTECHPACK_NAME3, testTECHPACK_NAME2, testTECHPACK_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Dwhtypes was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDwhtypeFactoryConstructorWithWhereObjectNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DwhtypeFactory(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing DwhtypeFactory constructor. All rows found from Dwhtype
   * table are put into vector and data validation is on.
   */
  @Test
  public void testDwhtypeFactoryConstructorWithValidate() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DwhtypeFactory(rockFactory, whereObject, true);

    /* Asserting all Dwhtypes are found and put into vector */
    try {     
      Vector<Dwhtype> actualVector = (Vector) vec.get(objUnderTest);
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
  public void testDwhtypeFactoryConstructorWithValidateNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DwhtypeFactory(null, whereObject, true);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing DwhtypeFactory constructor. All rows found from Dwhtype
   * table are put into vector and data validation is on.
   */
  @Test
  public void testDwhtypeFactoryConstructorWithOrderClause() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new DwhtypeFactory(rockFactory, whereObject, "ORDER BY TECHPACK_NAME");

    /* Asserting all Dwhtypes are found and put into vector */
    try {
      Vector<Dwhtype> actualVector = (Vector) vec.get(objUnderTest);
      String actual = actualVector.size() + ", " + actualVector.get(0).getTechpack_name() + ", " +  actualVector.get(1).getTechpack_name() + ", " +  actualVector.get(2).getTechpack_name();
      String expected = "3, testTECHPACK_NAME1, testTECHPACK_NAME2, testTECHPACK_NAME3";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more Dwhtypes was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDwhtypeFactoryConstructorWithOrderClauseNullRockfactory() throws Exception {

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new DwhtypeFactory(null, whereObject, "ORDER BY TECHPACK_NAME");
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

    assertEquals( "testTECHPACK_NAME2" , objUnderTest.getElementAt(1).getTechpack_name().toString());
  }
  
  /**
   * Testing Element retrieving from a vector at certain location.
   */
  @Test
  public void testGetElementAtOutOfBounds() throws Exception {

    assertEquals(null, objUnderTest.getElementAt(5));
  }
  
  /**
   * Testing size retrieving of the vector containing Dwhtype objects.
   */
  @Test
  public void testSize() throws Exception {

    assertEquals(3, objUnderTest.size());
  }
  
  /**
   * Testing vector retrieving containing Dwhtype objects.
   */
  @Test
  public void testGet() throws Exception {

    try {
      Vector<Dwhtype> actualVector = objUnderTest.get();
      String actual = actualVector.size() + ", " + actualVector.get(0).getTechpack_name() + ", " +  actualVector.get(1).getTechpack_name() + ", " +  actualVector.get(2).getTechpack_name();
      String expected = "3, testTECHPACK_NAME3, testTECHPACK_NAME2, testTECHPACK_NAME1";
      assertEquals(expected, actual);
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test Failed - One or more aggregations was not loaded from the table!\n " + aioobe);
    }
  }
  
  /**
   * Test comparing two Dwhtype objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithSameObjects() throws Exception {

    /* Creating another vector with the same objects */
    Vector otherVector = new Vector();
    for (int i = 3; i > 0; i--) {
      Dwhtype testObject = new Dwhtype(rockFactory, "testSTORAGEID" + i);
      otherVector.add(testObject);
    }

    /* Asserting the two vectors are the same */
    assertEquals(true, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Dwhtype objects. True is returned if the two vectors
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
   * Test comparing two Dwhtype objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithNullVector() throws Exception {

    Vector otherVector = null;
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Dwhtype objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAmountOfObjects() throws Exception {

    /* Creating another vector with only one object */
    Vector otherVector = new Vector();
    Dwhtype testObject = new Dwhtype(rockFactory, "testSTORAGEID1");
    otherVector.add(testObject);
    
    /* Asserting the two vectors are the same */
    assertEquals(false, objUnderTest.equals(otherVector));
  }
  
  /**
   * Test comparing two Dwhtype objects. True is returned if the two vectors
   * containing the objects are the same, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentObjects() throws Exception {

    /* Creating another vector with different objects */
    Vector otherVector = new Vector();
    for (int i = 1; i < 4; i++) {
      Dwhtype testObject = new Dwhtype(rockFactory, "testSTORAGEID" + i);
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dwhtype");
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
    assertEquals(DwhtypeFactory.class, clonedObject.getClass());
  }
}