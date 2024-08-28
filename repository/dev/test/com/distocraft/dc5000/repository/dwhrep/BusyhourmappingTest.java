package com.distocraft.dc5000.repository.dwhrep;

import static org.junit.Assert.*;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashSet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;

/**
 * Test class for Busyhourmapping. Changes to Busyhourmapping table are made via
 * this class.
 */
public class BusyhourmappingTest {

  private static Busyhourmapping objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSIONID;
  
  private static Field BHLEVEL;
  
  private static Field BHTYPE;
  
  private static Field TARGETVERSIONID;
  
  private static Field BHOBJECT;
  
  private static Field TYPEID;
  
  private static Field BHTARGETTYPE;
  
  private static Field BHTARGETLEVEL;
  
  private static Field ENABLE;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Busyhourmapping.class.getDeclaredField("newItem");
		VERSIONID = Busyhourmapping.class.getDeclaredField("VERSIONID");
		BHLEVEL = Busyhourmapping.class.getDeclaredField("BHLEVEL");
		BHTYPE = Busyhourmapping.class.getDeclaredField("BHTYPE");
		TARGETVERSIONID = Busyhourmapping.class.getDeclaredField("TARGETVERSIONID");
		BHOBJECT = Busyhourmapping.class.getDeclaredField("BHOBJECT");
		TYPEID = Busyhourmapping.class.getDeclaredField("TYPEID");
		BHTARGETTYPE = Busyhourmapping.class.getDeclaredField("BHTARGETTYPE");
		BHTARGETLEVEL = Busyhourmapping.class.getDeclaredField("BHTARGETLEVEL");
		ENABLE = Busyhourmapping.class.getDeclaredField("ENABLE");
		newItem.setAccessible(true);
		VERSIONID.setAccessible(true);
		BHLEVEL.setAccessible(true);
		BHTYPE.setAccessible(true);
		TARGETVERSIONID.setAccessible(true);
		BHOBJECT.setAccessible(true);
		TYPEID.setAccessible(true);
		BHTARGETTYPE.setAccessible(true);
		BHTARGETLEVEL.setAccessible(true);
		ENABLE.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Busyhourmapping ( VERSIONID VARCHAR(31)  ,BHLEVEL VARCHAR(31) ,BHTYPE VARCHAR(31) ,TARGETVERSIONID VARCHAR(31) ,BHOBJECT VARCHAR(31) ,TYPEID VARCHAR(31) ,BHTARGETTYPE VARCHAR(31) ,BHTARGETLEVEL VARCHAR(31) ,ENABLE INTEGER )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Busyhourmapping");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Busyhourmapping VALUES( 'testVERSIONID'  ,'testBHLEVEL'  ,'testBHTYPE'  ,'testTARGETVERSIONID'  ,'testBHOBJECT'  ,'testTYPEID'  ,'testBHTARGETTYPE'  ,'testBHTARGETLEVEL'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Busyhourmapping(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT",  "testTYPEID");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Busyhourmapping");
    objUnderTest = null;
  }
  
  /**
   * Testing Busyhourmapping constructor variable initialization with null values.
   */
  @Test
  public void testBusyhourmappingConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Busyhourmapping(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + BHLEVEL.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + TARGETVERSIONID.get(objUnderTest)  + ", " + BHOBJECT.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + BHTARGETTYPE.get(objUnderTest)  + ", " + BHTARGETLEVEL.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Busyhourmapping constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testBusyhourmappingConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Busyhourmapping(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT",  "testTYPEID");

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + BHLEVEL.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + TARGETVERSIONID.get(objUnderTest)  + ", " + BHOBJECT.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + BHTARGETTYPE.get(objUnderTest)  + ", " + BHTARGETLEVEL.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testBHLEVEL"  + ", testBHTYPE"  + ", testTARGETVERSIONID"  + ", testBHOBJECT"  + ", testTYPEID"  + ", testBHTARGETTYPE"  + ", testBHTARGETLEVEL"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testBusyhourmappingConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Busyhourmapping(null ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT",  "testTYPEID");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Busyhourmapping constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testBusyhourmappingConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Busyhourmapping whereObject = new Busyhourmapping(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Busyhourmapping(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + BHLEVEL.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + TARGETVERSIONID.get(objUnderTest)  + ", " + BHOBJECT.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + BHTARGETTYPE.get(objUnderTest)  + ", " + BHTARGETLEVEL.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testBHLEVEL"  + ", testBHTYPE"  + ", testTARGETVERSIONID"  + ", testBHOBJECT"  + ", testTYPEID"  + ", testBHTARGETTYPE"  + ", testBHTARGETLEVEL"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testBusyhourmappingConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Busyhourmapping whereObject = new Busyhourmapping(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Busyhourmapping(null, whereObject);
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing modified columns set, get and clean methods. A set object is
   * initialized using set method, which is then retrieved using get method and
   * finally the set is cleared using clean method.
   */
  @Test
  public void testGetSetAndClearModifiedColumns() throws Exception {

    /* Creating a set for testing */
    HashSet testSet = new HashSet();
    testSet.add("testClause");

    /* calling the set, get and clean methods */
    objUnderTest.setModifiedColumns(testSet);
    HashSet actualSet = (HashSet) objUnderTest.gimmeModifiedColumns();
    String actualSetToString = actualSet.toString();
    objUnderTest.cleanModifiedColumns();

    /* Asserting that the field has been set and cleared */
    String actual = actualSetToString + ", " + testSet.toString();
    assertEquals("[testClause], []", actual);
  }
  
  /**
   * Testing table name retrieving. Returned value is static.
   */
  @Test
  public void testGetTableName() throws Exception {

    /* Invoking tested method and asserting that correct value is returned */
    assertEquals("Busyhourmapping", objUnderTest.getTableName());
  }
  
  /**
   * Testing database updating. Affected row count is returned.
   */
  @Test
  public void testUpdateDB() throws Exception {

    /* Invoking tested method and asserting the update has been made */
    String actual = objUnderTest.updateDB() + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + false, actual);
  }
  
  /**
   * Testing database updating. Affected row count is returned.
   */
  @Test
  public void testUpdateDBwithTimestamp() throws Exception {

    /* Invoking tested method and asserting the update has been made */
    String actual = objUnderTest.updateDB(true) + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + false, actual);
  }
  
  /**
   * Testing database updating. Affected row count is returned.
   */
  @Test
  public void testUpdateDBWithConstructedWhereClause() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Busyhourmapping whereObject = new Busyhourmapping(rockFactory);

    /* Invoking tested method and asserting the update has been made */
    String actual = objUnderTest.updateDB(true, whereObject) + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + false, actual);
  }
  
  /**
   * Testing inserting into database. Affected row count is returned.
   */
  @Test
  public void testInsertDB() throws Exception {

    /* Invoking tested method and asserting the insert has been made */
    String actual = objUnderTest.insertDB() + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + false, actual);
  }
  
  /**
   * Testing inserting into database. Affected row count is returned.
   */
  @Test
  public void testInsertDBwithTimestamp() throws Exception {

    /* Invoking tested method and asserting the insert has been made */
    String actual = objUnderTest.insertDB(true, true) + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + false, actual);
  }
  
  /**
   * Testing deleting from database. Affected row count is returned.
   */
  @Test
  public void testDeleteDB() throws Exception {

    /* Invoking tested method and asserting the delete has been made */
    String actual = objUnderTest.deleteDB() + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + true, actual);
  }
  
  /**
   * Testing deleting from database. Affected row count is returned.
   */
  @Test
  public void testDeleteDBWithConstructedWhereClause() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Busyhourmapping whereObject = new Busyhourmapping(rockFactory);

    /* Invoking tested method and asserting the delete has been made */
    String actual = objUnderTest.deleteDB(whereObject) + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + true, actual);
  }
  
  /**
   * Testing data saving to the database.
   */
  @Test
  public void testSaveDB() throws Exception {


    /* Calling the tested method twice with different setting */
    objUnderTest.saveDB();
    newItem.set(objUnderTest, true);
    objUnderTest.saveDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Busyhourmapping");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Invoking tested method and asserting the data has been saved */
    String actual = rows + ", " + newItem.get(objUnderTest);
    assertEquals("2, " + false, actual);
  }
  
  /**
   * Testing data saving to the database.
   */
  @Test
  public void testSaveToDB() throws Exception {

    /* Calling the tested method twice, first insert, next update */
    newItem.set(objUnderTest, true);
    objUnderTest.saveToDB();
    VERSIONID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("VERSIONID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Busyhourmapping");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSIONID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSIONID FROM Busyhourmapping");
    while (res.next()) {
      queryResult = res.getString(1);
    }

    /* Invoking tested method and asserting the data has been saved */
    String actual = rows + ", " + queryResult + ", " + newItem.get(objUnderTest);
    assertEquals("2, changed, " + false, actual);
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_tag() throws Exception {
    
    String expected = "<Busyhourmapping VERSIONID=\"'testVERSIONID'\" BHLEVEL=\"'testBHLEVEL'\" BHTYPE=\"'testBHTYPE'\" TARGETVERSIONID=\"'testTARGETVERSIONID'\" BHOBJECT=\"'testBHOBJECT'\" TYPEID=\"'testTYPEID'\" BHTARGETTYPE=\"'testBHTARGETTYPE'\" BHTARGETLEVEL=\"'testBHTARGETLEVEL'\" ENABLE=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Busyhourmapping VERSIONID=\"'testVERSIONID'\" BHLEVEL=\"'testBHLEVEL'\" BHTYPE=\"'testBHTYPE'\" TARGETVERSIONID=\"'testTARGETVERSIONID'\" BHOBJECT=\"'testBHOBJECT'\" TYPEID=\"'testTYPEID'\" BHTARGETTYPE=\"'testBHTARGETTYPE'\" BHTARGETLEVEL=\"'testBHTARGETLEVEL'\" ENABLE=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Busyhourmapping>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Busyhourmapping ( VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, TYPEID, BHTARGETTYPE, BHTARGETLEVEL, ENABLE ) values "
      + "( 'testVERSIONID', 'testBHLEVEL', 'testBHTYPE', 'testTARGETVERSIONID', 'testBHOBJECT', 'testTYPEID', 'testBHTARGETTYPE', 'testBHTARGETLEVEL', 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersionid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersionid(BusyhourmappingTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(BusyhourmappingTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhlevel() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhlevel(BusyhourmappingTest.testStringGenerator("anotherBHLEVEL", 255));
    assertEquals(BusyhourmappingTest.testStringGenerator("anotherBHLEVEL", 255), BHLEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhtype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhtype(BusyhourmappingTest.testStringGenerator("anotherBHTYPE", 32));
    assertEquals(BusyhourmappingTest.testStringGenerator("anotherBHTYPE", 32), BHTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTargetversionid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTargetversionid(BusyhourmappingTest.testStringGenerator("anotherTARGETVERSIONID", 128));
    assertEquals(BusyhourmappingTest.testStringGenerator("anotherTARGETVERSIONID", 128), TARGETVERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhobject() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhobject(BusyhourmappingTest.testStringGenerator("anotherBHOBJECT", 32));
    assertEquals(BusyhourmappingTest.testStringGenerator("anotherBHOBJECT", 32), BHOBJECT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTypeid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTypeid(BusyhourmappingTest.testStringGenerator("anotherTYPEID", 255));
    assertEquals(BusyhourmappingTest.testStringGenerator("anotherTYPEID", 255), TYPEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhtargettype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhtargettype(BusyhourmappingTest.testStringGenerator("anotherBHTARGETTYPE", 128));
    assertEquals(BusyhourmappingTest.testStringGenerator("anotherBHTARGETTYPE", 128), BHTARGETTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhtargetlevel() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhtargetlevel(BusyhourmappingTest.testStringGenerator("anotherBHTARGETLEVEL", 128));
    assertEquals(BusyhourmappingTest.testStringGenerator("anotherBHTARGETLEVEL", 128), BHTARGETLEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetEnable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setEnable(555);
    assertEquals(555, ENABLE.get(objUnderTest));
  }
    
  /**
   * Testing timestamp retrieving.
   */
  @Test
  public void testGetTimestamp() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    assertEquals("LAST_UPDATED", objUnderTest.gettimeStampName());
  }
  
  /**
   * Testing column and sequence setting and retrieving via get method.
   */
  @Test
  public void testGetcolumnsAndSequences() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column and sequences */
    String[] expectedKeys = { "testColumn", "testSequence" };
    objUnderTest.setcolumnsAndSequences(expectedKeys);

    /* Asserting that */
    String[] actualKeys = objUnderTest.getcolumnsAndSequences();
    try {
      for (int i = 0; i < actualKeys.length; i++) {
        assertEquals(expectedKeys[i], actualKeys[i]);
      }
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test failed - There were different amount of actual keys than expected! \n" + aioobe);
    } catch (Exception e) {
      fail("Test Failed - Unexpected error occurred: \n" + e);
    }
  }
  
  /**
   * Testing primary key retrieving via get method.
   */
  @Test
  public void testGetPrimaryKeys() throws Exception {

    String[] actualKeys = objUnderTest.getprimaryKeyNames();
    String[] expectedKeys = { "VERSIONID","BHLEVEL","BHTYPE","TARGETVERSIONID","BHOBJECT","TYPEID"};

    try {
      for (int i = 0; i < actualKeys.length; i++) {
        assertEquals(expectedKeys[i], actualKeys[i]);
      }
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      fail("Test failed - There were different amount of actual keys than expected! \n" + aioobe);
    } catch (Exception e) {
      fail("Test Failed - Unexpected error occurred: \n" + e);
    }
  }
  
  /**
   * Testing rockfactory object retrieving via get method.
   */
  @Test
  public void testGetRockFactory() throws Exception {

    RockFactory actualRock = objUnderTest.getRockFactory();
    String actual = actualRock.getDbURL() + ", " + actualRock.getUserName() + ", " + actualRock.getPassword() + ", "
        + actualRock.getDriverName();
    assertEquals("jdbc:hsqldb:mem:testdb, sa, , org.hsqldb.jdbcDriver", actual);
  }
  
  /**
   * Testing null removing from column values.
   */
  @Test
  public void testRemoveNulls() throws Exception {

    /* Initializing tested object with nulls */
    objUnderTest = new Busyhourmapping(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSIONID.get(objUnderTest)  + ", " + BHLEVEL.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + TARGETVERSIONID.get(objUnderTest)  + ", " + BHOBJECT.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + BHTARGETTYPE.get(objUnderTest)  + ", " + BHTARGETLEVEL.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing if the current primary key(s) equals with the parameter given
   * $testedClass object. If the primary keys are the same, true is returned,
   * otherwise false.
   */
  @Test
  public void testDbEquals() throws Exception {
    
    /* Creating a $testedClass object to which tested one is compared */
    Busyhourmapping compareObj = new Busyhourmapping(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT",  "testTYPEID");

    /* Testing first with null primary key value */
    VERSIONID.set(objUnderTest, null);
  	BHLEVEL.set(objUnderTest, null);
  	BHTYPE.set(objUnderTest, null);
  	TARGETVERSIONID.set(objUnderTest, null);
  	BHOBJECT.set(objUnderTest, null);
  	TYPEID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSIONID.set(objUnderTest,  "different");
  	BHLEVEL.set(objUnderTest,  "different");
  	BHTYPE.set(objUnderTest,  "different");
  	TARGETVERSIONID.set(objUnderTest,  "different");
  	BHOBJECT.set(objUnderTest,  "different");
  	TYPEID.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSIONID.set(objUnderTest,  "testVERSIONID");
  	BHLEVEL.set(objUnderTest,  "testBHLEVEL");
  	BHTYPE.set(objUnderTest,  "testBHTYPE");
  	TARGETVERSIONID.set(objUnderTest,  "testTARGETVERSIONID");
  	BHOBJECT.set(objUnderTest,  "testBHOBJECT");
  	TYPEID.set(objUnderTest,  "testTYPEID");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Busyhourmapping with our current one. If the two
   * Busyhourmappings are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnBusyhourmapping() throws Exception {

    /* Creating another Busyhourmapping which will be compared to the tested one */
    Busyhourmapping comparedObj = new Busyhourmapping(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Busyhourmapping with our current one. If the two
   * Busyhourmappings are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentBusyhourmapping() throws Exception {

    /* Creating another Busyhourmapping which will be compared to the tested one */
    Busyhourmapping comparedObj = new Busyhourmapping(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT",  "testTYPEID");
    comparedObj.setVersionid( "DifferentVERSIONID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Busyhourmapping with our current one. If the two
   * Busyhourmappings are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameBusyhourmapping() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Busyhourmapping comparedObj = new Busyhourmapping(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT",  "testTYPEID");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Busyhourmapping with our current one using null value.
   */
  @Test
  public void testEqualsWithNullBusyhourmapping() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Busyhourmapping comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Busyhourmapping was null \n");
    } catch (NullPointerException npe) {
      // Test passed
    } catch (Exception e) {
      fail("Test Failed - Unexpected exception thrown! \n" + e);
    }
  }
  
  /**
   * Testing cloning of tested class.
   */
  @Test
  public void testClone() throws Exception {

    Object actualObject = objUnderTest.clone();
    assertEquals(Busyhourmapping.class, actualObject.getClass());
  }
  
  /**
   * Testing checking the primary key definitions. If no primary keys are
   * defined false is returned, otherwise true.
   */
  @Test
  public void testIsPrimaryDefined() throws Exception {

    String actual = objUnderTest.isPrimaryDefined() + ", ";
    Field primaryKeyNames = objUnderTest.getClass().getDeclaredField("primaryKeyNames");
    primaryKeyNames.setAccessible(true);
    String[] emptyPrimaries = {};
    primaryKeyNames.set(objUnderTest, emptyPrimaries);
    actual += objUnderTest.isPrimaryDefined();
    assertEquals(true + ", " + false, actual);
  }
  
  /**
   * Not implemented.
   */
  @Test
  public void testSetDefaults() throws Exception {
  }
  
  /**
   * Testing if tested object exists in the database. If object exists true is
   * returned, otherwise false.
   */
  @Test
  public void testExistsDB() throws Exception {

    String actual = objUnderTest.existsDB() + ", ";
    Busyhourmapping testAgg = new Busyhourmapping(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT",  "testTYPEID");
    VERSIONID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Versionid.
   */
  @Test
  public void testGetVersionidColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getVersionidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Versionid.
  */
  @Test
  public void testGetVersionidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getVersionidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Versionid.
  */
  @Test
  public void testGetVersionidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getVersionidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Bhlevel.
   */
  @Test
  public void testGetBhlevelColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getBhlevelColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Bhlevel.
  */
  @Test
  public void testGetBhlevelDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBhlevelDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Bhlevel.
  */
  @Test
  public void testGetBhlevelSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBhlevelSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Bhtype.
   */
  @Test
  public void testGetBhtypeColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getBhtypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Bhtype.
  */
  @Test
  public void testGetBhtypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBhtypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Bhtype.
  */
  @Test
  public void testGetBhtypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBhtypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Targetversionid.
   */
  @Test
  public void testGetTargetversionidColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getTargetversionidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Targetversionid.
  */
  @Test
  public void testGetTargetversionidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTargetversionidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Targetversionid.
  */
  @Test
  public void testGetTargetversionidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTargetversionidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Bhobject.
   */
  @Test
  public void testGetBhobjectColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getBhobjectColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Bhobject.
  */
  @Test
  public void testGetBhobjectDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBhobjectDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Bhobject.
  */
  @Test
  public void testGetBhobjectSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBhobjectSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Typeid.
   */
  @Test
  public void testGetTypeidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getTypeidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Typeid.
  */
  @Test
  public void testGetTypeidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTypeidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Typeid.
  */
  @Test
  public void testGetTypeidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTypeidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Bhtargettype.
   */
  @Test
  public void testGetBhtargettypeColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getBhtargettypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Bhtargettype.
  */
  @Test
  public void testGetBhtargettypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBhtargettypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Bhtargettype.
  */
  @Test
  public void testGetBhtargettypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBhtargettypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Bhtargetlevel.
   */
  @Test
  public void testGetBhtargetlevelColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getBhtargetlevelColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Bhtargetlevel.
  */
  @Test
  public void testGetBhtargetlevelDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBhtargetlevelDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Bhtargetlevel.
  */
  @Test
  public void testGetBhtargetlevelSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBhtargetlevelSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Enable.
   */
  @Test
  public void testGetEnableColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getEnableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Enable.
  */
  @Test
  public void testGetEnableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getEnableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Enable.
  */
  @Test
  public void testGetEnableSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getEnableSQLType());    
  }
  
    /**
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Busyhourmapping object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Busyhourmapping(rockFactory, false);
    objUnderTest.setOriginal(objUnderTest);
    assertNotNull(objUnderTest.getOriginal());
  }

  /**
   * Testing checking of rock object. If it is new or updated true is returned,
   * otherwise false.
   */
  @Test
  public void testIsUpdated() throws Exception {

    /* No changes made to tested object */
    HashSet modifiedColumns = new HashSet();
    objUnderTest.setModifiedColumns(modifiedColumns);
    String actual = objUnderTest.isUpdated() + ", ";
    
    /* Rock object has been changed */
    Busyhourmapping changedOriginal = new Busyhourmapping(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Busyhourmapping(rockFactory, false);
    actual += objUnderTest.isUpdated();
    assertEquals(false + ", " + true + ", " + true + ", " + true, actual);
  }
  
  /**
   * Testing checking if rock object is changed.
   */
  @Test
  public void testIsChanged() throws Exception {
    
    /* Testing rock object checking with original object */
    String actual = objUnderTest.isChanged() + ", ";
    
    /* Changing the original object */
    Busyhourmapping changedOriginal = new Busyhourmapping(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isChanged();
    assertEquals(false + ", " + true, actual);
  }
  
  /**
   * Method checking the maximum length of a string used to test column setting.
   * If test string is too long, it will be cut to be within size limit.
   */
  private static String testStringGenerator(String testString, int maxSize) throws Exception {
  
    /* Checking if the test string is too large */
    if(testString.length() > maxSize) {
      testString = testString.substring(0, maxSize);
    }
    
    return testString;
  }
}