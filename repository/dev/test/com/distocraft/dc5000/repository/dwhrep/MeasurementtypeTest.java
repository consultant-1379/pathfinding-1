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
 * Test class for Measurementtype. Changes to Measurementtype table are made via
 * this class.
 */
public class MeasurementtypeTest {

  private static Measurementtype objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field TYPEID;
  
  private static Field TYPECLASSID;
  
  private static Field TYPENAME;
  
  private static Field VENDORID;
  
  private static Field FOLDERNAME;
  
  private static Field DESCRIPTION;
  
  private static Field STATUS;
  
  private static Field VERSIONID;
  
  private static Field OBJECTID;
  
  private static Field OBJECTNAME;
  
  private static Field OBJECTVERSION;
  
  private static Field OBJECTTYPE;
  
  private static Field JOINABLE;
  
  private static Field SIZING;
  
  private static Field TOTALAGG;
  
  private static Field ELEMENTBHSUPPORT;
  
  private static Field RANKINGTABLE;
  
  private static Field DELTACALCSUPPORT;
  
  private static Field PLAINTABLE;
  
  private static Field UNIVERSEEXTENSION;
  
  private static Field VECTORSUPPORT;
  
  private static Field DATAFORMATSUPPORT;
  
  private static Field FOLLOWJOHN;
  
  private static Field ONEMINAGG;
  
  private static Field FIFTEENMINAGG;
  
  private static Field EVENTSCALCTABLE;
  
  private static Field MIXEDPARTITIONSTABLE;
  
  private static Field LOADFILE_DUP_CHECK;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Measurementtype.class.getDeclaredField("newItem");
		TYPEID = Measurementtype.class.getDeclaredField("TYPEID");
		TYPECLASSID = Measurementtype.class.getDeclaredField("TYPECLASSID");
		TYPENAME = Measurementtype.class.getDeclaredField("TYPENAME");
		VENDORID = Measurementtype.class.getDeclaredField("VENDORID");
		FOLDERNAME = Measurementtype.class.getDeclaredField("FOLDERNAME");
		DESCRIPTION = Measurementtype.class.getDeclaredField("DESCRIPTION");
		STATUS = Measurementtype.class.getDeclaredField("STATUS");
		VERSIONID = Measurementtype.class.getDeclaredField("VERSIONID");
		OBJECTID = Measurementtype.class.getDeclaredField("OBJECTID");
		OBJECTNAME = Measurementtype.class.getDeclaredField("OBJECTNAME");
		OBJECTVERSION = Measurementtype.class.getDeclaredField("OBJECTVERSION");
		OBJECTTYPE = Measurementtype.class.getDeclaredField("OBJECTTYPE");
		JOINABLE = Measurementtype.class.getDeclaredField("JOINABLE");
		SIZING = Measurementtype.class.getDeclaredField("SIZING");
		TOTALAGG = Measurementtype.class.getDeclaredField("TOTALAGG");
		ELEMENTBHSUPPORT = Measurementtype.class.getDeclaredField("ELEMENTBHSUPPORT");
		RANKINGTABLE = Measurementtype.class.getDeclaredField("RANKINGTABLE");
		DELTACALCSUPPORT = Measurementtype.class.getDeclaredField("DELTACALCSUPPORT");
		PLAINTABLE = Measurementtype.class.getDeclaredField("PLAINTABLE");
		UNIVERSEEXTENSION = Measurementtype.class.getDeclaredField("UNIVERSEEXTENSION");
		VECTORSUPPORT = Measurementtype.class.getDeclaredField("VECTORSUPPORT");
		DATAFORMATSUPPORT = Measurementtype.class.getDeclaredField("DATAFORMATSUPPORT");
		FOLLOWJOHN = Measurementtype.class.getDeclaredField("FOLLOWJOHN");
		ONEMINAGG = Measurementtype.class.getDeclaredField("ONEMINAGG");
		FIFTEENMINAGG = Measurementtype.class.getDeclaredField("FIFTEENMINAGG");
		EVENTSCALCTABLE = Measurementtype.class.getDeclaredField("EVENTSCALCTABLE");
		MIXEDPARTITIONSTABLE = Measurementtype.class.getDeclaredField("MIXEDPARTITIONSTABLE");
		LOADFILE_DUP_CHECK = Measurementtype.class.getDeclaredField("LOADFILE_DUP_CHECK");
		newItem.setAccessible(true);
		TYPEID.setAccessible(true);
		TYPECLASSID.setAccessible(true);
		TYPENAME.setAccessible(true);
		VENDORID.setAccessible(true);
		FOLDERNAME.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		STATUS.setAccessible(true);
		VERSIONID.setAccessible(true);
		OBJECTID.setAccessible(true);
		OBJECTNAME.setAccessible(true);
		OBJECTVERSION.setAccessible(true);
		OBJECTTYPE.setAccessible(true);
		JOINABLE.setAccessible(true);
		SIZING.setAccessible(true);
		TOTALAGG.setAccessible(true);
		ELEMENTBHSUPPORT.setAccessible(true);
		RANKINGTABLE.setAccessible(true);
		DELTACALCSUPPORT.setAccessible(true);
		PLAINTABLE.setAccessible(true);
		UNIVERSEEXTENSION.setAccessible(true);
		VECTORSUPPORT.setAccessible(true);
		DATAFORMATSUPPORT.setAccessible(true);
		FOLLOWJOHN.setAccessible(true);
		ONEMINAGG.setAccessible(true);
		FIFTEENMINAGG.setAccessible(true);
		EVENTSCALCTABLE.setAccessible(true);
		MIXEDPARTITIONSTABLE.setAccessible(true);
		LOADFILE_DUP_CHECK.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Measurementtype ( TYPEID VARCHAR(31)  ,TYPECLASSID VARCHAR(31) ,TYPENAME VARCHAR(31) ,VENDORID VARCHAR(31) ,FOLDERNAME VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,STATUS BIGINT  ,VERSIONID VARCHAR(31) ,OBJECTID VARCHAR(31) ,OBJECTNAME VARCHAR(31) ,OBJECTVERSION INTEGER  ,OBJECTTYPE VARCHAR(31) ,JOINABLE VARCHAR(31) ,SIZING VARCHAR(31) ,TOTALAGG INTEGER  ,ELEMENTBHSUPPORT INTEGER  ,RANKINGTABLE INTEGER  ,DELTACALCSUPPORT INTEGER  ,PLAINTABLE INTEGER  ,UNIVERSEEXTENSION VARCHAR(31) ,VECTORSUPPORT INTEGER  ,DATAFORMATSUPPORT INTEGER  ,FOLLOWJOHN INTEGER  ,ONEMINAGG INTEGER  ,FIFTEENMINAGG INTEGER  ,EVENTSCALCTABLE INTEGER  ,MIXEDPARTITIONSTABLE INTEGER  ,LOADFILE_DUP_CHECK INTEGER ,SONAGG INTEGER, SONFIFTEENMINAGG INTEGER ,ROPGRPCELL varchar(255))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Measurementtype");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Measurementtype VALUES( 'testTYPEID'  ,'testTYPECLASSID'  ,'testTYPENAME'  ,'testVENDORID'  ,'testFOLDERNAME'  ,'testDESCRIPTION'  ,1  ,'testVERSIONID'  ,'testOBJECTID'  ,'testOBJECTNAME'  ,1  ,'testOBJECTTYPE'  ,'testJOINABLE'  ,'testSIZING'  ,1  ,1  ,1  ,1  ,1  ,'testUNIVERSEEXTENSION'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1 ,0 ,0 ,'test' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Measurementtype(rockFactory ,  "testTYPEID");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Measurementtype");
    objUnderTest = null;
  }
  
  /**
   * Testing Measurementtype constructor variable initialization with null values.
   */
  @Test
  public void testMeasurementtypeConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Measurementtype(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + TYPECLASSID.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + VENDORID.get(objUnderTest)  + ", " + FOLDERNAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + OBJECTID.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + OBJECTVERSION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + JOINABLE.get(objUnderTest)  + ", " + SIZING.get(objUnderTest)  + ", " + TOTALAGG.get(objUnderTest)  + ", " + ELEMENTBHSUPPORT.get(objUnderTest)  + ", " + RANKINGTABLE.get(objUnderTest)  + ", " + DELTACALCSUPPORT.get(objUnderTest)  + ", " + PLAINTABLE.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + VECTORSUPPORT.get(objUnderTest)  + ", " + DATAFORMATSUPPORT.get(objUnderTest)  + ", " + FOLLOWJOHN.get(objUnderTest)  + ", " + ONEMINAGG.get(objUnderTest)  + ", " + FIFTEENMINAGG.get(objUnderTest)  + ", " + EVENTSCALCTABLE.get(objUnderTest)  + ", " + MIXEDPARTITIONSTABLE.get(objUnderTest)  + ", " + LOADFILE_DUP_CHECK.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Measurementtype constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeasurementtypeConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Measurementtype(rockFactory ,  "testTYPEID");

    /* Asserting that variables are initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + TYPECLASSID.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + VENDORID.get(objUnderTest)  + ", " + FOLDERNAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + OBJECTID.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + OBJECTVERSION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + JOINABLE.get(objUnderTest)  + ", " + SIZING.get(objUnderTest)  + ", " + TOTALAGG.get(objUnderTest)  + ", " + ELEMENTBHSUPPORT.get(objUnderTest)  + ", " + RANKINGTABLE.get(objUnderTest)  + ", " + DELTACALCSUPPORT.get(objUnderTest)  + ", " + PLAINTABLE.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + VECTORSUPPORT.get(objUnderTest)  + ", " + DATAFORMATSUPPORT.get(objUnderTest)  + ", " + FOLLOWJOHN.get(objUnderTest)  + ", " + ONEMINAGG.get(objUnderTest)  + ", " + FIFTEENMINAGG.get(objUnderTest)  + ", " + EVENTSCALCTABLE.get(objUnderTest)  + ", " + MIXEDPARTITIONSTABLE.get(objUnderTest)  + ", " + LOADFILE_DUP_CHECK.get(objUnderTest) ;
    String expected =  "testTYPEID"  + ", testTYPECLASSID"  + ", testTYPENAME"  + ", testVENDORID"  + ", testFOLDERNAME"  + ", testDESCRIPTION"  + ", 1"  + ", testVERSIONID"  + ", testOBJECTID"  + ", testOBJECTNAME"  + ", 1"  + ", testOBJECTTYPE"  + ", testJOINABLE"  + ", testSIZING"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testUNIVERSEEXTENSION"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementtypeConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Measurementtype(null ,  "testTYPEID");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Measurementtype constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeasurementtypeConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Measurementtype whereObject = new Measurementtype(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Measurementtype(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + TYPECLASSID.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + VENDORID.get(objUnderTest)  + ", " + FOLDERNAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + OBJECTID.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + OBJECTVERSION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + JOINABLE.get(objUnderTest)  + ", " + SIZING.get(objUnderTest)  + ", " + TOTALAGG.get(objUnderTest)  + ", " + ELEMENTBHSUPPORT.get(objUnderTest)  + ", " + RANKINGTABLE.get(objUnderTest)  + ", " + DELTACALCSUPPORT.get(objUnderTest)  + ", " + PLAINTABLE.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + VECTORSUPPORT.get(objUnderTest)  + ", " + DATAFORMATSUPPORT.get(objUnderTest)  + ", " + FOLLOWJOHN.get(objUnderTest)  + ", " + ONEMINAGG.get(objUnderTest)  + ", " + FIFTEENMINAGG.get(objUnderTest)  + ", " + EVENTSCALCTABLE.get(objUnderTest)  + ", " + MIXEDPARTITIONSTABLE.get(objUnderTest)  + ", " + LOADFILE_DUP_CHECK.get(objUnderTest) ;
    String expected =  "testTYPEID"  + ", testTYPECLASSID"  + ", testTYPENAME"  + ", testVENDORID"  + ", testFOLDERNAME"  + ", testDESCRIPTION"  + ", 1"  + ", testVERSIONID"  + ", testOBJECTID"  + ", testOBJECTNAME"  + ", 1"  + ", testOBJECTTYPE"  + ", testJOINABLE"  + ", testSIZING"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testUNIVERSEEXTENSION"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementtypeConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Measurementtype whereObject = new Measurementtype(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Measurementtype(null, whereObject);
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
    assertEquals("Measurementtype", objUnderTest.getTableName());
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
    Measurementtype whereObject = new Measurementtype(rockFactory);

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
    Measurementtype whereObject = new Measurementtype(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementtype");
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
    TYPEID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("TYPEID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementtype");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the TYPEID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT TYPEID FROM Measurementtype");
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
    
    String expected = "<Measurementtype TYPEID=\"'testTYPEID'\" TYPECLASSID=\"'testTYPECLASSID'\" TYPENAME=\"'testTYPENAME'\" VENDORID=\"'testVENDORID'\" FOLDERNAME=\"'testFOLDERNAME'\" DESCRIPTION=\"'testDESCRIPTION'\" STATUS=\"1\" VERSIONID=\"'testVERSIONID'\" OBJECTID=\"'testOBJECTID'\" OBJECTNAME=\"'testOBJECTNAME'\" OBJECTVERSION=\"1\" OBJECTTYPE=\"'testOBJECTTYPE'\" JOINABLE=\"'testJOINABLE'\" SIZING=\"'testSIZING'\" TOTALAGG=\"1\" ELEMENTBHSUPPORT=\"1\" RANKINGTABLE=\"1\" DELTACALCSUPPORT=\"1\" PLAINTABLE=\"1\" UNIVERSEEXTENSION=\"'testUNIVERSEEXTENSION'\" VECTORSUPPORT=\"1\" DATAFORMATSUPPORT=\"1\" FOLLOWJOHN=\"1\" ONEMINAGG=\"1\" FIFTEENMINAGG=\"1\" EVENTSCALCTABLE=\"1\" MIXEDPARTITIONSTABLE=\"1\" LOADFILE_DUP_CHECK=\"1\" SONAGG=\"0\" SONFIFTEENMINAGG=\"0\" ROPGRPCELL=\"'test'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Measurementtype TYPEID=\"'testTYPEID'\" TYPECLASSID=\"'testTYPECLASSID'\" TYPENAME=\"'testTYPENAME'\" VENDORID=\"'testVENDORID'\" FOLDERNAME=\"'testFOLDERNAME'\" DESCRIPTION=\"'testDESCRIPTION'\" STATUS=\"1\" VERSIONID=\"'testVERSIONID'\" OBJECTID=\"'testOBJECTID'\" OBJECTNAME=\"'testOBJECTNAME'\" OBJECTVERSION=\"1\" OBJECTTYPE=\"'testOBJECTTYPE'\" JOINABLE=\"'testJOINABLE'\" SIZING=\"'testSIZING'\" TOTALAGG=\"1\" ELEMENTBHSUPPORT=\"1\" RANKINGTABLE=\"1\" DELTACALCSUPPORT=\"1\" PLAINTABLE=\"1\" UNIVERSEEXTENSION=\"'testUNIVERSEEXTENSION'\" VECTORSUPPORT=\"1\" DATAFORMATSUPPORT=\"1\" FOLLOWJOHN=\"1\" ONEMINAGG=\"1\" FIFTEENMINAGG=\"1\" EVENTSCALCTABLE=\"1\" MIXEDPARTITIONSTABLE=\"1\" LOADFILE_DUP_CHECK=\"1\" SONAGG=\"0\" SONFIFTEENMINAGG=\"0\" ROPGRPCELL=\"'test'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Measurementtype>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Measurementtype ( TYPEID, TYPECLASSID, TYPENAME, VENDORID, FOLDERNAME, DESCRIPTION, STATUS, VERSIONID, OBJECTID, OBJECTNAME, OBJECTVERSION, OBJECTTYPE, JOINABLE, SIZING, TOTALAGG, ELEMENTBHSUPPORT, RANKINGTABLE, DELTACALCSUPPORT, PLAINTABLE, UNIVERSEEXTENSION, VECTORSUPPORT, DATAFORMATSUPPORT, FOLLOWJOHN, ONEMINAGG, FIFTEENMINAGG, EVENTSCALCTABLE, MIXEDPARTITIONSTABLE, LOADFILE_DUP_CHECK, SONAGG, SONFIFTEENMINAGG, ROPGRPCELL ) values "
      + "( 'testTYPEID', 'testTYPECLASSID', 'testTYPENAME', 'testVENDORID', 'testFOLDERNAME', 'testDESCRIPTION', 1, 'testVERSIONID', 'testOBJECTID', 'testOBJECTNAME', 1, 'testOBJECTTYPE', 'testJOINABLE', 'testSIZING', 1, 1, 1, 1, 1, 'testUNIVERSEEXTENSION', 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 'test' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTypeid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTypeid(MeasurementtypeTest.testStringGenerator("anotherTYPEID", 255));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherTYPEID", 255), TYPEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTypeclassid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTypeclassid(MeasurementtypeTest.testStringGenerator("anotherTYPECLASSID", 255));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherTYPECLASSID", 255), TYPECLASSID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTypename() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTypename(MeasurementtypeTest.testStringGenerator("anotherTYPENAME", 255));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherTYPENAME", 255), TYPENAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVendorid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVendorid(MeasurementtypeTest.testStringGenerator("anotherVENDORID", 128));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherVENDORID", 128), VENDORID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFoldername() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFoldername(MeasurementtypeTest.testStringGenerator("anotherFOLDERNAME", 128));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherFOLDERNAME", 128), FOLDERNAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(MeasurementtypeTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatus() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatus(555L);
    assertEquals(555L, STATUS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersionid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersionid(MeasurementtypeTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjectid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjectid(MeasurementtypeTest.testStringGenerator("anotherOBJECTID", 255));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherOBJECTID", 255), OBJECTID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjectname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjectname(MeasurementtypeTest.testStringGenerator("anotherOBJECTNAME", 255));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherOBJECTNAME", 255), OBJECTNAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjectversion() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjectversion(555);
    assertEquals(555, OBJECTVERSION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjecttype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjecttype(MeasurementtypeTest.testStringGenerator("anotherOBJECTTYPE", 255));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherOBJECTTYPE", 255), OBJECTTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetJoinable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setJoinable(MeasurementtypeTest.testStringGenerator("anotherJOINABLE", 255));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherJOINABLE", 255), JOINABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSizing() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSizing(MeasurementtypeTest.testStringGenerator("anotherSIZING", 16));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherSIZING", 16), SIZING.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTotalagg() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTotalagg(555);
    assertEquals(555, TOTALAGG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetElementbhsupport() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setElementbhsupport(555);
    assertEquals(555, ELEMENTBHSUPPORT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetRankingtable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setRankingtable(555);
    assertEquals(555, RANKINGTABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDeltacalcsupport() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDeltacalcsupport(555);
    assertEquals(555, DELTACALCSUPPORT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPlaintable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPlaintable(555);
    assertEquals(555, PLAINTABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniverseextension() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniverseextension(MeasurementtypeTest.testStringGenerator("anotherUNIVERSEEXTENSION", 12));
    assertEquals(MeasurementtypeTest.testStringGenerator("anotherUNIVERSEEXTENSION", 12), UNIVERSEEXTENSION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVectorsupport() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVectorsupport(555);
    assertEquals(555, VECTORSUPPORT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataformatsupport() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataformatsupport(555);
    assertEquals(555, DATAFORMATSUPPORT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFollowjohn() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFollowjohn(555);
    assertEquals(555, FOLLOWJOHN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetOneminagg() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setOneminagg(555);
    assertEquals(555, ONEMINAGG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFifteenminagg() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFifteenminagg(555);
    assertEquals(555, FIFTEENMINAGG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetEventscalctable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setEventscalctable(555);
    assertEquals(555, EVENTSCALCTABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMixedpartitionstable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMixedpartitionstable(555);
    assertEquals(555, MIXEDPARTITIONSTABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLoadfile_dup_check() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLoadfile_dup_check(555);
    assertEquals(555, LOADFILE_DUP_CHECK.get(objUnderTest));
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
    String[] expectedKeys = { "TYPEID"};

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
    objUnderTest = new Measurementtype(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  TYPEID.get(objUnderTest)  + ", " + TYPECLASSID.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + VENDORID.get(objUnderTest)  + ", " + FOLDERNAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + OBJECTID.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + OBJECTVERSION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + JOINABLE.get(objUnderTest)  + ", " + SIZING.get(objUnderTest)  + ", " + TOTALAGG.get(objUnderTest)  + ", " + ELEMENTBHSUPPORT.get(objUnderTest)  + ", " + RANKINGTABLE.get(objUnderTest)  + ", " + DELTACALCSUPPORT.get(objUnderTest)  + ", " + PLAINTABLE.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + VECTORSUPPORT.get(objUnderTest)  + ", " + DATAFORMATSUPPORT.get(objUnderTest)  + ", " + FOLLOWJOHN.get(objUnderTest)  + ", " + ONEMINAGG.get(objUnderTest)  + ", " + FIFTEENMINAGG.get(objUnderTest)  + ", " + EVENTSCALCTABLE.get(objUnderTest)  + ", " + MIXEDPARTITIONSTABLE.get(objUnderTest)  + ", " + LOADFILE_DUP_CHECK.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0"  + ", "  + ", "  + ", "  + ", 0"  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0" ;
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
    Measurementtype compareObj = new Measurementtype(rockFactory ,  "testTYPEID");

    /* Testing first with null primary key value */
    TYPEID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    TYPEID.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    TYPEID.set(objUnderTest,  "testTYPEID");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Measurementtype with our current one. If the two
   * Measurementtypes are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeasurementtype() throws Exception {

    /* Creating another Measurementtype which will be compared to the tested one */
    Measurementtype comparedObj = new Measurementtype(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementtype with our current one. If the two
   * Measurementtypes are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeasurementtype() throws Exception {

    /* Creating another Measurementtype which will be compared to the tested one */
    Measurementtype comparedObj = new Measurementtype(rockFactory ,  "testTYPEID");
    comparedObj.setTypeid( "DifferentTYPEID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementtype with our current one. If the two
   * Measurementtypes are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeasurementtype() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Measurementtype comparedObj = new Measurementtype(rockFactory ,  "testTYPEID");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementtype with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeasurementtype() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Measurementtype comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Measurementtype was null \n");
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
    assertEquals(Measurementtype.class, actualObject.getClass());
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
    Measurementtype testAgg = new Measurementtype(rockFactory ,  "testTYPEID");
    TYPEID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
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
   * Testing columnsize retrieving for Typeclassid.
   */
  @Test
  public void testGetTypeclassidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getTypeclassidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Typeclassid.
  */
  @Test
  public void testGetTypeclassidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTypeclassidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Typeclassid.
  */
  @Test
  public void testGetTypeclassidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTypeclassidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Typename.
   */
  @Test
  public void testGetTypenameColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getTypenameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Typename.
  */
  @Test
  public void testGetTypenameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTypenameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Typename.
  */
  @Test
  public void testGetTypenameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTypenameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Vendorid.
   */
  @Test
  public void testGetVendoridColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getVendoridColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Vendorid.
  */
  @Test
  public void testGetVendoridDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getVendoridDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Vendorid.
  */
  @Test
  public void testGetVendoridSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getVendoridSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Foldername.
   */
  @Test
  public void testGetFoldernameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getFoldernameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Foldername.
  */
  @Test
  public void testGetFoldernameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFoldernameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Foldername.
  */
  @Test
  public void testGetFoldernameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getFoldernameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Description.
   */
  @Test
  public void testGetDescriptionColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getDescriptionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Description.
  */
  @Test
  public void testGetDescriptionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDescriptionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Description.
  */
  @Test
  public void testGetDescriptionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDescriptionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Status.
   */
  @Test
  public void testGetStatusColumnSize() throws Exception {
    
     assertEquals(9, objUnderTest.getStatusColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Status.
  */
  @Test
  public void testGetStatusDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getStatusDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Status.
  */
  @Test
  public void testGetStatusSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getStatusSQLType());    
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
   * Testing columnsize retrieving for Objectid.
   */
  @Test
  public void testGetObjectidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getObjectidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objectid.
  */
  @Test
  public void testGetObjectidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjectidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objectid.
  */
  @Test
  public void testGetObjectidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjectidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objectname.
   */
  @Test
  public void testGetObjectnameColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getObjectnameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objectname.
  */
  @Test
  public void testGetObjectnameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjectnameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objectname.
  */
  @Test
  public void testGetObjectnameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjectnameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objectversion.
   */
  @Test
  public void testGetObjectversionColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getObjectversionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objectversion.
  */
  @Test
  public void testGetObjectversionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjectversionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objectversion.
  */
  @Test
  public void testGetObjectversionSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getObjectversionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objecttype.
   */
  @Test
  public void testGetObjecttypeColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getObjecttypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objecttype.
  */
  @Test
  public void testGetObjecttypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjecttypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objecttype.
  */
  @Test
  public void testGetObjecttypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjecttypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Joinable.
   */
  @Test
  public void testGetJoinableColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getJoinableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Joinable.
  */
  @Test
  public void testGetJoinableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getJoinableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Joinable.
  */
  @Test
  public void testGetJoinableSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getJoinableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Sizing.
   */
  @Test
  public void testGetSizingColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getSizingColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Sizing.
  */
  @Test
  public void testGetSizingDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSizingDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Sizing.
  */
  @Test
  public void testGetSizingSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSizingSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Totalagg.
   */
  @Test
  public void testGetTotalaggColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getTotalaggColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Totalagg.
  */
  @Test
  public void testGetTotalaggDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTotalaggDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Totalagg.
  */
  @Test
  public void testGetTotalaggSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getTotalaggSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Elementbhsupport.
   */
  @Test
  public void testGetElementbhsupportColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getElementbhsupportColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Elementbhsupport.
  */
  @Test
  public void testGetElementbhsupportDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getElementbhsupportDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Elementbhsupport.
  */
  @Test
  public void testGetElementbhsupportSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getElementbhsupportSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Rankingtable.
   */
  @Test
  public void testGetRankingtableColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getRankingtableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Rankingtable.
  */
  @Test
  public void testGetRankingtableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getRankingtableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Rankingtable.
  */
  @Test
  public void testGetRankingtableSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getRankingtableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Deltacalcsupport.
   */
  @Test
  public void testGetDeltacalcsupportColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getDeltacalcsupportColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Deltacalcsupport.
  */
  @Test
  public void testGetDeltacalcsupportDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDeltacalcsupportDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Deltacalcsupport.
  */
  @Test
  public void testGetDeltacalcsupportSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getDeltacalcsupportSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Plaintable.
   */
  @Test
  public void testGetPlaintableColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getPlaintableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Plaintable.
  */
  @Test
  public void testGetPlaintableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPlaintableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Plaintable.
  */
  @Test
  public void testGetPlaintableSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getPlaintableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Universeextension.
   */
  @Test
  public void testGetUniverseextensionColumnSize() throws Exception {
    
     assertEquals(12, objUnderTest.getUniverseextensionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Universeextension.
  */
  @Test
  public void testGetUniverseextensionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUniverseextensionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Universeextension.
  */
  @Test
  public void testGetUniverseextensionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUniverseextensionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Vectorsupport.
   */
  @Test
  public void testGetVectorsupportColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getVectorsupportColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Vectorsupport.
  */
  @Test
  public void testGetVectorsupportDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getVectorsupportDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Vectorsupport.
  */
  @Test
  public void testGetVectorsupportSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getVectorsupportSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Dataformatsupport.
   */
  @Test
  public void testGetDataformatsupportColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getDataformatsupportColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dataformatsupport.
  */
  @Test
  public void testGetDataformatsupportDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDataformatsupportDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dataformatsupport.
  */
  @Test
  public void testGetDataformatsupportSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getDataformatsupportSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Followjohn.
   */
  @Test
  public void testGetFollowjohnColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getFollowjohnColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Followjohn.
  */
  @Test
  public void testGetFollowjohnDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFollowjohnDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Followjohn.
  */
  @Test
  public void testGetFollowjohnSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getFollowjohnSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Oneminagg.
   */
  @Test
  public void testGetOneminaggColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getOneminaggColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Oneminagg.
  */
  @Test
  public void testGetOneminaggDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getOneminaggDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Oneminagg.
  */
  @Test
  public void testGetOneminaggSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getOneminaggSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Fifteenminagg.
   */
  @Test
  public void testGetFifteenminaggColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getFifteenminaggColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Fifteenminagg.
  */
  @Test
  public void testGetFifteenminaggDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFifteenminaggDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Fifteenminagg.
  */
  @Test
  public void testGetFifteenminaggSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getFifteenminaggSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Eventscalctable.
   */
  @Test
  public void testGetEventscalctableColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getEventscalctableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Eventscalctable.
  */
  @Test
  public void testGetEventscalctableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getEventscalctableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Eventscalctable.
  */
  @Test
  public void testGetEventscalctableSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getEventscalctableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Mixedpartitionstable.
   */
  @Test
  public void testGetMixedpartitionstableColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getMixedpartitionstableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Mixedpartitionstable.
  */
  @Test
  public void testGetMixedpartitionstableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMixedpartitionstableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Mixedpartitionstable.
  */
  @Test
  public void testGetMixedpartitionstableSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getMixedpartitionstableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Loadfile_dup_check.
   */
  @Test
  public void testGetLoadfile_dup_checkColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getLoadfile_dup_checkColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Loadfile_dup_check.
  */
  @Test
  public void testGetLoadfile_dup_checkDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLoadfile_dup_checkDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Loadfile_dup_check.
  */
  @Test
  public void testGetLoadfile_dup_checkSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getLoadfile_dup_checkSQLType());    
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
   * Testing original Measurementtype object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Measurementtype(rockFactory, false);
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
    Measurementtype changedOriginal = new Measurementtype(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Measurementtype(rockFactory, false);
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
    Measurementtype changedOriginal = new Measurementtype(rockFactory, false);
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