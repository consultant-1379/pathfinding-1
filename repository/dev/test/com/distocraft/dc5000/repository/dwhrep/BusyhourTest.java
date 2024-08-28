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
 * Test class for Busyhour. Changes to Busyhour table are made via
 * this class.
 */
public class BusyhourTest {

  private static Busyhour objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSIONID;
  
  private static Field BHLEVEL;
  
  private static Field BHTYPE;
  
  private static Field BHCRITERIA;
  
  private static Field WHERECLAUSE;
  
  private static Field DESCRIPTION;
  
  private static Field TARGETVERSIONID;
  
  private static Field BHOBJECT;
  
  private static Field BHELEMENT;
  
  private static Field ENABLE;
  
  private static Field AGGREGATIONTYPE;
  
  private static Field OFFSET;
  
  private static Field WINDOWSIZE;
  
  private static Field LOOKBACK;
  
  private static Field P_THRESHOLD;
  
  private static Field N_THRESHOLD;
  
  private static Field CLAUSE;
  
  private static Field PLACEHOLDERTYPE;
  
  private static Field GROUPING;
  
  private static Field REACTIVATEVIEWS;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Busyhour.class.getDeclaredField("newItem");
		VERSIONID = Busyhour.class.getDeclaredField("VERSIONID");
		BHLEVEL = Busyhour.class.getDeclaredField("BHLEVEL");
		BHTYPE = Busyhour.class.getDeclaredField("BHTYPE");
		BHCRITERIA = Busyhour.class.getDeclaredField("BHCRITERIA");
		WHERECLAUSE = Busyhour.class.getDeclaredField("WHERECLAUSE");
		DESCRIPTION = Busyhour.class.getDeclaredField("DESCRIPTION");
		TARGETVERSIONID = Busyhour.class.getDeclaredField("TARGETVERSIONID");
		BHOBJECT = Busyhour.class.getDeclaredField("BHOBJECT");
		BHELEMENT = Busyhour.class.getDeclaredField("BHELEMENT");
		ENABLE = Busyhour.class.getDeclaredField("ENABLE");
		AGGREGATIONTYPE = Busyhour.class.getDeclaredField("AGGREGATIONTYPE");
		OFFSET = Busyhour.class.getDeclaredField("OFFSET");
		WINDOWSIZE = Busyhour.class.getDeclaredField("WINDOWSIZE");
		LOOKBACK = Busyhour.class.getDeclaredField("LOOKBACK");
		P_THRESHOLD = Busyhour.class.getDeclaredField("P_THRESHOLD");
		N_THRESHOLD = Busyhour.class.getDeclaredField("N_THRESHOLD");
		CLAUSE = Busyhour.class.getDeclaredField("CLAUSE");
		PLACEHOLDERTYPE = Busyhour.class.getDeclaredField("PLACEHOLDERTYPE");
		GROUPING = Busyhour.class.getDeclaredField("GROUPING");
		REACTIVATEVIEWS = Busyhour.class.getDeclaredField("REACTIVATEVIEWS");
		newItem.setAccessible(true);
		VERSIONID.setAccessible(true);
		BHLEVEL.setAccessible(true);
		BHTYPE.setAccessible(true);
		BHCRITERIA.setAccessible(true);
		WHERECLAUSE.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		TARGETVERSIONID.setAccessible(true);
		BHOBJECT.setAccessible(true);
		BHELEMENT.setAccessible(true);
		ENABLE.setAccessible(true);
		AGGREGATIONTYPE.setAccessible(true);
		OFFSET.setAccessible(true);
		WINDOWSIZE.setAccessible(true);
		LOOKBACK.setAccessible(true);
		P_THRESHOLD.setAccessible(true);
		N_THRESHOLD.setAccessible(true);
		CLAUSE.setAccessible(true);
		PLACEHOLDERTYPE.setAccessible(true);
		GROUPING.setAccessible(true);
		REACTIVATEVIEWS.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Busyhour ( VERSIONID VARCHAR(31)  ,BHLEVEL VARCHAR(31) ,BHTYPE VARCHAR(31) ,BHCRITERIA VARCHAR(31) ,WHERECLAUSE VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,TARGETVERSIONID VARCHAR(31) ,BHOBJECT VARCHAR(31) ,BHELEMENT INTEGER  ,ENABLE INTEGER  ,AGGREGATIONTYPE VARCHAR(31) ,OFFSET INTEGER  ,WINDOWSIZE INTEGER  ,LOOKBACK INTEGER  ,P_THRESHOLD INTEGER  ,N_THRESHOLD INTEGER  ,CLAUSE VARCHAR(31) ,PLACEHOLDERTYPE VARCHAR(31) ,GROUPING VARCHAR(31) ,REACTIVATEVIEWS INTEGER )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Busyhour");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Busyhour VALUES( 'testVERSIONID'  ,'testBHLEVEL'  ,'testBHTYPE'  ,'testBHCRITERIA'  ,'testWHERECLAUSE'  ,'testDESCRIPTION'  ,'testTARGETVERSIONID'  ,'testBHOBJECT'  ,1  ,1  ,'testAGGREGATIONTYPE'  ,1  ,1  ,1  ,1  ,1  ,'testCLAUSE'  ,'testPLACEHOLDERTYPE'  ,'testGROUPING'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Busyhour(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Busyhour");
    objUnderTest = null;
  }
  
  /**
   * Testing Busyhour constructor variable initialization with null values.
   */
  @Test
  public void testBusyhourConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Busyhour(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + BHLEVEL.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + BHCRITERIA.get(objUnderTest)  + ", " + WHERECLAUSE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + TARGETVERSIONID.get(objUnderTest)  + ", " + BHOBJECT.get(objUnderTest)  + ", " + BHELEMENT.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest)  + ", " + AGGREGATIONTYPE.get(objUnderTest)  + ", " + OFFSET.get(objUnderTest)  + ", " + WINDOWSIZE.get(objUnderTest)  + ", " + LOOKBACK.get(objUnderTest)  + ", " + P_THRESHOLD.get(objUnderTest)  + ", " + N_THRESHOLD.get(objUnderTest)  + ", " + CLAUSE.get(objUnderTest)  + ", " + PLACEHOLDERTYPE.get(objUnderTest)  + ", " + GROUPING.get(objUnderTest)  + ", " + REACTIVATEVIEWS.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Busyhour constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testBusyhourConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Busyhour(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT");

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + BHLEVEL.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + BHCRITERIA.get(objUnderTest)  + ", " + WHERECLAUSE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + TARGETVERSIONID.get(objUnderTest)  + ", " + BHOBJECT.get(objUnderTest)  + ", " + BHELEMENT.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest)  + ", " + AGGREGATIONTYPE.get(objUnderTest)  + ", " + OFFSET.get(objUnderTest)  + ", " + WINDOWSIZE.get(objUnderTest)  + ", " + LOOKBACK.get(objUnderTest)  + ", " + P_THRESHOLD.get(objUnderTest)  + ", " + N_THRESHOLD.get(objUnderTest)  + ", " + CLAUSE.get(objUnderTest)  + ", " + PLACEHOLDERTYPE.get(objUnderTest)  + ", " + GROUPING.get(objUnderTest)  + ", " + REACTIVATEVIEWS.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testBHLEVEL"  + ", testBHTYPE"  + ", testBHCRITERIA"  + ", testWHERECLAUSE"  + ", testDESCRIPTION"  + ", testTARGETVERSIONID"  + ", testBHOBJECT"  + ", 1"  + ", 1"  + ", testAGGREGATIONTYPE"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testCLAUSE"  + ", testPLACEHOLDERTYPE"  + ", testGROUPING"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testBusyhourConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Busyhour(null ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Busyhour constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testBusyhourConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Busyhour whereObject = new Busyhour(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Busyhour(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + BHLEVEL.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + BHCRITERIA.get(objUnderTest)  + ", " + WHERECLAUSE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + TARGETVERSIONID.get(objUnderTest)  + ", " + BHOBJECT.get(objUnderTest)  + ", " + BHELEMENT.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest)  + ", " + AGGREGATIONTYPE.get(objUnderTest)  + ", " + OFFSET.get(objUnderTest)  + ", " + WINDOWSIZE.get(objUnderTest)  + ", " + LOOKBACK.get(objUnderTest)  + ", " + P_THRESHOLD.get(objUnderTest)  + ", " + N_THRESHOLD.get(objUnderTest)  + ", " + CLAUSE.get(objUnderTest)  + ", " + PLACEHOLDERTYPE.get(objUnderTest)  + ", " + GROUPING.get(objUnderTest)  + ", " + REACTIVATEVIEWS.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testBHLEVEL"  + ", testBHTYPE"  + ", testBHCRITERIA"  + ", testWHERECLAUSE"  + ", testDESCRIPTION"  + ", testTARGETVERSIONID"  + ", testBHOBJECT"  + ", 1"  + ", 1"  + ", testAGGREGATIONTYPE"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testCLAUSE"  + ", testPLACEHOLDERTYPE"  + ", testGROUPING"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testBusyhourConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Busyhour whereObject = new Busyhour(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Busyhour(null, whereObject);
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
    assertEquals("Busyhour", objUnderTest.getTableName());
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
    Busyhour whereObject = new Busyhour(rockFactory);

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
    Busyhour whereObject = new Busyhour(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Busyhour");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Busyhour");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSIONID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSIONID FROM Busyhour");
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
    
    String expected = "<Busyhour VERSIONID=\"'testVERSIONID'\" BHLEVEL=\"'testBHLEVEL'\" BHTYPE=\"'testBHTYPE'\" BHCRITERIA=\"'testBHCRITERIA'\" WHERECLAUSE=\"'testWHERECLAUSE'\" DESCRIPTION=\"'testDESCRIPTION'\" TARGETVERSIONID=\"'testTARGETVERSIONID'\" BHOBJECT=\"'testBHOBJECT'\" BHELEMENT=\"1\" ENABLE=\"1\" AGGREGATIONTYPE=\"'testAGGREGATIONTYPE'\" OFFSET=\"1\" WINDOWSIZE=\"1\" LOOKBACK=\"1\" P_THRESHOLD=\"1\" N_THRESHOLD=\"1\" CLAUSE=\"'testCLAUSE'\" PLACEHOLDERTYPE=\"'testPLACEHOLDERTYPE'\" GROUPING=\"'testGROUPING'\" REACTIVATEVIEWS=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Busyhour VERSIONID=\"'testVERSIONID'\" BHLEVEL=\"'testBHLEVEL'\" BHTYPE=\"'testBHTYPE'\" BHCRITERIA=\"'testBHCRITERIA'\" WHERECLAUSE=\"'testWHERECLAUSE'\" DESCRIPTION=\"'testDESCRIPTION'\" TARGETVERSIONID=\"'testTARGETVERSIONID'\" BHOBJECT=\"'testBHOBJECT'\" BHELEMENT=\"1\" ENABLE=\"1\" AGGREGATIONTYPE=\"'testAGGREGATIONTYPE'\" OFFSET=\"1\" WINDOWSIZE=\"1\" LOOKBACK=\"1\" P_THRESHOLD=\"1\" N_THRESHOLD=\"1\" CLAUSE=\"'testCLAUSE'\" PLACEHOLDERTYPE=\"'testPLACEHOLDERTYPE'\" GROUPING=\"'testGROUPING'\" REACTIVATEVIEWS=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Busyhour>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Busyhour ( VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, OFFSET, WINDOWSIZE, LOOKBACK, P_THRESHOLD, N_THRESHOLD, CLAUSE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS ) values "
      + "( 'testVERSIONID', 'testBHLEVEL', 'testBHTYPE', 'testBHCRITERIA', 'testWHERECLAUSE', 'testDESCRIPTION', 'testTARGETVERSIONID', 'testBHOBJECT', 1, 1, 'testAGGREGATIONTYPE', 1, 1, 1, 1, 1, 'testCLAUSE', 'testPLACEHOLDERTYPE', 'testGROUPING', 1 );\n";
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
    objUnderTest.setVersionid(BusyhourTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(BusyhourTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhlevel() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhlevel(BusyhourTest.testStringGenerator("anotherBHLEVEL", 255));
    assertEquals(BusyhourTest.testStringGenerator("anotherBHLEVEL", 255), BHLEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhtype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhtype(BusyhourTest.testStringGenerator("anotherBHTYPE", 32));
    assertEquals(BusyhourTest.testStringGenerator("anotherBHTYPE", 32), BHTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhcriteria() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhcriteria(BusyhourTest.testStringGenerator("anotherBHCRITERIA", 32000));
    assertEquals(BusyhourTest.testStringGenerator("anotherBHCRITERIA", 32000), BHCRITERIA.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetWhereclause() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setWhereclause(BusyhourTest.testStringGenerator("anotherWHERECLAUSE", 32000));
    assertEquals(BusyhourTest.testStringGenerator("anotherWHERECLAUSE", 32000), WHERECLAUSE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(BusyhourTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(BusyhourTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTargetversionid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTargetversionid(BusyhourTest.testStringGenerator("anotherTARGETVERSIONID", 128));
    assertEquals(BusyhourTest.testStringGenerator("anotherTARGETVERSIONID", 128), TARGETVERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhobject() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhobject(BusyhourTest.testStringGenerator("anotherBHOBJECT", 32));
    assertEquals(BusyhourTest.testStringGenerator("anotherBHOBJECT", 32), BHOBJECT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhelement() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhelement(555);
    assertEquals(555, BHELEMENT.get(objUnderTest));
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
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetAggregationtype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setAggregationtype(BusyhourTest.testStringGenerator("anotherAGGREGATIONTYPE", 128));
    assertEquals(BusyhourTest.testStringGenerator("anotherAGGREGATIONTYPE", 128), AGGREGATIONTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetOffset() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setOffset(555);
    assertEquals(555, OFFSET.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetWindowsize() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setWindowsize(555);
    assertEquals(555, WINDOWSIZE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLookback() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLookback(555);
    assertEquals(555, LOOKBACK.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetP_threshold() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setP_threshold(555);
    assertEquals(555, P_THRESHOLD.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetN_threshold() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setN_threshold(555);
    assertEquals(555, N_THRESHOLD.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetClause() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setClause(BusyhourTest.testStringGenerator("anotherCLAUSE", 32000));
    assertEquals(BusyhourTest.testStringGenerator("anotherCLAUSE", 32000), CLAUSE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPlaceholdertype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPlaceholdertype(BusyhourTest.testStringGenerator("anotherPLACEHOLDERTYPE", 128));
    assertEquals(BusyhourTest.testStringGenerator("anotherPLACEHOLDERTYPE", 128), PLACEHOLDERTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetGrouping() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setGrouping(BusyhourTest.testStringGenerator("anotherGROUPING", 32));
    assertEquals(BusyhourTest.testStringGenerator("anotherGROUPING", 32), GROUPING.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetReactivateviews() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setReactivateviews(555);
    assertEquals(555, REACTIVATEVIEWS.get(objUnderTest));
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
    String[] expectedKeys = { "VERSIONID","BHLEVEL","BHTYPE","TARGETVERSIONID","BHOBJECT"};

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
    objUnderTest = new Busyhour(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSIONID.get(objUnderTest)  + ", " + BHLEVEL.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + BHCRITERIA.get(objUnderTest)  + ", " + WHERECLAUSE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + TARGETVERSIONID.get(objUnderTest)  + ", " + BHOBJECT.get(objUnderTest)  + ", " + BHELEMENT.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest)  + ", " + AGGREGATIONTYPE.get(objUnderTest)  + ", " + OFFSET.get(objUnderTest)  + ", " + WINDOWSIZE.get(objUnderTest)  + ", " + LOOKBACK.get(objUnderTest)  + ", " + P_THRESHOLD.get(objUnderTest)  + ", " + N_THRESHOLD.get(objUnderTest)  + ", " + CLAUSE.get(objUnderTest)  + ", " + PLACEHOLDERTYPE.get(objUnderTest)  + ", " + GROUPING.get(objUnderTest)  + ", " + REACTIVATEVIEWS.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", "  + ", "  + ", 0" ;
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
    Busyhour compareObj = new Busyhour(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT");

    /* Testing first with null primary key value */
    VERSIONID.set(objUnderTest, null);
  	BHLEVEL.set(objUnderTest, null);
  	BHTYPE.set(objUnderTest, null);
  	TARGETVERSIONID.set(objUnderTest, null);
  	BHOBJECT.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSIONID.set(objUnderTest,  "different");
  	BHLEVEL.set(objUnderTest,  "different");
  	BHTYPE.set(objUnderTest,  "different");
  	TARGETVERSIONID.set(objUnderTest,  "different");
  	BHOBJECT.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSIONID.set(objUnderTest,  "testVERSIONID");
  	BHLEVEL.set(objUnderTest,  "testBHLEVEL");
  	BHTYPE.set(objUnderTest,  "testBHTYPE");
  	TARGETVERSIONID.set(objUnderTest,  "testTARGETVERSIONID");
  	BHOBJECT.set(objUnderTest,  "testBHOBJECT");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Busyhour with our current one. If the two
   * Busyhours are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnBusyhour() throws Exception {

    /* Creating another Busyhour which will be compared to the tested one */
    Busyhour comparedObj = new Busyhour(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Busyhour with our current one. If the two
   * Busyhours are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentBusyhour() throws Exception {

    /* Creating another Busyhour which will be compared to the tested one */
    Busyhour comparedObj = new Busyhour(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT");
    comparedObj.setVersionid( "DifferentVERSIONID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Busyhour with our current one. If the two
   * Busyhours are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameBusyhour() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Busyhour comparedObj = new Busyhour(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Busyhour with our current one using null value.
   */
  @Test
  public void testEqualsWithNullBusyhour() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Busyhour comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Busyhour was null \n");
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
    assertEquals(Busyhour.class, actualObject.getClass());
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
    Busyhour testAgg = new Busyhour(rockFactory ,  "testVERSIONID",  "testBHLEVEL",  "testBHTYPE",  "testTARGETVERSIONID",  "testBHOBJECT");
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
   * Testing columnsize retrieving for Bhcriteria.
   */
  @Test
  public void testGetBhcriteriaColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getBhcriteriaColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Bhcriteria.
  */
  @Test
  public void testGetBhcriteriaDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBhcriteriaDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Bhcriteria.
  */
  @Test
  public void testGetBhcriteriaSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBhcriteriaSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Whereclause.
   */
  @Test
  public void testGetWhereclauseColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getWhereclauseColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Whereclause.
  */
  @Test
  public void testGetWhereclauseDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getWhereclauseDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Whereclause.
  */
  @Test
  public void testGetWhereclauseSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getWhereclauseSQLType());    
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
   * Testing columnsize retrieving for Bhelement.
   */
  @Test
  public void testGetBhelementColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getBhelementColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Bhelement.
  */
  @Test
  public void testGetBhelementDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBhelementDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Bhelement.
  */
  @Test
  public void testGetBhelementSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getBhelementSQLType());    
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
   * Testing columnsize retrieving for Aggregationtype.
   */
  @Test
  public void testGetAggregationtypeColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getAggregationtypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Aggregationtype.
  */
  @Test
  public void testGetAggregationtypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getAggregationtypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Aggregationtype.
  */
  @Test
  public void testGetAggregationtypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getAggregationtypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Offset.
   */
  @Test
  public void testGetOffsetColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getOffsetColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Offset.
  */
  @Test
  public void testGetOffsetDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getOffsetDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Offset.
  */
  @Test
  public void testGetOffsetSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getOffsetSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Windowsize.
   */
  @Test
  public void testGetWindowsizeColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getWindowsizeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Windowsize.
  */
  @Test
  public void testGetWindowsizeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getWindowsizeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Windowsize.
  */
  @Test
  public void testGetWindowsizeSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getWindowsizeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Lookback.
   */
  @Test
  public void testGetLookbackColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getLookbackColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Lookback.
  */
  @Test
  public void testGetLookbackDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLookbackDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Lookback.
  */
  @Test
  public void testGetLookbackSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getLookbackSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for P_threshold.
   */
  @Test
  public void testGetP_thresholdColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getP_thresholdColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for P_threshold.
  */
  @Test
  public void testGetP_thresholdDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getP_thresholdDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for P_threshold.
  */
  @Test
  public void testGetP_thresholdSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getP_thresholdSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for N_threshold.
   */
  @Test
  public void testGetN_thresholdColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getN_thresholdColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for N_threshold.
  */
  @Test
  public void testGetN_thresholdDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getN_thresholdDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for N_threshold.
  */
  @Test
  public void testGetN_thresholdSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getN_thresholdSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Clause.
   */
  @Test
  public void testGetClauseColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getClauseColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Clause.
  */
  @Test
  public void testGetClauseDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getClauseDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Clause.
  */
  @Test
  public void testGetClauseSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getClauseSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Placeholdertype.
   */
  @Test
  public void testGetPlaceholdertypeColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getPlaceholdertypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Placeholdertype.
  */
  @Test
  public void testGetPlaceholdertypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPlaceholdertypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Placeholdertype.
  */
  @Test
  public void testGetPlaceholdertypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPlaceholdertypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Grouping.
   */
  @Test
  public void testGetGroupingColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getGroupingColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Grouping.
  */
  @Test
  public void testGetGroupingDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getGroupingDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Grouping.
  */
  @Test
  public void testGetGroupingSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getGroupingSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Reactivateviews.
   */
  @Test
  public void testGetReactivateviewsColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getReactivateviewsColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Reactivateviews.
  */
  @Test
  public void testGetReactivateviewsDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getReactivateviewsDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Reactivateviews.
  */
  @Test
  public void testGetReactivateviewsSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getReactivateviewsSQLType());    
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
   * Testing original Busyhour object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Busyhour(rockFactory, false);
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
    Busyhour changedOriginal = new Busyhour(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Busyhour(rockFactory, false);
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
    Busyhour changedOriginal = new Busyhour(rockFactory, false);
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