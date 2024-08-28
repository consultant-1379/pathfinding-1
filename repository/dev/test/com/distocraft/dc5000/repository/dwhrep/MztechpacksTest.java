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
 * Test class for Mztechpacks. Changes to Mztechpacks table are made via
 * this class.
 */
public class MztechpacksTest {

  private static Mztechpacks objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSIONID;
  
  private static Field TECHPACK_NAME;
  
  private static Field STATUS;
  
  private static Field CREATIONDATE;
  
  private static Field PRODUCT_NUMBER;
  
  private static Field TYPE;
  
  private static Field TECHPACK_VERSION;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Mztechpacks.class.getDeclaredField("newItem");
		VERSIONID = Mztechpacks.class.getDeclaredField("VERSIONID");
		TECHPACK_NAME = Mztechpacks.class.getDeclaredField("TECHPACK_NAME");
		STATUS = Mztechpacks.class.getDeclaredField("STATUS");
		CREATIONDATE = Mztechpacks.class.getDeclaredField("CREATIONDATE");
		PRODUCT_NUMBER = Mztechpacks.class.getDeclaredField("PRODUCT_NUMBER");
		TYPE = Mztechpacks.class.getDeclaredField("TYPE");
		TECHPACK_VERSION = Mztechpacks.class.getDeclaredField("TECHPACK_VERSION");
		newItem.setAccessible(true);
		VERSIONID.setAccessible(true);
		TECHPACK_NAME.setAccessible(true);
		STATUS.setAccessible(true);
		CREATIONDATE.setAccessible(true);
		PRODUCT_NUMBER.setAccessible(true);
		TYPE.setAccessible(true);
		TECHPACK_VERSION.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Mztechpacks ( VERSIONID VARCHAR(128)  ,TECHPACK_NAME VARCHAR(30) ,STATUS VARCHAR(10) ,CREATIONDATE TIMESTAMP  ,PRODUCT_NUMBER VARCHAR(255) ,TYPE VARCHAR(10) ,TECHPACK_VERSION VARCHAR(32))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Mztechpacks");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Mztechpacks VALUES(  'testVERSIONID'   ,  'testTECHPACK_NAME'   ,  'testSTATUS'  ,'2000-01-01 00:00:00.0'  ,  'testPRODUCT_NUMBER'   ,  'testTYPE'   ,  'testTECHPACK_VERSION'  )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Mztechpacks(rockFactory ,   "testVERSIONID" ,   "testTECHPACK_VERSION" );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Mztechpacks");
    objUnderTest = null;
  }
  
  /**
   * Testing Mztechpacks constructor variable initialization with null values.
   */
  @Test
  public void testMztechpacksConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Mztechpacks(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + TECHPACK_NAME.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + CREATIONDATE.get(objUnderTest)  + ", " + PRODUCT_NUMBER.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + TECHPACK_VERSION.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Mztechpacks constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMztechpacksConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Mztechpacks(rockFactory ,  "testVERSIONID",  "testTECHPACK_VERSION");

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + TECHPACK_NAME.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + CREATIONDATE.get(objUnderTest)  + ", " + PRODUCT_NUMBER.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + TECHPACK_VERSION.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testTECHPACK_NAME"  + ", testSTATUS"  + ", 2000-01-01 00:00:00.0"  + ", testPRODUCT_NUMBER"  + ", testTYPE"  + ", testTECHPACK_VERSION" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMztechpacksConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Mztechpacks(null ,  "testVERSIONID",  "testTECHPACK_VERSION");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Mztechpacks constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMztechpacksConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Mztechpacks whereObject = new Mztechpacks(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Mztechpacks(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + TECHPACK_NAME.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + CREATIONDATE.get(objUnderTest)  + ", " + PRODUCT_NUMBER.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + TECHPACK_VERSION.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testTECHPACK_NAME"  + ", testSTATUS"  + ", 2000-01-01 00:00:00.0"  + ", testPRODUCT_NUMBER"  + ", testTYPE"  + ", testTECHPACK_VERSION" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMztechpacksConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Mztechpacks whereObject = new Mztechpacks(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Mztechpacks(null, whereObject);
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
    assertEquals("Mztechpacks", objUnderTest.getTableName());
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
    Mztechpacks whereObject = new Mztechpacks(rockFactory);

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
    Mztechpacks whereObject = new Mztechpacks(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Mztechpacks");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Mztechpacks");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSIONID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSIONID FROM Mztechpacks");
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
    
    String expected = "<Mztechpacks VERSIONID=\"'testVERSIONID'\" TECHPACK_NAME=\"'testTECHPACK_NAME'\" STATUS=\"'testSTATUS'\" CREATIONDATE=\"'2000-01-01 00:00:00.0'\" PRODUCT_NUMBER=\"'testPRODUCT_NUMBER'\" TYPE=\"'testTYPE'\" TECHPACK_VERSION=\"'testTECHPACK_VERSION'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Mztechpacks VERSIONID=\"'testVERSIONID'\" TECHPACK_NAME=\"'testTECHPACK_NAME'\" STATUS=\"'testSTATUS'\" CREATIONDATE=\"'2000-01-01 00:00:00.0'\" PRODUCT_NUMBER=\"'testPRODUCT_NUMBER'\" TYPE=\"'testTYPE'\" TECHPACK_VERSION=\"'testTECHPACK_VERSION'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Mztechpacks>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Mztechpacks ( VERSIONID, TECHPACK_NAME, STATUS, CREATIONDATE, PRODUCT_NUMBER, TYPE, TECHPACK_VERSION ) values "
      + "( 'testVERSIONID', 'testTECHPACK_NAME', 'testSTATUS', '2000-01-01 00:00:00.0', 'testPRODUCT_NUMBER', 'testTYPE', 'testTECHPACK_VERSION' );\n";
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
    objUnderTest.setVersionid(MztechpacksTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(MztechpacksTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTechpack_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTechpack_name(MztechpacksTest.testStringGenerator("anotherTECHPACK_NAME", 30));
    assertEquals(MztechpacksTest.testStringGenerator("anotherTECHPACK_NAME", 30), TECHPACK_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatus() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatus(MztechpacksTest.testStringGenerator("anotherSTATUS", 10));
    assertEquals(MztechpacksTest.testStringGenerator("anotherSTATUS", 10), STATUS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCreationdate() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCreationdate(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), CREATIONDATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetProduct_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setProduct_number(MztechpacksTest.testStringGenerator("anotherPRODUCT_NUMBER", 255));
    assertEquals(MztechpacksTest.testStringGenerator("anotherPRODUCT_NUMBER", 255), PRODUCT_NUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetType() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setType(MztechpacksTest.testStringGenerator("anotherTYPE", 10));
    assertEquals(MztechpacksTest.testStringGenerator("anotherTYPE", 10), TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTechpack_version() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTechpack_version(MztechpacksTest.testStringGenerator("anotherTECHPACK_VERSION", 32));
    assertEquals(MztechpacksTest.testStringGenerator("anotherTECHPACK_VERSION", 32), TECHPACK_VERSION.get(objUnderTest));
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
    String[] expectedKeys = { "VERSIONID","TECHPACK_VERSION"};

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
    objUnderTest = new Mztechpacks(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSIONID.get(objUnderTest)  + ", " + TECHPACK_NAME.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + CREATIONDATE.get(objUnderTest)  + ", " + PRODUCT_NUMBER.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + TECHPACK_VERSION.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", " + new Timestamp(0)  + ", "  + ", "  + ", " ;
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
    Mztechpacks compareObj = new Mztechpacks(rockFactory ,  "testVERSIONID",  "testTECHPACK_VERSION");

    /* Testing first with null primary key value */
    VERSIONID.set(objUnderTest, null);
  	TECHPACK_VERSION.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSIONID.set(objUnderTest,  "different");
  	TECHPACK_VERSION.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSIONID.set(objUnderTest,  "testVERSIONID");
  	TECHPACK_VERSION.set(objUnderTest,  "testTECHPACK_VERSION");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Mztechpacks with our current one. If the two
   * Mztechpackss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMztechpacks() throws Exception {

    /* Creating another Mztechpacks which will be compared to the tested one */
    Mztechpacks comparedObj = new Mztechpacks(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Mztechpacks with our current one. If the two
   * Mztechpackss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMztechpacks() throws Exception {

    /* Creating another Mztechpacks which will be compared to the tested one */
    Mztechpacks comparedObj = new Mztechpacks(rockFactory ,  "testVERSIONID",  "testTECHPACK_VERSION");
    comparedObj.setVersionid( "DifferentVERSIONID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Mztechpacks with our current one. If the two
   * Mztechpackss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMztechpacks() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Mztechpacks comparedObj = new Mztechpacks(rockFactory ,  "testVERSIONID",  "testTECHPACK_VERSION");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Mztechpacks with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMztechpacks() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Mztechpacks comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Mztechpacks was null \n");
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
    assertEquals(Mztechpacks.class, actualObject.getClass());
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
    Mztechpacks testAgg = new Mztechpacks(rockFactory ,  "testVERSIONID",  "testTECHPACK_VERSION");
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
   * Testing columnsize retrieving for Techpack_name.
   */
  @Test
  public void testGetTechpack_nameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getTechpack_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Techpack_name.
  */
  @Test
  public void testGetTechpack_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTechpack_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Techpack_name.
  */
  @Test
  public void testGetTechpack_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTechpack_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Status.
   */
  @Test
  public void testGetStatusColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getStatusColumnSize());   
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
    
    assertEquals(12, objUnderTest.getStatusSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Creationdate.
   */
  @Test
  public void testGetCreationdateColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getCreationdateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Creationdate.
  */
  @Test
  public void testGetCreationdateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCreationdateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Creationdate.
  */
  @Test
  public void testGetCreationdateSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getCreationdateSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Product_number.
   */
  @Test
  public void testGetProduct_numberColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getProduct_numberColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Product_number.
  */
  @Test
  public void testGetProduct_numberDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getProduct_numberDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Product_number.
  */
  @Test
  public void testGetProduct_numberSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getProduct_numberSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Type.
   */
  @Test
  public void testGetTypeColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getTypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Type.
  */
  @Test
  public void testGetTypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Type.
  */
  @Test
  public void testGetTypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Techpack_version.
   */
  @Test
  public void testGetTechpack_versionColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getTechpack_versionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Techpack_version.
  */
  @Test
  public void testGetTechpack_versionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTechpack_versionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Techpack_version.
  */
  @Test
  public void testGetTechpack_versionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTechpack_versionSQLType());    
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
   * Testing original Mztechpacks object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Mztechpacks(rockFactory, false);
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
    Mztechpacks changedOriginal = new Mztechpacks(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Mztechpacks(rockFactory, false);
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
    Mztechpacks changedOriginal = new Mztechpacks(rockFactory, false);
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