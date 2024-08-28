package com.distocraft.dc5000.etl.rock;

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
 * Test class for Meta_collections. Changes to Meta_collections table are made via
 * this class.
 */
public class Meta_collectionsTest {

  private static Meta_collections objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field COLLECTION_ID;
  
  private static Field COLLECTION_NAME;
  
  private static Field COLLECTION;
  
  private static Field MAIL_ERROR_ADDR;
  
  private static Field MAIL_FAIL_ADDR;
  
  private static Field MAIL_BUG_ADDR;
  
  private static Field MAX_ERRORS;
  
  private static Field MAX_FK_ERRORS;
  
  private static Field MAX_COL_LIMIT_ERRORS;
  
  private static Field CHECK_FK_ERROR_FLAG;
  
  private static Field CHECK_COL_LIMITS_FLAG;
  
  private static Field LAST_TRANSFER_DATE;
  
  private static Field VERSION_NUMBER;
  
  private static Field COLLECTION_SET_ID;
  
  private static Field USE_BATCH_ID;
  
  private static Field PRIORITY;
  
  private static Field QUEUE_TIME_LIMIT;
  
  private static Field ENABLED_FLAG;
  
  private static Field SETTYPE;
  
  private static Field FOLDABLE_FLAG;
  
  private static Field MEASTYPE;
  
  private static Field HOLD_FLAG;
  
  private static Field SCHEDULING_INFO;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_collections.class.getDeclaredField("newItem");
		COLLECTION_ID = Meta_collections.class.getDeclaredField("COLLECTION_ID");
		COLLECTION_NAME = Meta_collections.class.getDeclaredField("COLLECTION_NAME");
		COLLECTION = Meta_collections.class.getDeclaredField("COLLECTION");
		MAIL_ERROR_ADDR = Meta_collections.class.getDeclaredField("MAIL_ERROR_ADDR");
		MAIL_FAIL_ADDR = Meta_collections.class.getDeclaredField("MAIL_FAIL_ADDR");
		MAIL_BUG_ADDR = Meta_collections.class.getDeclaredField("MAIL_BUG_ADDR");
		MAX_ERRORS = Meta_collections.class.getDeclaredField("MAX_ERRORS");
		MAX_FK_ERRORS = Meta_collections.class.getDeclaredField("MAX_FK_ERRORS");
		MAX_COL_LIMIT_ERRORS = Meta_collections.class.getDeclaredField("MAX_COL_LIMIT_ERRORS");
		CHECK_FK_ERROR_FLAG = Meta_collections.class.getDeclaredField("CHECK_FK_ERROR_FLAG");
		CHECK_COL_LIMITS_FLAG = Meta_collections.class.getDeclaredField("CHECK_COL_LIMITS_FLAG");
		LAST_TRANSFER_DATE = Meta_collections.class.getDeclaredField("LAST_TRANSFER_DATE");
		VERSION_NUMBER = Meta_collections.class.getDeclaredField("VERSION_NUMBER");
		COLLECTION_SET_ID = Meta_collections.class.getDeclaredField("COLLECTION_SET_ID");
		USE_BATCH_ID = Meta_collections.class.getDeclaredField("USE_BATCH_ID");
		PRIORITY = Meta_collections.class.getDeclaredField("PRIORITY");
		QUEUE_TIME_LIMIT = Meta_collections.class.getDeclaredField("QUEUE_TIME_LIMIT");
		ENABLED_FLAG = Meta_collections.class.getDeclaredField("ENABLED_FLAG");
		SETTYPE = Meta_collections.class.getDeclaredField("SETTYPE");
		FOLDABLE_FLAG = Meta_collections.class.getDeclaredField("FOLDABLE_FLAG");
		MEASTYPE = Meta_collections.class.getDeclaredField("MEASTYPE");
		HOLD_FLAG = Meta_collections.class.getDeclaredField("HOLD_FLAG");
		SCHEDULING_INFO = Meta_collections.class.getDeclaredField("SCHEDULING_INFO");
		newItem.setAccessible(true);
		COLLECTION_ID.setAccessible(true);
		COLLECTION_NAME.setAccessible(true);
		COLLECTION.setAccessible(true);
		MAIL_ERROR_ADDR.setAccessible(true);
		MAIL_FAIL_ADDR.setAccessible(true);
		MAIL_BUG_ADDR.setAccessible(true);
		MAX_ERRORS.setAccessible(true);
		MAX_FK_ERRORS.setAccessible(true);
		MAX_COL_LIMIT_ERRORS.setAccessible(true);
		CHECK_FK_ERROR_FLAG.setAccessible(true);
		CHECK_COL_LIMITS_FLAG.setAccessible(true);
		LAST_TRANSFER_DATE.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		COLLECTION_SET_ID.setAccessible(true);
		USE_BATCH_ID.setAccessible(true);
		PRIORITY.setAccessible(true);
		QUEUE_TIME_LIMIT.setAccessible(true);
		ENABLED_FLAG.setAccessible(true);
		SETTYPE.setAccessible(true);
		FOLDABLE_FLAG.setAccessible(true);
		MEASTYPE.setAccessible(true);
		HOLD_FLAG.setAccessible(true);
		SCHEDULING_INFO.setAccessible(true);
	  timeStampName = Meta_collections.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_collections ( COLLECTION_ID BIGINT  ,COLLECTION_NAME VARCHAR(31) ,COLLECTION VARCHAR(31) ,MAIL_ERROR_ADDR VARCHAR(31) ,MAIL_FAIL_ADDR VARCHAR(31) ,MAIL_BUG_ADDR VARCHAR(31) ,MAX_ERRORS BIGINT  ,MAX_FK_ERRORS BIGINT  ,MAX_COL_LIMIT_ERRORS BIGINT  ,CHECK_FK_ERROR_FLAG VARCHAR(31) ,CHECK_COL_LIMITS_FLAG VARCHAR(31) ,LAST_TRANSFER_DATE TIMESTAMP  ,VERSION_NUMBER VARCHAR(31) ,COLLECTION_SET_ID BIGINT  ,USE_BATCH_ID VARCHAR(31) ,PRIORITY BIGINT  ,QUEUE_TIME_LIMIT BIGINT  ,ENABLED_FLAG VARCHAR(31) ,SETTYPE VARCHAR(31) ,FOLDABLE_FLAG VARCHAR(31) ,MEASTYPE VARCHAR(31) ,HOLD_FLAG VARCHAR(31) ,SCHEDULING_INFO VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_collections");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_collections VALUES( 1  ,'testCOLLECTION_NAME'  ,'testCOLLECTION'  ,'testMAIL_ERROR_ADDR'  ,'testMAIL_FAIL_ADDR'  ,'testMAIL_BUG_ADDR'  ,1  ,1  ,1  ,'testCHECK_FK_ERROR_FLAG'  ,'testCHECK_COL_LIMITS_FLAG'  ,'2000-01-01 00:00:00.0'  ,'testVERSION_NUMBER'  ,1  ,'testUSE_BATCH_ID'  ,1  ,1  ,'testENABLED_FLAG'  ,'testSETTYPE'  ,'testFOLDABLE_FLAG'  ,'testMEASTYPE'  ,'testHOLD_FLAG'  ,'testSCHEDULING_INFO' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_collections(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_collections");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_collections constructor variable initialization with null values.
   */
  @Test
  public void testMeta_collectionsConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_collections(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  COLLECTION_ID.get(objUnderTest)  + ", " + COLLECTION_NAME.get(objUnderTest)  + ", " + COLLECTION.get(objUnderTest)  + ", " + MAIL_ERROR_ADDR.get(objUnderTest)  + ", " + MAIL_FAIL_ADDR.get(objUnderTest)  + ", " + MAIL_BUG_ADDR.get(objUnderTest)  + ", " + MAX_ERRORS.get(objUnderTest)  + ", " + MAX_FK_ERRORS.get(objUnderTest)  + ", " + MAX_COL_LIMIT_ERRORS.get(objUnderTest)  + ", " + CHECK_FK_ERROR_FLAG.get(objUnderTest)  + ", " + CHECK_COL_LIMITS_FLAG.get(objUnderTest)  + ", " + LAST_TRANSFER_DATE.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + USE_BATCH_ID.get(objUnderTest)  + ", " + PRIORITY.get(objUnderTest)  + ", " + QUEUE_TIME_LIMIT.get(objUnderTest)  + ", " + ENABLED_FLAG.get(objUnderTest)  + ", " + SETTYPE.get(objUnderTest)  + ", " + FOLDABLE_FLAG.get(objUnderTest)  + ", " + MEASTYPE.get(objUnderTest)  + ", " + HOLD_FLAG.get(objUnderTest)  + ", " + SCHEDULING_INFO.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_collections constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_collectionsConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_collections(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );

    /* Asserting that variables are initialized */
    String actual =  COLLECTION_ID.get(objUnderTest)  + ", " + COLLECTION_NAME.get(objUnderTest)  + ", " + COLLECTION.get(objUnderTest)  + ", " + MAIL_ERROR_ADDR.get(objUnderTest)  + ", " + MAIL_FAIL_ADDR.get(objUnderTest)  + ", " + MAIL_BUG_ADDR.get(objUnderTest)  + ", " + MAX_ERRORS.get(objUnderTest)  + ", " + MAX_FK_ERRORS.get(objUnderTest)  + ", " + MAX_COL_LIMIT_ERRORS.get(objUnderTest)  + ", " + CHECK_FK_ERROR_FLAG.get(objUnderTest)  + ", " + CHECK_COL_LIMITS_FLAG.get(objUnderTest)  + ", " + LAST_TRANSFER_DATE.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + USE_BATCH_ID.get(objUnderTest)  + ", " + PRIORITY.get(objUnderTest)  + ", " + QUEUE_TIME_LIMIT.get(objUnderTest)  + ", " + ENABLED_FLAG.get(objUnderTest)  + ", " + SETTYPE.get(objUnderTest)  + ", " + FOLDABLE_FLAG.get(objUnderTest)  + ", " + MEASTYPE.get(objUnderTest)  + ", " + HOLD_FLAG.get(objUnderTest)  + ", " + SCHEDULING_INFO.get(objUnderTest) ;
    String expected =  "1"  + ", testCOLLECTION_NAME"  + ", testCOLLECTION"  + ", testMAIL_ERROR_ADDR"  + ", testMAIL_FAIL_ADDR"  + ", testMAIL_BUG_ADDR"  + ", 1"  + ", 1"  + ", 1"  + ", testCHECK_FK_ERROR_FLAG"  + ", testCHECK_COL_LIMITS_FLAG"  + ", 2000-01-01 00:00:00.0"  + ", testVERSION_NUMBER"  + ", 1"  + ", testUSE_BATCH_ID"  + ", 1"  + ", 1"  + ", testENABLED_FLAG"  + ", testSETTYPE"  + ", testFOLDABLE_FLAG"  + ", testMEASTYPE"  + ", testHOLD_FLAG"  + ", testSCHEDULING_INFO" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_collectionsConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_collections(null ,  1L ,  "testVERSION_NUMBER",  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_collections constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_collectionsConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_collections whereObject = new Meta_collections(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_collections(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  COLLECTION_ID.get(objUnderTest)  + ", " + COLLECTION_NAME.get(objUnderTest)  + ", " + COLLECTION.get(objUnderTest)  + ", " + MAIL_ERROR_ADDR.get(objUnderTest)  + ", " + MAIL_FAIL_ADDR.get(objUnderTest)  + ", " + MAIL_BUG_ADDR.get(objUnderTest)  + ", " + MAX_ERRORS.get(objUnderTest)  + ", " + MAX_FK_ERRORS.get(objUnderTest)  + ", " + MAX_COL_LIMIT_ERRORS.get(objUnderTest)  + ", " + CHECK_FK_ERROR_FLAG.get(objUnderTest)  + ", " + CHECK_COL_LIMITS_FLAG.get(objUnderTest)  + ", " + LAST_TRANSFER_DATE.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + USE_BATCH_ID.get(objUnderTest)  + ", " + PRIORITY.get(objUnderTest)  + ", " + QUEUE_TIME_LIMIT.get(objUnderTest)  + ", " + ENABLED_FLAG.get(objUnderTest)  + ", " + SETTYPE.get(objUnderTest)  + ", " + FOLDABLE_FLAG.get(objUnderTest)  + ", " + MEASTYPE.get(objUnderTest)  + ", " + HOLD_FLAG.get(objUnderTest)  + ", " + SCHEDULING_INFO.get(objUnderTest) ;
    String expected =  "1"  + ", testCOLLECTION_NAME"  + ", testCOLLECTION"  + ", testMAIL_ERROR_ADDR"  + ", testMAIL_FAIL_ADDR"  + ", testMAIL_BUG_ADDR"  + ", 1"  + ", 1"  + ", 1"  + ", testCHECK_FK_ERROR_FLAG"  + ", testCHECK_COL_LIMITS_FLAG"  + ", 2000-01-01 00:00:00.0"  + ", testVERSION_NUMBER"  + ", 1"  + ", testUSE_BATCH_ID"  + ", 1"  + ", 1"  + ", testENABLED_FLAG"  + ", testSETTYPE"  + ", testFOLDABLE_FLAG"  + ", testMEASTYPE"  + ", testHOLD_FLAG"  + ", testSCHEDULING_INFO" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_collectionsConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_collections whereObject = new Meta_collections(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_collections(null, whereObject);
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
    assertEquals("Meta_collections", objUnderTest.getTableName());
  }
  
  /**
   * Testing database updating. Affected row count is returned.
   */
  @Test
  public void testUpdateDB() throws Exception {
    /**/
  	timeStampName.set(objUnderTest, "");

    /* Invoking tested method and asserting the update has been made */
    String actual = objUnderTest.updateDB() + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + false, actual);
  }
  
  /**
   * Testing database updating. Affected row count is returned.
   */
  @Test
  public void testUpdateDBwithTimestamp() throws Exception {
    /**/
  	timeStampName.set(objUnderTest, "");

    /* Invoking tested method and asserting the update has been made */
    String actual = objUnderTest.updateDB(true) + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + false, actual);
  }
  
  /**
   * Testing database updating. Affected row count is returned.
   */
  @Test
  public void testUpdateDBWithConstructedWhereClause() throws Exception {
    /**/
  	timeStampName.set(objUnderTest, "");

    /* Creating where object which tells what sort of query is to be done */
    Meta_collections whereObject = new Meta_collections(rockFactory);

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
    Meta_collections whereObject = new Meta_collections(rockFactory);

    /* Invoking tested method and asserting the delete has been made */
    String actual = objUnderTest.deleteDB(whereObject) + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + true, actual);
  }
  
  /**
   * Testing data saving to the database.
   */
  @Test
  public void testSaveDB() throws Exception {

    /**/
  	timeStampName.set(objUnderTest, "");

    /* Calling the tested method twice with different setting */
    objUnderTest.saveDB();
    newItem.set(objUnderTest, true);
    objUnderTest.saveDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_collections");
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
    /**/
  	timeStampName.set(objUnderTest, "");

    /* Calling the tested method twice, first insert, next update */
    newItem.set(objUnderTest, true);
    objUnderTest.saveToDB();
    COLLECTION_ID.set(objUnderTest, 1L);
    HashSet testSet = new HashSet();
    testSet.add("COLLECTION_ID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_collections");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the COLLECTION_ID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT COLLECTION_ID FROM Meta_collections");
    while (res.next()) {
      queryResult = res.getString(1);
    }

    /* Invoking tested method and asserting the data has been saved */
    String actual = rows + ", " + queryResult + ", " + newItem.get(objUnderTest);
    assertEquals("2, 1, " + false, actual);
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_tag() throws Exception {
    
    String expected = "<Meta_collections COLLECTION_ID=\"1\" COLLECTION_NAME=\"'testCOLLECTION_NAME'\" COLLECTION=\"'testCOLLECTION'\" MAIL_ERROR_ADDR=\"'testMAIL_ERROR_ADDR'\" MAIL_FAIL_ADDR=\"'testMAIL_FAIL_ADDR'\" MAIL_BUG_ADDR=\"'testMAIL_BUG_ADDR'\" MAX_ERRORS=\"1\" MAX_FK_ERRORS=\"1\" MAX_COL_LIMIT_ERRORS=\"1\" CHECK_FK_ERROR_FLAG=\"'testCHECK_FK_ERROR_FLAG'\" CHECK_COL_LIMITS_FLAG=\"'testCHECK_COL_LIMITS_FLAG'\" LAST_TRANSFER_DATE=\"'2000-01-01 00:00:00.0'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" COLLECTION_SET_ID=\"1\" USE_BATCH_ID=\"'testUSE_BATCH_ID'\" PRIORITY=\"1\" QUEUE_TIME_LIMIT=\"1\" ENABLED_FLAG=\"'testENABLED_FLAG'\" SETTYPE=\"'testSETTYPE'\" FOLDABLE_FLAG=\"'testFOLDABLE_FLAG'\" MEASTYPE=\"'testMEASTYPE'\" HOLD_FLAG=\"'testHOLD_FLAG'\" SCHEDULING_INFO=\"'testSCHEDULING_INFO'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_collections COLLECTION_ID=\"1\" COLLECTION_NAME=\"'testCOLLECTION_NAME'\" COLLECTION=\"'testCOLLECTION'\" MAIL_ERROR_ADDR=\"'testMAIL_ERROR_ADDR'\" MAIL_FAIL_ADDR=\"'testMAIL_FAIL_ADDR'\" MAIL_BUG_ADDR=\"'testMAIL_BUG_ADDR'\" MAX_ERRORS=\"1\" MAX_FK_ERRORS=\"1\" MAX_COL_LIMIT_ERRORS=\"1\" CHECK_FK_ERROR_FLAG=\"'testCHECK_FK_ERROR_FLAG'\" CHECK_COL_LIMITS_FLAG=\"'testCHECK_COL_LIMITS_FLAG'\" LAST_TRANSFER_DATE=\"'2000-01-01 00:00:00.0'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" COLLECTION_SET_ID=\"1\" USE_BATCH_ID=\"'testUSE_BATCH_ID'\" PRIORITY=\"1\" QUEUE_TIME_LIMIT=\"1\" ENABLED_FLAG=\"'testENABLED_FLAG'\" SETTYPE=\"'testSETTYPE'\" FOLDABLE_FLAG=\"'testFOLDABLE_FLAG'\" MEASTYPE=\"'testMEASTYPE'\" HOLD_FLAG=\"'testHOLD_FLAG'\" SCHEDULING_INFO=\"'testSCHEDULING_INFO'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_collections>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_collections ( COLLECTION_ID, COLLECTION_NAME, COLLECTION, MAIL_ERROR_ADDR, MAIL_FAIL_ADDR, MAIL_BUG_ADDR, MAX_ERRORS, MAX_FK_ERRORS, MAX_COL_LIMIT_ERRORS, CHECK_FK_ERROR_FLAG, CHECK_COL_LIMITS_FLAG, LAST_TRANSFER_DATE, VERSION_NUMBER, COLLECTION_SET_ID, USE_BATCH_ID, PRIORITY, QUEUE_TIME_LIMIT, ENABLED_FLAG, SETTYPE, FOLDABLE_FLAG, MEASTYPE, HOLD_FLAG, SCHEDULING_INFO ) values "
      + "( 1, 'testCOLLECTION_NAME', 'testCOLLECTION', 'testMAIL_ERROR_ADDR', 'testMAIL_FAIL_ADDR', 'testMAIL_BUG_ADDR', 1, 1, 1, 'testCHECK_FK_ERROR_FLAG', 'testCHECK_COL_LIMITS_FLAG', '2000-01-01 00:00:00.0', 'testVERSION_NUMBER', 1, 'testUSE_BATCH_ID', 1, 1, 'testENABLED_FLAG', 'testSETTYPE', 'testFOLDABLE_FLAG', 'testMEASTYPE', 'testHOLD_FLAG', 'testSCHEDULING_INFO' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCollection_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCollection_id(555L);
    assertEquals(555L, COLLECTION_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCollection_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCollection_name(Meta_collectionsTest.testStringGenerator("anotherCOLLECTION_NAME", 128));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherCOLLECTION_NAME", 128), COLLECTION_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCollection() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCollection(Meta_collectionsTest.testStringGenerator("anotherCOLLECTION", 200));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherCOLLECTION", 200), COLLECTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMail_error_addr() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMail_error_addr(Meta_collectionsTest.testStringGenerator("anotherMAIL_ERROR_ADDR", 100));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherMAIL_ERROR_ADDR", 100), MAIL_ERROR_ADDR.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMail_fail_addr() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMail_fail_addr(Meta_collectionsTest.testStringGenerator("anotherMAIL_FAIL_ADDR", 100));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherMAIL_FAIL_ADDR", 100), MAIL_FAIL_ADDR.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMail_bug_addr() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMail_bug_addr(Meta_collectionsTest.testStringGenerator("anotherMAIL_BUG_ADDR", 100));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherMAIL_BUG_ADDR", 100), MAIL_BUG_ADDR.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMax_errors() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMax_errors(555L);
    assertEquals(555L, MAX_ERRORS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMax_fk_errors() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMax_fk_errors(555L);
    assertEquals(555L, MAX_FK_ERRORS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMax_col_limit_errors() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMax_col_limit_errors(555L);
    assertEquals(555L, MAX_COL_LIMIT_ERRORS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCheck_fk_error_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCheck_fk_error_flag(Meta_collectionsTest.testStringGenerator("anotherCHECK_FK_ERROR_FLAG", 1));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherCHECK_FK_ERROR_FLAG", 1), CHECK_FK_ERROR_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCheck_col_limits_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCheck_col_limits_flag(Meta_collectionsTest.testStringGenerator("anotherCHECK_COL_LIMITS_FLAG", 1));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherCHECK_COL_LIMITS_FLAG", 1), CHECK_COL_LIMITS_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLast_transfer_date() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLast_transfer_date(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), LAST_TRANSFER_DATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_collectionsTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCollection_set_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCollection_set_id(555L);
    assertEquals(555L, COLLECTION_SET_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUse_batch_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUse_batch_id(Meta_collectionsTest.testStringGenerator("anotherUSE_BATCH_ID", 1));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherUSE_BATCH_ID", 1), USE_BATCH_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPriority() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPriority(555L);
    assertEquals(555L, PRIORITY.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetQueue_time_limit() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setQueue_time_limit(555L);
    assertEquals(555L, QUEUE_TIME_LIMIT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetEnabled_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setEnabled_flag(Meta_collectionsTest.testStringGenerator("anotherENABLED_FLAG", 1));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherENABLED_FLAG", 1), ENABLED_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSettype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSettype(Meta_collectionsTest.testStringGenerator("anotherSETTYPE", 10));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherSETTYPE", 10), SETTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFoldable_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFoldable_flag(Meta_collectionsTest.testStringGenerator("anotherFOLDABLE_FLAG", 1));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherFOLDABLE_FLAG", 1), FOLDABLE_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMeastype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMeastype(Meta_collectionsTest.testStringGenerator("anotherMEASTYPE", 30));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherMEASTYPE", 30), MEASTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetHold_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setHold_flag(Meta_collectionsTest.testStringGenerator("anotherHOLD_FLAG", 1));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherHOLD_FLAG", 1), HOLD_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetScheduling_info() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setScheduling_info(Meta_collectionsTest.testStringGenerator("anotherSCHEDULING_INFO", 2000));
    assertEquals(Meta_collectionsTest.testStringGenerator("anotherSCHEDULING_INFO", 2000), SCHEDULING_INFO.get(objUnderTest));
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
    String[] expectedKeys = { "COLLECTION_ID","VERSION_NUMBER","COLLECTION_SET_ID"};

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
    objUnderTest = new Meta_collections(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  COLLECTION_ID.get(objUnderTest)  + ", " + COLLECTION_NAME.get(objUnderTest)  + ", " + COLLECTION.get(objUnderTest)  + ", " + MAIL_ERROR_ADDR.get(objUnderTest)  + ", " + MAIL_FAIL_ADDR.get(objUnderTest)  + ", " + MAIL_BUG_ADDR.get(objUnderTest)  + ", " + MAX_ERRORS.get(objUnderTest)  + ", " + MAX_FK_ERRORS.get(objUnderTest)  + ", " + MAX_COL_LIMIT_ERRORS.get(objUnderTest)  + ", " + CHECK_FK_ERROR_FLAG.get(objUnderTest)  + ", " + CHECK_COL_LIMITS_FLAG.get(objUnderTest)  + ", " + LAST_TRANSFER_DATE.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + USE_BATCH_ID.get(objUnderTest)  + ", " + PRIORITY.get(objUnderTest)  + ", " + QUEUE_TIME_LIMIT.get(objUnderTest)  + ", " + ENABLED_FLAG.get(objUnderTest)  + ", " + SETTYPE.get(objUnderTest)  + ", " + FOLDABLE_FLAG.get(objUnderTest)  + ", " + MEASTYPE.get(objUnderTest)  + ", " + HOLD_FLAG.get(objUnderTest)  + ", " + SCHEDULING_INFO.get(objUnderTest) ;
    String expected =  "0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", "  + ", " + new Timestamp(0)  + ", "  + ", 0"  + ", "  + ", 0"  + ", 0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", " ;
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
    Meta_collections compareObj = new Meta_collections(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );

    /* Testing first with null primary key value */
    COLLECTION_ID.set(objUnderTest, null);
  	VERSION_NUMBER.set(objUnderTest, null);
  	COLLECTION_SET_ID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    COLLECTION_ID.set(objUnderTest,  7L );
  	VERSION_NUMBER.set(objUnderTest,  "different");
  	COLLECTION_SET_ID.set(objUnderTest,  7L );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    COLLECTION_ID.set(objUnderTest,  1L );
  	VERSION_NUMBER.set(objUnderTest,  "testVERSION_NUMBER");
  	COLLECTION_SET_ID.set(objUnderTest,  1L );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_collections with our current one. If the two
   * Meta_collectionss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_collections() throws Exception {

    /* Creating another Meta_collections which will be compared to the tested one */
    Meta_collections comparedObj = new Meta_collections(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_collections with our current one. If the two
   * Meta_collectionss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_collections() throws Exception {

    /* Creating another Meta_collections which will be compared to the tested one */
    Meta_collections comparedObj = new Meta_collections(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );
    comparedObj.setCollection_id( 7L );

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_collections with our current one. If the two
   * Meta_collectionss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_collections() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_collections comparedObj = new Meta_collections(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_collections with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_collections() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_collections comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_collections was null \n");
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
    assertEquals(Meta_collections.class, actualObject.getClass());
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
    Meta_collections testAgg = new Meta_collections(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );
    COLLECTION_ID.set(objUnderTest, 7L);

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Collection_id.
   */
  @Test
  public void testGetCollection_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getCollection_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Collection_id.
  */
  @Test
  public void testGetCollection_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCollection_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Collection_id.
  */
  @Test
  public void testGetCollection_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getCollection_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Collection_name.
   */
  @Test
  public void testGetCollection_nameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getCollection_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Collection_name.
  */
  @Test
  public void testGetCollection_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCollection_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Collection_name.
  */
  @Test
  public void testGetCollection_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getCollection_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Collection.
   */
  @Test
  public void testGetCollectionColumnSize() throws Exception {
    
     assertEquals(200, objUnderTest.getCollectionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Collection.
  */
  @Test
  public void testGetCollectionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCollectionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Collection.
  */
  @Test
  public void testGetCollectionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getCollectionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Mail_error_addr.
   */
  @Test
  public void testGetMail_error_addrColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getMail_error_addrColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Mail_error_addr.
  */
  @Test
  public void testGetMail_error_addrDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMail_error_addrDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Mail_error_addr.
  */
  @Test
  public void testGetMail_error_addrSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMail_error_addrSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Mail_fail_addr.
   */
  @Test
  public void testGetMail_fail_addrColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getMail_fail_addrColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Mail_fail_addr.
  */
  @Test
  public void testGetMail_fail_addrDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMail_fail_addrDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Mail_fail_addr.
  */
  @Test
  public void testGetMail_fail_addrSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMail_fail_addrSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Mail_bug_addr.
   */
  @Test
  public void testGetMail_bug_addrColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getMail_bug_addrColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Mail_bug_addr.
  */
  @Test
  public void testGetMail_bug_addrDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMail_bug_addrDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Mail_bug_addr.
  */
  @Test
  public void testGetMail_bug_addrSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMail_bug_addrSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Max_errors.
   */
  @Test
  public void testGetMax_errorsColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getMax_errorsColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Max_errors.
  */
  @Test
  public void testGetMax_errorsDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMax_errorsDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Max_errors.
  */
  @Test
  public void testGetMax_errorsSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getMax_errorsSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Max_fk_errors.
   */
  @Test
  public void testGetMax_fk_errorsColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getMax_fk_errorsColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Max_fk_errors.
  */
  @Test
  public void testGetMax_fk_errorsDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMax_fk_errorsDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Max_fk_errors.
  */
  @Test
  public void testGetMax_fk_errorsSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getMax_fk_errorsSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Max_col_limit_errors.
   */
  @Test
  public void testGetMax_col_limit_errorsColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getMax_col_limit_errorsColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Max_col_limit_errors.
  */
  @Test
  public void testGetMax_col_limit_errorsDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMax_col_limit_errorsDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Max_col_limit_errors.
  */
  @Test
  public void testGetMax_col_limit_errorsSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getMax_col_limit_errorsSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Check_fk_error_flag.
   */
  @Test
  public void testGetCheck_fk_error_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getCheck_fk_error_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Check_fk_error_flag.
  */
  @Test
  public void testGetCheck_fk_error_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCheck_fk_error_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Check_fk_error_flag.
  */
  @Test
  public void testGetCheck_fk_error_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getCheck_fk_error_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Check_col_limits_flag.
   */
  @Test
  public void testGetCheck_col_limits_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getCheck_col_limits_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Check_col_limits_flag.
  */
  @Test
  public void testGetCheck_col_limits_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCheck_col_limits_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Check_col_limits_flag.
  */
  @Test
  public void testGetCheck_col_limits_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getCheck_col_limits_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Last_transfer_date.
   */
  @Test
  public void testGetLast_transfer_dateColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getLast_transfer_dateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Last_transfer_date.
  */
  @Test
  public void testGetLast_transfer_dateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLast_transfer_dateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Last_transfer_date.
  */
  @Test
  public void testGetLast_transfer_dateSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getLast_transfer_dateSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Version_number.
   */
  @Test
  public void testGetVersion_numberColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getVersion_numberColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Version_number.
  */
  @Test
  public void testGetVersion_numberDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getVersion_numberDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Version_number.
  */
  @Test
  public void testGetVersion_numberSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getVersion_numberSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Collection_set_id.
   */
  @Test
  public void testGetCollection_set_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getCollection_set_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Collection_set_id.
  */
  @Test
  public void testGetCollection_set_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCollection_set_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Collection_set_id.
  */
  @Test
  public void testGetCollection_set_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getCollection_set_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Use_batch_id.
   */
  @Test
  public void testGetUse_batch_idColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getUse_batch_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Use_batch_id.
  */
  @Test
  public void testGetUse_batch_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUse_batch_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Use_batch_id.
  */
  @Test
  public void testGetUse_batch_idSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUse_batch_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Priority.
   */
  @Test
  public void testGetPriorityColumnSize() throws Exception {
    
     assertEquals(3, objUnderTest.getPriorityColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Priority.
  */
  @Test
  public void testGetPriorityDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPriorityDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Priority.
  */
  @Test
  public void testGetPrioritySQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getPrioritySQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Queue_time_limit.
   */
  @Test
  public void testGetQueue_time_limitColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getQueue_time_limitColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Queue_time_limit.
  */
  @Test
  public void testGetQueue_time_limitDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getQueue_time_limitDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Queue_time_limit.
  */
  @Test
  public void testGetQueue_time_limitSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getQueue_time_limitSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Enabled_flag.
   */
  @Test
  public void testGetEnabled_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getEnabled_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Enabled_flag.
  */
  @Test
  public void testGetEnabled_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getEnabled_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Enabled_flag.
  */
  @Test
  public void testGetEnabled_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getEnabled_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Settype.
   */
  @Test
  public void testGetSettypeColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getSettypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Settype.
  */
  @Test
  public void testGetSettypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSettypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Settype.
  */
  @Test
  public void testGetSettypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSettypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Foldable_flag.
   */
  @Test
  public void testGetFoldable_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getFoldable_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Foldable_flag.
  */
  @Test
  public void testGetFoldable_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFoldable_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Foldable_flag.
  */
  @Test
  public void testGetFoldable_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getFoldable_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Meastype.
   */
  @Test
  public void testGetMeastypeColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getMeastypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Meastype.
  */
  @Test
  public void testGetMeastypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMeastypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Meastype.
  */
  @Test
  public void testGetMeastypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMeastypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Hold_flag.
   */
  @Test
  public void testGetHold_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getHold_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Hold_flag.
  */
  @Test
  public void testGetHold_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getHold_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Hold_flag.
  */
  @Test
  public void testGetHold_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getHold_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Scheduling_info.
   */
  @Test
  public void testGetScheduling_infoColumnSize() throws Exception {
    
     assertEquals(2000, objUnderTest.getScheduling_infoColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Scheduling_info.
  */
  @Test
  public void testGetScheduling_infoDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getScheduling_infoDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Scheduling_info.
  */
  @Test
  public void testGetScheduling_infoSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getScheduling_infoSQLType());    
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
   * Testing original Meta_collections object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_collections(rockFactory, false);
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
    Meta_collections changedOriginal = new Meta_collections(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_collections(rockFactory, false);
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
    Meta_collections changedOriginal = new Meta_collections(rockFactory, false);
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