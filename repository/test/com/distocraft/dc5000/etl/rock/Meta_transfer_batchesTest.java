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
 * Test class for Meta_transfer_batches. Changes to Meta_transfer_batches table are made via
 * this class.
 */
public class Meta_transfer_batchesTest {

  private static Meta_transfer_batches objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field ID;
  
  private static Field START_DATE;
  
  private static Field END_DATE;
  
  private static Field FAIL_FLAG;
  
  private static Field STATUS;
  
  private static Field VERSION_NUMBER;
  
  private static Field COLLECTION_SET_ID;
  
  private static Field COLLECTION_ID;
  
  private static Field META_COLLECTION_NAME;
  
  private static Field META_COLLECTION_SET_NAME;
  
  private static Field SETTYPE;
  
  private static Field SLOT_ID;
  
  private static Field SCHEDULING_INFO;
  
  private static Field SERVICE_NODE;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_transfer_batches.class.getDeclaredField("newItem");
		ID = Meta_transfer_batches.class.getDeclaredField("ID");
		START_DATE = Meta_transfer_batches.class.getDeclaredField("START_DATE");
		END_DATE = Meta_transfer_batches.class.getDeclaredField("END_DATE");
		FAIL_FLAG = Meta_transfer_batches.class.getDeclaredField("FAIL_FLAG");
		STATUS = Meta_transfer_batches.class.getDeclaredField("STATUS");
		VERSION_NUMBER = Meta_transfer_batches.class.getDeclaredField("VERSION_NUMBER");
		COLLECTION_SET_ID = Meta_transfer_batches.class.getDeclaredField("COLLECTION_SET_ID");
		COLLECTION_ID = Meta_transfer_batches.class.getDeclaredField("COLLECTION_ID");
		META_COLLECTION_NAME = Meta_transfer_batches.class.getDeclaredField("META_COLLECTION_NAME");
		META_COLLECTION_SET_NAME = Meta_transfer_batches.class.getDeclaredField("META_COLLECTION_SET_NAME");
		SETTYPE = Meta_transfer_batches.class.getDeclaredField("SETTYPE");
		SLOT_ID = Meta_transfer_batches.class.getDeclaredField("SLOT_ID");
		SCHEDULING_INFO = Meta_transfer_batches.class.getDeclaredField("SCHEDULING_INFO");
		SERVICE_NODE = Meta_transfer_batches.class.getDeclaredField("SERVICE_NODE");
		newItem.setAccessible(true);
		ID.setAccessible(true);
		START_DATE.setAccessible(true);
		END_DATE.setAccessible(true);
		FAIL_FLAG.setAccessible(true);
		STATUS.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		COLLECTION_SET_ID.setAccessible(true);
		COLLECTION_ID.setAccessible(true);
		META_COLLECTION_NAME.setAccessible(true);
		META_COLLECTION_SET_NAME.setAccessible(true);
		SETTYPE.setAccessible(true);
		SLOT_ID.setAccessible(true);
		SCHEDULING_INFO.setAccessible(true);
		SERVICE_NODE.setAccessible(true);
	  timeStampName = Meta_transfer_batches.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_transfer_batches ( ID BIGINT  ,START_DATE TIMESTAMP  ,END_DATE TIMESTAMP  ,FAIL_FLAG VARCHAR(1) ,STATUS VARCHAR(10) ,VERSION_NUMBER VARCHAR(32) ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,META_COLLECTION_NAME VARCHAR(128) ,META_COLLECTION_SET_NAME VARCHAR(128) ,SETTYPE VARCHAR(10) ,SLOT_ID INTEGER  ,SCHEDULING_INFO VARCHAR(16000) ,SERVICE_NODE VARCHAR(64))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_transfer_batches");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_transfer_batches VALUES( 1  ,'2000-01-01 00:00:00.0'  ,'2000-01-01 00:00:00.0'  ,  't'  ,  'testSTATUS'  ,  'testVERSION_NUMBER'   ,1  ,1  ,  'testMETA_COLLECTION_NAME'   ,  'testMETA_COLLECTION_SET_NAME'   ,  'testSETTYP'  ,1  ,  'testSCHEDULING_INFO'   ,  'testSERVICE_NODE'  )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_transfer_batches(rockFactory ,  1L ,   "testVERSION_NUMBER" ,   "testMETA_COLLECTION_NAME" ,   "testMETA_COLLECTION_SET_NAME" );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_transfer_batches");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_transfer_batches constructor variable initialization with null values.
   */
  @Test
  public void testMeta_transfer_batchesConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transfer_batches(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  ID.get(objUnderTest)  + ", " + START_DATE.get(objUnderTest)  + ", " + END_DATE.get(objUnderTest)  + ", " + FAIL_FLAG.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + META_COLLECTION_NAME.get(objUnderTest)  + ", " + META_COLLECTION_SET_NAME.get(objUnderTest)  + ", " + SETTYPE.get(objUnderTest)  + ", " + SLOT_ID.get(objUnderTest)  + ", " + SCHEDULING_INFO.get(objUnderTest)  + ", " + SERVICE_NODE.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_transfer_batches constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_transfer_batchesConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transfer_batches(rockFactory ,  1L ,  "testVERSION_NUMBER",  "testMETA_COLLECTION_NAME",  "testMETA_COLLECTION_SET_NAME");

    /* Asserting that variables are initialized */
    String actual =  ID.get(objUnderTest)  + ", " + START_DATE.get(objUnderTest)  + ", " + END_DATE.get(objUnderTest)  + ", " + FAIL_FLAG.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + META_COLLECTION_NAME.get(objUnderTest)  + ", " + META_COLLECTION_SET_NAME.get(objUnderTest)  + ", " + SETTYPE.get(objUnderTest)  + ", " + SLOT_ID.get(objUnderTest)  + ", " + SCHEDULING_INFO.get(objUnderTest)  + ", " + SERVICE_NODE.get(objUnderTest) ;
    String expected =  "1"  + ", 2000-01-01 00:00:00.0"  + ", 2000-01-01 00:00:00.0"  + ", t"  + ", testSTATUS"  + ", testVERSION_NUMBER"  + ", 1"  + ", 1"  + ", testMETA_COLLECTION_NAME"  + ", testMETA_COLLECTION_SET_NAME"  + ", testSETTYP"  + ", 1"  + ", testSCHEDULING_INFO"  + ", testSERVICE_NODE" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_transfer_batchesConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_transfer_batches(null ,  1L ,  "testVERSION_NUMBER",  "testMETA_COLLECTION_NAME",  "testMETA_COLLECTION_SET_NAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_transfer_batches constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_transfer_batchesConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_transfer_batches whereObject = new Meta_transfer_batches(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_transfer_batches(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  ID.get(objUnderTest)  + ", " + START_DATE.get(objUnderTest)  + ", " + END_DATE.get(objUnderTest)  + ", " + FAIL_FLAG.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + META_COLLECTION_NAME.get(objUnderTest)  + ", " + META_COLLECTION_SET_NAME.get(objUnderTest)  + ", " + SETTYPE.get(objUnderTest)  + ", " + SLOT_ID.get(objUnderTest)  + ", " + SCHEDULING_INFO.get(objUnderTest)  + ", " + SERVICE_NODE.get(objUnderTest) ;
    String expected =  "1"  + ", 2000-01-01 00:00:00.0"  + ", 2000-01-01 00:00:00.0"  + ", t"  + ", testSTATUS"  + ", testVERSION_NUMBER"  + ", 1"  + ", 1"  + ", testMETA_COLLECTION_NAME"  + ", testMETA_COLLECTION_SET_NAME"  + ", testSETTYP"  + ", 1"  + ", testSCHEDULING_INFO"  + ", testSERVICE_NODE" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_transfer_batchesConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_transfer_batches whereObject = new Meta_transfer_batches(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_transfer_batches(null, whereObject);
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
    assertEquals("Meta_transfer_batches", objUnderTest.getTableName());
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
    Meta_transfer_batches whereObject = new Meta_transfer_batches(rockFactory);

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
    Meta_transfer_batches whereObject = new Meta_transfer_batches(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_transfer_batches");
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
    ID.set(objUnderTest, 1L);
    HashSet testSet = new HashSet();
    testSet.add("ID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_transfer_batches");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the ID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT ID FROM Meta_transfer_batches");
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
    
    String expected = "<Meta_transfer_batches ID=\"1\" START_DATE=\"'2000-01-01 00:00:00.0'\" END_DATE=\"'2000-01-01 00:00:00.0'\" FAIL_FLAG=\"'t'\" STATUS=\"'testSTATUS'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" COLLECTION_SET_ID=\"1\" COLLECTION_ID=\"1\" META_COLLECTION_NAME=\"'testMETA_COLLECTION_NAME'\" META_COLLECTION_SET_NAME=\"'testMETA_COLLECTION_SET_NAME'\" SETTYPE=\"'testSETTYP'\" SLOT_ID=\"1\" SCHEDULING_INFO=\"'testSCHEDULING_INFO'\" SERVICE_NODE=\"'testSERVICE_NODE'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_transfer_batches ID=\"1\" START_DATE=\"'2000-01-01 00:00:00.0'\" END_DATE=\"'2000-01-01 00:00:00.0'\" FAIL_FLAG=\"'t'\" STATUS=\"'testSTATUS'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" COLLECTION_SET_ID=\"1\" COLLECTION_ID=\"1\" META_COLLECTION_NAME=\"'testMETA_COLLECTION_NAME'\" META_COLLECTION_SET_NAME=\"'testMETA_COLLECTION_SET_NAME'\" SETTYPE=\"'testSETTYP'\" SLOT_ID=\"1\" SCHEDULING_INFO=\"'testSCHEDULING_INFO'\" SERVICE_NODE=\"'testSERVICE_NODE'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_transfer_batches>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_transfer_batches ( ID, START_DATE, END_DATE, FAIL_FLAG, STATUS, VERSION_NUMBER, COLLECTION_SET_ID, COLLECTION_ID, META_COLLECTION_NAME, META_COLLECTION_SET_NAME, SETTYPE, SLOT_ID, SCHEDULING_INFO, SERVICE_NODE ) values "
      + "( 1, '2000-01-01 00:00:00.0', '2000-01-01 00:00:00.0', 't', 'testSTATUS', 'testVERSION_NUMBER', 1, 1, 'testMETA_COLLECTION_NAME', 'testMETA_COLLECTION_SET_NAME', 'testSETTYP', 1, 'testSCHEDULING_INFO', 'testSERVICE_NODE' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetId() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setId(555L);
    assertEquals(555L, ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStart_date() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStart_date(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), START_DATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetEnd_date() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setEnd_date(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), END_DATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFail_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFail_flag(Meta_transfer_batchesTest.testStringGenerator("anotherFAIL_FLAG", 1));
    assertEquals(Meta_transfer_batchesTest.testStringGenerator("anotherFAIL_FLAG", 1), FAIL_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatus() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatus(Meta_transfer_batchesTest.testStringGenerator("anotherSTATUS", 10));
    assertEquals(Meta_transfer_batchesTest.testStringGenerator("anotherSTATUS", 10), STATUS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_transfer_batchesTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_transfer_batchesTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
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
  public void testSetAndGetMeta_collection_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMeta_collection_name(Meta_transfer_batchesTest.testStringGenerator("anotherMETA_COLLECTION_NAME", 128));
    assertEquals(Meta_transfer_batchesTest.testStringGenerator("anotherMETA_COLLECTION_NAME", 128), META_COLLECTION_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMeta_collection_set_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMeta_collection_set_name(Meta_transfer_batchesTest.testStringGenerator("anotherMETA_COLLECTION_SET_NAME", 128));
    assertEquals(Meta_transfer_batchesTest.testStringGenerator("anotherMETA_COLLECTION_SET_NAME", 128), META_COLLECTION_SET_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSettype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSettype(Meta_transfer_batchesTest.testStringGenerator("anotherSETTYPE", 10));
    assertEquals(Meta_transfer_batchesTest.testStringGenerator("anotherSETTYPE", 10), SETTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSlot_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSlot_id(555);
    assertEquals(555, SLOT_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetScheduling_info() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setScheduling_info(Meta_transfer_batchesTest.testStringGenerator("anotherSCHEDULING_INFO", 16000));
    assertEquals(Meta_transfer_batchesTest.testStringGenerator("anotherSCHEDULING_INFO", 16000), SCHEDULING_INFO.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetService_node() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setService_node(Meta_transfer_batchesTest.testStringGenerator("anotherSERVICE_NODE", 64));
    assertEquals(Meta_transfer_batchesTest.testStringGenerator("anotherSERVICE_NODE", 64), SERVICE_NODE.get(objUnderTest));
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
    String[] expectedKeys = { "ID","VERSION_NUMBER","META_COLLECTION_NAME","META_COLLECTION_SET_NAME"};

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
    objUnderTest = new Meta_transfer_batches(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  ID.get(objUnderTest)  + ", " + START_DATE.get(objUnderTest)  + ", " + END_DATE.get(objUnderTest)  + ", " + FAIL_FLAG.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + META_COLLECTION_NAME.get(objUnderTest)  + ", " + META_COLLECTION_SET_NAME.get(objUnderTest)  + ", " + SETTYPE.get(objUnderTest)  + ", " + SLOT_ID.get(objUnderTest)  + ", " + SCHEDULING_INFO.get(objUnderTest)  + ", " + SERVICE_NODE.get(objUnderTest) ;
    String expected =  "0"  + ", " + new Timestamp(0)  + ", " + new Timestamp(0)  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", "  + ", "  + ", "  + ", 0"  + ", "  + ", " ;
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
    Meta_transfer_batches compareObj = new Meta_transfer_batches(rockFactory ,  1L ,  "testVERSION_NUMBER",  "testMETA_COLLECTION_NAME",  "testMETA_COLLECTION_SET_NAME");

    /* Testing first with null primary key value */
    ID.set(objUnderTest, null);
  	VERSION_NUMBER.set(objUnderTest, null);
  	META_COLLECTION_NAME.set(objUnderTest, null);
  	META_COLLECTION_SET_NAME.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    ID.set(objUnderTest,  7L );
  	VERSION_NUMBER.set(objUnderTest,  "different");
  	META_COLLECTION_NAME.set(objUnderTest,  "different");
  	META_COLLECTION_SET_NAME.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    ID.set(objUnderTest,  1L );
  	VERSION_NUMBER.set(objUnderTest,  "testVERSION_NUMBER");
  	META_COLLECTION_NAME.set(objUnderTest,  "testMETA_COLLECTION_NAME");
  	META_COLLECTION_SET_NAME.set(objUnderTest,  "testMETA_COLLECTION_SET_NAME");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_transfer_batches with our current one. If the two
   * Meta_transfer_batchess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_transfer_batches() throws Exception {

    /* Creating another Meta_transfer_batches which will be compared to the tested one */
    Meta_transfer_batches comparedObj = new Meta_transfer_batches(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_transfer_batches with our current one. If the two
   * Meta_transfer_batchess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_transfer_batches() throws Exception {

    /* Creating another Meta_transfer_batches which will be compared to the tested one */
    Meta_transfer_batches comparedObj = new Meta_transfer_batches(rockFactory ,  1L ,  "testVERSION_NUMBER",  "testMETA_COLLECTION_NAME",  "testMETA_COLLECTION_SET_NAME");
    comparedObj.setId( 7L );

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_transfer_batches with our current one. If the two
   * Meta_transfer_batchess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_transfer_batches() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_transfer_batches comparedObj = new Meta_transfer_batches(rockFactory ,  1L ,  "testVERSION_NUMBER",  "testMETA_COLLECTION_NAME",  "testMETA_COLLECTION_SET_NAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_transfer_batches with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_transfer_batches() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_transfer_batches comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_transfer_batches was null \n");
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
    assertEquals(Meta_transfer_batches.class, actualObject.getClass());
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
    Meta_transfer_batches testAgg = new Meta_transfer_batches(rockFactory ,  1L ,  "testVERSION_NUMBER",  "testMETA_COLLECTION_NAME",  "testMETA_COLLECTION_SET_NAME");
    ID.set(objUnderTest, 7L);

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Id.
   */
  @Test
  public void testGetIdColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getIdColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Id.
  */
  @Test
  public void testGetIdDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIdDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Id.
  */
  @Test
  public void testGetIdSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getIdSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Start_date.
   */
  @Test
  public void testGetStart_dateColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getStart_dateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Start_date.
  */
  @Test
  public void testGetStart_dateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getStart_dateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Start_date.
  */
  @Test
  public void testGetStart_dateSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getStart_dateSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for End_date.
   */
  @Test
  public void testGetEnd_dateColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getEnd_dateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for End_date.
  */
  @Test
  public void testGetEnd_dateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getEnd_dateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for End_date.
  */
  @Test
  public void testGetEnd_dateSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getEnd_dateSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Fail_flag.
   */
  @Test
  public void testGetFail_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getFail_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Fail_flag.
  */
  @Test
  public void testGetFail_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFail_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Fail_flag.
  */
  @Test
  public void testGetFail_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getFail_flagSQLType());    
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
   * Testing columnsize retrieving for Meta_collection_name.
   */
  @Test
  public void testGetMeta_collection_nameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getMeta_collection_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Meta_collection_name.
  */
  @Test
  public void testGetMeta_collection_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMeta_collection_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Meta_collection_name.
  */
  @Test
  public void testGetMeta_collection_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMeta_collection_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Meta_collection_set_name.
   */
  @Test
  public void testGetMeta_collection_set_nameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getMeta_collection_set_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Meta_collection_set_name.
  */
  @Test
  public void testGetMeta_collection_set_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMeta_collection_set_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Meta_collection_set_name.
  */
  @Test
  public void testGetMeta_collection_set_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMeta_collection_set_nameSQLType());    
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
   * Testing columnsize retrieving for Slot_id.
   */
  @Test
  public void testGetSlot_idColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getSlot_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Slot_id.
  */
  @Test
  public void testGetSlot_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSlot_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Slot_id.
  */
  @Test
  public void testGetSlot_idSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getSlot_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Scheduling_info.
   */
  @Test
  public void testGetScheduling_infoColumnSize() throws Exception {
    
     assertEquals(16000, objUnderTest.getScheduling_infoColumnSize());   
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
   * Testing columnsize retrieving for Service_node.
   */
  @Test
  public void testGetService_nodeColumnSize() throws Exception {
    
     assertEquals(64, objUnderTest.getService_nodeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Service_node.
  */
  @Test
  public void testGetService_nodeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getService_nodeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Service_node.
  */
  @Test
  public void testGetService_nodeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getService_nodeSQLType());    
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
   * Testing original Meta_transfer_batches object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_transfer_batches(rockFactory, false);
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
    Meta_transfer_batches changedOriginal = new Meta_transfer_batches(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_transfer_batches(rockFactory, false);
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
    Meta_transfer_batches changedOriginal = new Meta_transfer_batches(rockFactory, false);
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