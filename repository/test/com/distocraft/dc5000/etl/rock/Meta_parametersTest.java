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
 * Test class for Meta_parameters. Changes to Meta_parameters table are made via
 * this class.
 */
public class Meta_parametersTest {

  private static Meta_parameters objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field RB_SEGMENT_NAME;
  
  private static Field USE_RB_SEGMENT_FLAG;
  
  private static Field DEFAULT_ERROR_MAIL_ADDR;
  
  private static Field DEFAULT_FAIL_MAIL_ADDR;
  
  private static Field DEFAULT_BUG_ERROR_MAIL_ADDR;
  
  private static Field DEFAULT_MAX_ERROR_VALUE;
  
  private static Field DEFAULT_MAX_FK_ERROR_VALUE;
  
  private static Field DEFAULT_MAX_COL_LIMIT_VALUE;
  
  private static Field TEMP_SUM_TABLESPACE;
  
  private static Field USE_TEMP_SUM_TABLESPACE_FLAG;
  
  private static Field BATCH_COLUMN_NAME;
  
  private static Field VERSION_NUMBER;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_parameters.class.getDeclaredField("newItem");
		RB_SEGMENT_NAME = Meta_parameters.class.getDeclaredField("RB_SEGMENT_NAME");
		USE_RB_SEGMENT_FLAG = Meta_parameters.class.getDeclaredField("USE_RB_SEGMENT_FLAG");
		DEFAULT_ERROR_MAIL_ADDR = Meta_parameters.class.getDeclaredField("DEFAULT_ERROR_MAIL_ADDR");
		DEFAULT_FAIL_MAIL_ADDR = Meta_parameters.class.getDeclaredField("DEFAULT_FAIL_MAIL_ADDR");
		DEFAULT_BUG_ERROR_MAIL_ADDR = Meta_parameters.class.getDeclaredField("DEFAULT_BUG_ERROR_MAIL_ADDR");
		DEFAULT_MAX_ERROR_VALUE = Meta_parameters.class.getDeclaredField("DEFAULT_MAX_ERROR_VALUE");
		DEFAULT_MAX_FK_ERROR_VALUE = Meta_parameters.class.getDeclaredField("DEFAULT_MAX_FK_ERROR_VALUE");
		DEFAULT_MAX_COL_LIMIT_VALUE = Meta_parameters.class.getDeclaredField("DEFAULT_MAX_COL_LIMIT_VALUE");
		TEMP_SUM_TABLESPACE = Meta_parameters.class.getDeclaredField("TEMP_SUM_TABLESPACE");
		USE_TEMP_SUM_TABLESPACE_FLAG = Meta_parameters.class.getDeclaredField("USE_TEMP_SUM_TABLESPACE_FLAG");
		BATCH_COLUMN_NAME = Meta_parameters.class.getDeclaredField("BATCH_COLUMN_NAME");
		VERSION_NUMBER = Meta_parameters.class.getDeclaredField("VERSION_NUMBER");
		newItem.setAccessible(true);
		RB_SEGMENT_NAME.setAccessible(true);
		USE_RB_SEGMENT_FLAG.setAccessible(true);
		DEFAULT_ERROR_MAIL_ADDR.setAccessible(true);
		DEFAULT_FAIL_MAIL_ADDR.setAccessible(true);
		DEFAULT_BUG_ERROR_MAIL_ADDR.setAccessible(true);
		DEFAULT_MAX_ERROR_VALUE.setAccessible(true);
		DEFAULT_MAX_FK_ERROR_VALUE.setAccessible(true);
		DEFAULT_MAX_COL_LIMIT_VALUE.setAccessible(true);
		TEMP_SUM_TABLESPACE.setAccessible(true);
		USE_TEMP_SUM_TABLESPACE_FLAG.setAccessible(true);
		BATCH_COLUMN_NAME.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
	  timeStampName = Meta_parameters.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_parameters ( RB_SEGMENT_NAME VARCHAR(30)  ,USE_RB_SEGMENT_FLAG VARCHAR(1) ,DEFAULT_ERROR_MAIL_ADDR VARCHAR(100) ,DEFAULT_FAIL_MAIL_ADDR VARCHAR(100) ,DEFAULT_BUG_ERROR_MAIL_ADDR VARCHAR(100) ,DEFAULT_MAX_ERROR_VALUE BIGINT  ,DEFAULT_MAX_FK_ERROR_VALUE BIGINT  ,DEFAULT_MAX_COL_LIMIT_VALUE BIGINT  ,TEMP_SUM_TABLESPACE VARCHAR(30) ,USE_TEMP_SUM_TABLESPACE_FLAG VARCHAR(1) ,BATCH_COLUMN_NAME VARCHAR(30) ,VERSION_NUMBER VARCHAR(32))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_parameters");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_parameters VALUES(  'testRB_SEGMENT_NAME'   ,  't'  ,  'testDEFAULT_ERROR_MAIL_ADDR'   ,  'testDEFAULT_FAIL_MAIL_ADDR'   ,  'testDEFAULT_BUG_ERROR_MAIL_ADDR'   ,1  ,1  ,1  ,  'testTEMP_SUM_TABLESPACE'   ,  't'  ,  'testBATCH_COLUMN_NAME'   ,  'testVERSION_NUMBER'  )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_parameters(rockFactory ,   "testVERSION_NUMBER" );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_parameters");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_parameters constructor variable initialization with null values.
   */
  @Test
  public void testMeta_parametersConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_parameters(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  RB_SEGMENT_NAME.get(objUnderTest)  + ", " + USE_RB_SEGMENT_FLAG.get(objUnderTest)  + ", " + DEFAULT_ERROR_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_FAIL_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_BUG_ERROR_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_MAX_ERROR_VALUE.get(objUnderTest)  + ", " + DEFAULT_MAX_FK_ERROR_VALUE.get(objUnderTest)  + ", " + DEFAULT_MAX_COL_LIMIT_VALUE.get(objUnderTest)  + ", " + TEMP_SUM_TABLESPACE.get(objUnderTest)  + ", " + USE_TEMP_SUM_TABLESPACE_FLAG.get(objUnderTest)  + ", " + BATCH_COLUMN_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_parameters constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_parametersConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_parameters(rockFactory ,  "testVERSION_NUMBER");

    /* Asserting that variables are initialized */
    String actual =  RB_SEGMENT_NAME.get(objUnderTest)  + ", " + USE_RB_SEGMENT_FLAG.get(objUnderTest)  + ", " + DEFAULT_ERROR_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_FAIL_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_BUG_ERROR_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_MAX_ERROR_VALUE.get(objUnderTest)  + ", " + DEFAULT_MAX_FK_ERROR_VALUE.get(objUnderTest)  + ", " + DEFAULT_MAX_COL_LIMIT_VALUE.get(objUnderTest)  + ", " + TEMP_SUM_TABLESPACE.get(objUnderTest)  + ", " + USE_TEMP_SUM_TABLESPACE_FLAG.get(objUnderTest)  + ", " + BATCH_COLUMN_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest) ;
    String expected =  "testRB_SEGMENT_NAME"  + ", t"  + ", testDEFAULT_ERROR_MAIL_ADDR"  + ", testDEFAULT_FAIL_MAIL_ADDR"  + ", testDEFAULT_BUG_ERROR_MAIL_ADDR"  + ", 1"  + ", 1"  + ", 1"  + ", testTEMP_SUM_TABLESPACE"  + ", t"  + ", testBATCH_COLUMN_NAME"  + ", testVERSION_NUMBER" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_parametersConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_parameters(null ,  "testVERSION_NUMBER");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_parameters constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_parametersConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_parameters whereObject = new Meta_parameters(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_parameters(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  RB_SEGMENT_NAME.get(objUnderTest)  + ", " + USE_RB_SEGMENT_FLAG.get(objUnderTest)  + ", " + DEFAULT_ERROR_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_FAIL_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_BUG_ERROR_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_MAX_ERROR_VALUE.get(objUnderTest)  + ", " + DEFAULT_MAX_FK_ERROR_VALUE.get(objUnderTest)  + ", " + DEFAULT_MAX_COL_LIMIT_VALUE.get(objUnderTest)  + ", " + TEMP_SUM_TABLESPACE.get(objUnderTest)  + ", " + USE_TEMP_SUM_TABLESPACE_FLAG.get(objUnderTest)  + ", " + BATCH_COLUMN_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest) ;
    String expected =  "testRB_SEGMENT_NAME"  + ", t"  + ", testDEFAULT_ERROR_MAIL_ADDR"  + ", testDEFAULT_FAIL_MAIL_ADDR"  + ", testDEFAULT_BUG_ERROR_MAIL_ADDR"  + ", 1"  + ", 1"  + ", 1"  + ", testTEMP_SUM_TABLESPACE"  + ", t"  + ", testBATCH_COLUMN_NAME"  + ", testVERSION_NUMBER" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_parametersConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_parameters whereObject = new Meta_parameters(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_parameters(null, whereObject);
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
    assertEquals("Meta_parameters", objUnderTest.getTableName());
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
    Meta_parameters whereObject = new Meta_parameters(rockFactory);

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
    Meta_parameters whereObject = new Meta_parameters(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_parameters");
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
    RB_SEGMENT_NAME.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("RB_SEGMENT_NAME");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_parameters");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the RB_SEGMENT_NAME column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT RB_SEGMENT_NAME FROM Meta_parameters");
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
    
    String expected = "<Meta_parameters RB_SEGMENT_NAME=\"'testRB_SEGMENT_NAME'\" USE_RB_SEGMENT_FLAG=\"'t'\" DEFAULT_ERROR_MAIL_ADDR=\"'testDEFAULT_ERROR_MAIL_ADDR'\" DEFAULT_FAIL_MAIL_ADDR=\"'testDEFAULT_FAIL_MAIL_ADDR'\" DEFAULT_BUG_ERROR_MAIL_ADDR=\"'testDEFAULT_BUG_ERROR_MAIL_ADDR'\" DEFAULT_MAX_ERROR_VALUE=\"1\" DEFAULT_MAX_FK_ERROR_VALUE=\"1\" DEFAULT_MAX_COL_LIMIT_VALUE=\"1\" TEMP_SUM_TABLESPACE=\"'testTEMP_SUM_TABLESPACE'\" USE_TEMP_SUM_TABLESPACE_FLAG=\"'t'\" BATCH_COLUMN_NAME=\"'testBATCH_COLUMN_NAME'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_parameters RB_SEGMENT_NAME=\"'testRB_SEGMENT_NAME'\" USE_RB_SEGMENT_FLAG=\"'t'\" DEFAULT_ERROR_MAIL_ADDR=\"'testDEFAULT_ERROR_MAIL_ADDR'\" DEFAULT_FAIL_MAIL_ADDR=\"'testDEFAULT_FAIL_MAIL_ADDR'\" DEFAULT_BUG_ERROR_MAIL_ADDR=\"'testDEFAULT_BUG_ERROR_MAIL_ADDR'\" DEFAULT_MAX_ERROR_VALUE=\"1\" DEFAULT_MAX_FK_ERROR_VALUE=\"1\" DEFAULT_MAX_COL_LIMIT_VALUE=\"1\" TEMP_SUM_TABLESPACE=\"'testTEMP_SUM_TABLESPACE'\" USE_TEMP_SUM_TABLESPACE_FLAG=\"'t'\" BATCH_COLUMN_NAME=\"'testBATCH_COLUMN_NAME'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_parameters>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_parameters ( RB_SEGMENT_NAME, USE_RB_SEGMENT_FLAG, DEFAULT_ERROR_MAIL_ADDR, DEFAULT_FAIL_MAIL_ADDR, DEFAULT_BUG_ERROR_MAIL_ADDR, DEFAULT_MAX_ERROR_VALUE, DEFAULT_MAX_FK_ERROR_VALUE, DEFAULT_MAX_COL_LIMIT_VALUE, TEMP_SUM_TABLESPACE, USE_TEMP_SUM_TABLESPACE_FLAG, BATCH_COLUMN_NAME, VERSION_NUMBER ) values "
      + "( 'testRB_SEGMENT_NAME', 't', 'testDEFAULT_ERROR_MAIL_ADDR', 'testDEFAULT_FAIL_MAIL_ADDR', 'testDEFAULT_BUG_ERROR_MAIL_ADDR', 1, 1, 1, 'testTEMP_SUM_TABLESPACE', 't', 'testBATCH_COLUMN_NAME', 'testVERSION_NUMBER' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetRb_segment_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setRb_segment_name(Meta_parametersTest.testStringGenerator("anotherRB_SEGMENT_NAME", 30));
    assertEquals(Meta_parametersTest.testStringGenerator("anotherRB_SEGMENT_NAME", 30), RB_SEGMENT_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUse_rb_segment_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUse_rb_segment_flag(Meta_parametersTest.testStringGenerator("anotherUSE_RB_SEGMENT_FLAG", 1));
    assertEquals(Meta_parametersTest.testStringGenerator("anotherUSE_RB_SEGMENT_FLAG", 1), USE_RB_SEGMENT_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDefault_error_mail_addr() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDefault_error_mail_addr(Meta_parametersTest.testStringGenerator("anotherDEFAULT_ERROR_MAIL_ADDR", 100));
    assertEquals(Meta_parametersTest.testStringGenerator("anotherDEFAULT_ERROR_MAIL_ADDR", 100), DEFAULT_ERROR_MAIL_ADDR.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDefault_fail_mail_addr() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDefault_fail_mail_addr(Meta_parametersTest.testStringGenerator("anotherDEFAULT_FAIL_MAIL_ADDR", 100));
    assertEquals(Meta_parametersTest.testStringGenerator("anotherDEFAULT_FAIL_MAIL_ADDR", 100), DEFAULT_FAIL_MAIL_ADDR.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDefault_bug_error_mail_addr() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDefault_bug_error_mail_addr(Meta_parametersTest.testStringGenerator("anotherDEFAULT_BUG_ERROR_MAIL_ADDR", 100));
    assertEquals(Meta_parametersTest.testStringGenerator("anotherDEFAULT_BUG_ERROR_MAIL_ADDR", 100), DEFAULT_BUG_ERROR_MAIL_ADDR.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDefault_max_error_value() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDefault_max_error_value(555L);
    assertEquals(555L, DEFAULT_MAX_ERROR_VALUE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDefault_max_fk_error_value() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDefault_max_fk_error_value(555L);
    assertEquals(555L, DEFAULT_MAX_FK_ERROR_VALUE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDefault_max_col_limit_value() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDefault_max_col_limit_value(555L);
    assertEquals(555L, DEFAULT_MAX_COL_LIMIT_VALUE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTemp_sum_tablespace() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTemp_sum_tablespace(Meta_parametersTest.testStringGenerator("anotherTEMP_SUM_TABLESPACE", 30));
    assertEquals(Meta_parametersTest.testStringGenerator("anotherTEMP_SUM_TABLESPACE", 30), TEMP_SUM_TABLESPACE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUse_temp_sum_tablespace_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUse_temp_sum_tablespace_flag(Meta_parametersTest.testStringGenerator("anotherUSE_TEMP_SUM_TABLESPACE_FLAG", 1));
    assertEquals(Meta_parametersTest.testStringGenerator("anotherUSE_TEMP_SUM_TABLESPACE_FLAG", 1), USE_TEMP_SUM_TABLESPACE_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBatch_column_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBatch_column_name(Meta_parametersTest.testStringGenerator("anotherBATCH_COLUMN_NAME", 30));
    assertEquals(Meta_parametersTest.testStringGenerator("anotherBATCH_COLUMN_NAME", 30), BATCH_COLUMN_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_parametersTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_parametersTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
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
    String[] expectedKeys = { "VERSION_NUMBER"};

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
    objUnderTest = new Meta_parameters(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  RB_SEGMENT_NAME.get(objUnderTest)  + ", " + USE_RB_SEGMENT_FLAG.get(objUnderTest)  + ", " + DEFAULT_ERROR_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_FAIL_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_BUG_ERROR_MAIL_ADDR.get(objUnderTest)  + ", " + DEFAULT_MAX_ERROR_VALUE.get(objUnderTest)  + ", " + DEFAULT_MAX_FK_ERROR_VALUE.get(objUnderTest)  + ", " + DEFAULT_MAX_COL_LIMIT_VALUE.get(objUnderTest)  + ", " + TEMP_SUM_TABLESPACE.get(objUnderTest)  + ", " + USE_TEMP_SUM_TABLESPACE_FLAG.get(objUnderTest)  + ", " + BATCH_COLUMN_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", "  + ", "  + ", " ;
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
    Meta_parameters compareObj = new Meta_parameters(rockFactory ,  "testVERSION_NUMBER");

    /* Testing first with null primary key value */
    VERSION_NUMBER.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSION_NUMBER.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSION_NUMBER.set(objUnderTest,  "testVERSION_NUMBER");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_parameters with our current one. If the two
   * Meta_parameterss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_parameters() throws Exception {

    /* Creating another Meta_parameters which will be compared to the tested one */
    Meta_parameters comparedObj = new Meta_parameters(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_parameters with our current one. If the two
   * Meta_parameterss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_parameters() throws Exception {

    /* Creating another Meta_parameters which will be compared to the tested one */
    Meta_parameters comparedObj = new Meta_parameters(rockFactory ,  "testVERSION_NUMBER");
    comparedObj.setRb_segment_name( "DifferentRB_SEGMENT_NAME");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_parameters with our current one. If the two
   * Meta_parameterss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_parameters() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_parameters comparedObj = new Meta_parameters(rockFactory ,  "testVERSION_NUMBER");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_parameters with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_parameters() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_parameters comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_parameters was null \n");
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
    assertEquals(Meta_parameters.class, actualObject.getClass());
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
    Meta_parameters testAgg = new Meta_parameters(rockFactory ,  "testVERSION_NUMBER");
    RB_SEGMENT_NAME.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Rb_segment_name.
   */
  @Test
  public void testGetRb_segment_nameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getRb_segment_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Rb_segment_name.
  */
  @Test
  public void testGetRb_segment_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getRb_segment_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Rb_segment_name.
  */
  @Test
  public void testGetRb_segment_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getRb_segment_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Use_rb_segment_flag.
   */
  @Test
  public void testGetUse_rb_segment_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getUse_rb_segment_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Use_rb_segment_flag.
  */
  @Test
  public void testGetUse_rb_segment_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUse_rb_segment_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Use_rb_segment_flag.
  */
  @Test
  public void testGetUse_rb_segment_flagSQLType() throws Exception {
    
    assertEquals(1, objUnderTest.getUse_rb_segment_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Default_error_mail_addr.
   */
  @Test
  public void testGetDefault_error_mail_addrColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getDefault_error_mail_addrColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Default_error_mail_addr.
  */
  @Test
  public void testGetDefault_error_mail_addrDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDefault_error_mail_addrDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Default_error_mail_addr.
  */
  @Test
  public void testGetDefault_error_mail_addrSQLType() throws Exception {
    
    assertEquals(1, objUnderTest.getDefault_error_mail_addrSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Default_fail_mail_addr.
   */
  @Test
  public void testGetDefault_fail_mail_addrColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getDefault_fail_mail_addrColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Default_fail_mail_addr.
  */
  @Test
  public void testGetDefault_fail_mail_addrDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDefault_fail_mail_addrDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Default_fail_mail_addr.
  */
  @Test
  public void testGetDefault_fail_mail_addrSQLType() throws Exception {
    
    assertEquals(1, objUnderTest.getDefault_fail_mail_addrSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Default_bug_error_mail_addr.
   */
  @Test
  public void testGetDefault_bug_error_mail_addrColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getDefault_bug_error_mail_addrColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Default_bug_error_mail_addr.
  */
  @Test
  public void testGetDefault_bug_error_mail_addrDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDefault_bug_error_mail_addrDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Default_bug_error_mail_addr.
  */
  @Test
  public void testGetDefault_bug_error_mail_addrSQLType() throws Exception {
    
    assertEquals(1, objUnderTest.getDefault_bug_error_mail_addrSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Default_max_error_value.
   */
  @Test
  public void testGetDefault_max_error_valueColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getDefault_max_error_valueColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Default_max_error_value.
  */
  @Test
  public void testGetDefault_max_error_valueDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDefault_max_error_valueDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Default_max_error_value.
  */
  @Test
  public void testGetDefault_max_error_valueSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getDefault_max_error_valueSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Default_max_fk_error_value.
   */
  @Test
  public void testGetDefault_max_fk_error_valueColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getDefault_max_fk_error_valueColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Default_max_fk_error_value.
  */
  @Test
  public void testGetDefault_max_fk_error_valueDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDefault_max_fk_error_valueDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Default_max_fk_error_value.
  */
  @Test
  public void testGetDefault_max_fk_error_valueSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getDefault_max_fk_error_valueSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Default_max_col_limit_value.
   */
  @Test
  public void testGetDefault_max_col_limit_valueColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getDefault_max_col_limit_valueColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Default_max_col_limit_value.
  */
  @Test
  public void testGetDefault_max_col_limit_valueDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDefault_max_col_limit_valueDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Default_max_col_limit_value.
  */
  @Test
  public void testGetDefault_max_col_limit_valueSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getDefault_max_col_limit_valueSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Temp_sum_tablespace.
   */
  @Test
  public void testGetTemp_sum_tablespaceColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getTemp_sum_tablespaceColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Temp_sum_tablespace.
  */
  @Test
  public void testGetTemp_sum_tablespaceDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTemp_sum_tablespaceDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Temp_sum_tablespace.
  */
  @Test
  public void testGetTemp_sum_tablespaceSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTemp_sum_tablespaceSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Use_temp_sum_tablespace_flag.
   */
  @Test
  public void testGetUse_temp_sum_tablespace_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getUse_temp_sum_tablespace_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Use_temp_sum_tablespace_flag.
  */
  @Test
  public void testGetUse_temp_sum_tablespace_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUse_temp_sum_tablespace_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Use_temp_sum_tablespace_flag.
  */
  @Test
  public void testGetUse_temp_sum_tablespace_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUse_temp_sum_tablespace_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Batch_column_name.
   */
  @Test
  public void testGetBatch_column_nameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getBatch_column_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Batch_column_name.
  */
  @Test
  public void testGetBatch_column_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBatch_column_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Batch_column_name.
  */
  @Test
  public void testGetBatch_column_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBatch_column_nameSQLType());    
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
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Meta_parameters object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_parameters(rockFactory, false);
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
    Meta_parameters changedOriginal = new Meta_parameters(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_parameters(rockFactory, false);
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
    Meta_parameters changedOriginal = new Meta_parameters(rockFactory, false);
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