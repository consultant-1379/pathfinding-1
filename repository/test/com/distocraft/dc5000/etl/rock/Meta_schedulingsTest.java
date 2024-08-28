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
 * Test class for Meta_schedulings. Changes to Meta_schedulings table are made via
 * this class.
 */
public class Meta_schedulingsTest {

  private static Meta_schedulings objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSION_NUMBER;
  
  private static Field ID;
  
  private static Field EXECUTION_TYPE;
  
  private static Field OS_COMMAND;
  
  private static Field SCHEDULING_MONTH;
  
  private static Field SCHEDULING_DAY;
  
  private static Field SCHEDULING_HOUR;
  
  private static Field SCHEDULING_MIN;
  
  private static Field COLLECTION_SET_ID;
  
  private static Field COLLECTION_ID;
  
  private static Field MON_FLAG;
  
  private static Field TUE_FLAG;
  
  private static Field WED_FLAG;
  
  private static Field THU_FLAG;
  
  private static Field FRI_FLAG;
  
  private static Field SAT_FLAG;
  
  private static Field SUN_FLAG;
  
  private static Field STATUS;
  
  private static Field LAST_EXECUTION_TIME;
  
  private static Field INTERVAL_HOUR;
  
  private static Field INTERVAL_MIN;
  
  private static Field NAME;
  
  private static Field HOLD_FLAG;
  
  private static Field PRIORITY;
  
  private static Field SCHEDULING_YEAR;
  
  private static Field TRIGGER_COMMAND;
  
  private static Field LAST_EXEC_TIME_MS;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_schedulings.class.getDeclaredField("newItem");
		VERSION_NUMBER = Meta_schedulings.class.getDeclaredField("VERSION_NUMBER");
		ID = Meta_schedulings.class.getDeclaredField("ID");
		EXECUTION_TYPE = Meta_schedulings.class.getDeclaredField("EXECUTION_TYPE");
		OS_COMMAND = Meta_schedulings.class.getDeclaredField("OS_COMMAND");
		SCHEDULING_MONTH = Meta_schedulings.class.getDeclaredField("SCHEDULING_MONTH");
		SCHEDULING_DAY = Meta_schedulings.class.getDeclaredField("SCHEDULING_DAY");
		SCHEDULING_HOUR = Meta_schedulings.class.getDeclaredField("SCHEDULING_HOUR");
		SCHEDULING_MIN = Meta_schedulings.class.getDeclaredField("SCHEDULING_MIN");
		COLLECTION_SET_ID = Meta_schedulings.class.getDeclaredField("COLLECTION_SET_ID");
		COLLECTION_ID = Meta_schedulings.class.getDeclaredField("COLLECTION_ID");
		MON_FLAG = Meta_schedulings.class.getDeclaredField("MON_FLAG");
		TUE_FLAG = Meta_schedulings.class.getDeclaredField("TUE_FLAG");
		WED_FLAG = Meta_schedulings.class.getDeclaredField("WED_FLAG");
		THU_FLAG = Meta_schedulings.class.getDeclaredField("THU_FLAG");
		FRI_FLAG = Meta_schedulings.class.getDeclaredField("FRI_FLAG");
		SAT_FLAG = Meta_schedulings.class.getDeclaredField("SAT_FLAG");
		SUN_FLAG = Meta_schedulings.class.getDeclaredField("SUN_FLAG");
		STATUS = Meta_schedulings.class.getDeclaredField("STATUS");
		LAST_EXECUTION_TIME = Meta_schedulings.class.getDeclaredField("LAST_EXECUTION_TIME");
		INTERVAL_HOUR = Meta_schedulings.class.getDeclaredField("INTERVAL_HOUR");
		INTERVAL_MIN = Meta_schedulings.class.getDeclaredField("INTERVAL_MIN");
		NAME = Meta_schedulings.class.getDeclaredField("NAME");
		HOLD_FLAG = Meta_schedulings.class.getDeclaredField("HOLD_FLAG");
		PRIORITY = Meta_schedulings.class.getDeclaredField("PRIORITY");
		SCHEDULING_YEAR = Meta_schedulings.class.getDeclaredField("SCHEDULING_YEAR");
		TRIGGER_COMMAND = Meta_schedulings.class.getDeclaredField("TRIGGER_COMMAND");
		LAST_EXEC_TIME_MS = Meta_schedulings.class.getDeclaredField("LAST_EXEC_TIME_MS");
		newItem.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		ID.setAccessible(true);
		EXECUTION_TYPE.setAccessible(true);
		OS_COMMAND.setAccessible(true);
		SCHEDULING_MONTH.setAccessible(true);
		SCHEDULING_DAY.setAccessible(true);
		SCHEDULING_HOUR.setAccessible(true);
		SCHEDULING_MIN.setAccessible(true);
		COLLECTION_SET_ID.setAccessible(true);
		COLLECTION_ID.setAccessible(true);
		MON_FLAG.setAccessible(true);
		TUE_FLAG.setAccessible(true);
		WED_FLAG.setAccessible(true);
		THU_FLAG.setAccessible(true);
		FRI_FLAG.setAccessible(true);
		SAT_FLAG.setAccessible(true);
		SUN_FLAG.setAccessible(true);
		STATUS.setAccessible(true);
		LAST_EXECUTION_TIME.setAccessible(true);
		INTERVAL_HOUR.setAccessible(true);
		INTERVAL_MIN.setAccessible(true);
		NAME.setAccessible(true);
		HOLD_FLAG.setAccessible(true);
		PRIORITY.setAccessible(true);
		SCHEDULING_YEAR.setAccessible(true);
		TRIGGER_COMMAND.setAccessible(true);
		LAST_EXEC_TIME_MS.setAccessible(true);
	  timeStampName = Meta_schedulings.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_schedulings ( VERSION_NUMBER VARCHAR(31)  ,ID BIGINT  ,EXECUTION_TYPE VARCHAR(31) ,OS_COMMAND VARCHAR(31) ,SCHEDULING_MONTH BIGINT  ,SCHEDULING_DAY BIGINT  ,SCHEDULING_HOUR BIGINT  ,SCHEDULING_MIN BIGINT  ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,MON_FLAG VARCHAR(31) ,TUE_FLAG VARCHAR(31) ,WED_FLAG VARCHAR(31) ,THU_FLAG VARCHAR(31) ,FRI_FLAG VARCHAR(31) ,SAT_FLAG VARCHAR(31) ,SUN_FLAG VARCHAR(31) ,STATUS VARCHAR(31) ,LAST_EXECUTION_TIME TIMESTAMP  ,INTERVAL_HOUR BIGINT  ,INTERVAL_MIN BIGINT  ,NAME VARCHAR(31) ,HOLD_FLAG VARCHAR(31) ,PRIORITY BIGINT  ,SCHEDULING_YEAR BIGINT  ,TRIGGER_COMMAND VARCHAR(31) ,LAST_EXEC_TIME_MS BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_schedulings");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_schedulings VALUES( 'testVERSION_NUMBER'  ,1  ,'testEXECUTION_TYPE'  ,'testOS_COMMAND'  ,1  ,1  ,1  ,1  ,1  ,1  ,'testMON_FLAG'  ,'testTUE_FLAG'  ,'testWED_FLAG'  ,'testTHU_FLAG'  ,'testFRI_FLAG'  ,'testSAT_FLAG'  ,'testSUN_FLAG'  ,'testSTATUS'  ,'2000-01-01 00:00:00.0'  ,1  ,1  ,'testNAME'  ,'testHOLD_FLAG'  ,1  ,1  ,'testTRIGGER_COMMAND'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_schedulings(rockFactory ,  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_schedulings");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_schedulings constructor variable initialization with null values.
   */
  @Test
  public void testMeta_schedulingsConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_schedulings(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSION_NUMBER.get(objUnderTest)  + ", " + ID.get(objUnderTest)  + ", " + EXECUTION_TYPE.get(objUnderTest)  + ", " + OS_COMMAND.get(objUnderTest)  + ", " + SCHEDULING_MONTH.get(objUnderTest)  + ", " + SCHEDULING_DAY.get(objUnderTest)  + ", " + SCHEDULING_HOUR.get(objUnderTest)  + ", " + SCHEDULING_MIN.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + MON_FLAG.get(objUnderTest)  + ", " + TUE_FLAG.get(objUnderTest)  + ", " + WED_FLAG.get(objUnderTest)  + ", " + THU_FLAG.get(objUnderTest)  + ", " + FRI_FLAG.get(objUnderTest)  + ", " + SAT_FLAG.get(objUnderTest)  + ", " + SUN_FLAG.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + LAST_EXECUTION_TIME.get(objUnderTest)  + ", " + INTERVAL_HOUR.get(objUnderTest)  + ", " + INTERVAL_MIN.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + HOLD_FLAG.get(objUnderTest)  + ", " + PRIORITY.get(objUnderTest)  + ", " + SCHEDULING_YEAR.get(objUnderTest)  + ", " + TRIGGER_COMMAND.get(objUnderTest)  + ", " + LAST_EXEC_TIME_MS.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_schedulings constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_schedulingsConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_schedulings(rockFactory ,  1L );

    /* Asserting that variables are initialized */
    String actual =  VERSION_NUMBER.get(objUnderTest)  + ", " + ID.get(objUnderTest)  + ", " + EXECUTION_TYPE.get(objUnderTest)  + ", " + OS_COMMAND.get(objUnderTest)  + ", " + SCHEDULING_MONTH.get(objUnderTest)  + ", " + SCHEDULING_DAY.get(objUnderTest)  + ", " + SCHEDULING_HOUR.get(objUnderTest)  + ", " + SCHEDULING_MIN.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + MON_FLAG.get(objUnderTest)  + ", " + TUE_FLAG.get(objUnderTest)  + ", " + WED_FLAG.get(objUnderTest)  + ", " + THU_FLAG.get(objUnderTest)  + ", " + FRI_FLAG.get(objUnderTest)  + ", " + SAT_FLAG.get(objUnderTest)  + ", " + SUN_FLAG.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + LAST_EXECUTION_TIME.get(objUnderTest)  + ", " + INTERVAL_HOUR.get(objUnderTest)  + ", " + INTERVAL_MIN.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + HOLD_FLAG.get(objUnderTest)  + ", " + PRIORITY.get(objUnderTest)  + ", " + SCHEDULING_YEAR.get(objUnderTest)  + ", " + TRIGGER_COMMAND.get(objUnderTest)  + ", " + LAST_EXEC_TIME_MS.get(objUnderTest) ;
    String expected =  "testVERSION_NUMBER"  + ", 1"  + ", testEXECUTION_TYPE"  + ", testOS_COMMAND"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testMON_FLAG"  + ", testTUE_FLAG"  + ", testWED_FLAG"  + ", testTHU_FLAG"  + ", testFRI_FLAG"  + ", testSAT_FLAG"  + ", testSUN_FLAG"  + ", testSTATUS"  + ", 2000-01-01 00:00:00.0"  + ", 1"  + ", 1"  + ", testNAME"  + ", testHOLD_FLAG"  + ", 1"  + ", 1"  + ", testTRIGGER_COMMAND"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_schedulingsConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_schedulings(null ,  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_schedulings constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_schedulingsConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_schedulings whereObject = new Meta_schedulings(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_schedulings(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSION_NUMBER.get(objUnderTest)  + ", " + ID.get(objUnderTest)  + ", " + EXECUTION_TYPE.get(objUnderTest)  + ", " + OS_COMMAND.get(objUnderTest)  + ", " + SCHEDULING_MONTH.get(objUnderTest)  + ", " + SCHEDULING_DAY.get(objUnderTest)  + ", " + SCHEDULING_HOUR.get(objUnderTest)  + ", " + SCHEDULING_MIN.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + MON_FLAG.get(objUnderTest)  + ", " + TUE_FLAG.get(objUnderTest)  + ", " + WED_FLAG.get(objUnderTest)  + ", " + THU_FLAG.get(objUnderTest)  + ", " + FRI_FLAG.get(objUnderTest)  + ", " + SAT_FLAG.get(objUnderTest)  + ", " + SUN_FLAG.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + LAST_EXECUTION_TIME.get(objUnderTest)  + ", " + INTERVAL_HOUR.get(objUnderTest)  + ", " + INTERVAL_MIN.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + HOLD_FLAG.get(objUnderTest)  + ", " + PRIORITY.get(objUnderTest)  + ", " + SCHEDULING_YEAR.get(objUnderTest)  + ", " + TRIGGER_COMMAND.get(objUnderTest)  + ", " + LAST_EXEC_TIME_MS.get(objUnderTest) ;
    String expected =  "testVERSION_NUMBER"  + ", 1"  + ", testEXECUTION_TYPE"  + ", testOS_COMMAND"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testMON_FLAG"  + ", testTUE_FLAG"  + ", testWED_FLAG"  + ", testTHU_FLAG"  + ", testFRI_FLAG"  + ", testSAT_FLAG"  + ", testSUN_FLAG"  + ", testSTATUS"  + ", 2000-01-01 00:00:00.0"  + ", 1"  + ", 1"  + ", testNAME"  + ", testHOLD_FLAG"  + ", 1"  + ", 1"  + ", testTRIGGER_COMMAND"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_schedulingsConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_schedulings whereObject = new Meta_schedulings(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_schedulings(null, whereObject);
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
    assertEquals("Meta_schedulings", objUnderTest.getTableName());
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
    Meta_schedulings whereObject = new Meta_schedulings(rockFactory);

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
    Meta_schedulings whereObject = new Meta_schedulings(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_schedulings");
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
    VERSION_NUMBER.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("VERSION_NUMBER");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_schedulings");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSION_NUMBER column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSION_NUMBER FROM Meta_schedulings");
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
    
    String expected = "<Meta_schedulings VERSION_NUMBER=\"'testVERSION_NUMBER'\" ID=\"1\" EXECUTION_TYPE=\"'testEXECUTION_TYPE'\" OS_COMMAND=\"'testOS_COMMAND'\" SCHEDULING_MONTH=\"1\" SCHEDULING_DAY=\"1\" SCHEDULING_HOUR=\"1\" SCHEDULING_MIN=\"1\" COLLECTION_SET_ID=\"1\" COLLECTION_ID=\"1\" MON_FLAG=\"'testMON_FLAG'\" TUE_FLAG=\"'testTUE_FLAG'\" WED_FLAG=\"'testWED_FLAG'\" THU_FLAG=\"'testTHU_FLAG'\" FRI_FLAG=\"'testFRI_FLAG'\" SAT_FLAG=\"'testSAT_FLAG'\" SUN_FLAG=\"'testSUN_FLAG'\" STATUS=\"'testSTATUS'\" LAST_EXECUTION_TIME=\"'2000-01-01 00:00:00.0'\" INTERVAL_HOUR=\"1\" INTERVAL_MIN=\"1\" NAME=\"'testNAME'\" HOLD_FLAG=\"'testHOLD_FLAG'\" PRIORITY=\"1\" SCHEDULING_YEAR=\"1\" TRIGGER_COMMAND=\"'testTRIGGER_COMMAND'\" LAST_EXEC_TIME_MS=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_schedulings VERSION_NUMBER=\"'testVERSION_NUMBER'\" ID=\"1\" EXECUTION_TYPE=\"'testEXECUTION_TYPE'\" OS_COMMAND=\"'testOS_COMMAND'\" SCHEDULING_MONTH=\"1\" SCHEDULING_DAY=\"1\" SCHEDULING_HOUR=\"1\" SCHEDULING_MIN=\"1\" COLLECTION_SET_ID=\"1\" COLLECTION_ID=\"1\" MON_FLAG=\"'testMON_FLAG'\" TUE_FLAG=\"'testTUE_FLAG'\" WED_FLAG=\"'testWED_FLAG'\" THU_FLAG=\"'testTHU_FLAG'\" FRI_FLAG=\"'testFRI_FLAG'\" SAT_FLAG=\"'testSAT_FLAG'\" SUN_FLAG=\"'testSUN_FLAG'\" STATUS=\"'testSTATUS'\" LAST_EXECUTION_TIME=\"'2000-01-01 00:00:00.0'\" INTERVAL_HOUR=\"1\" INTERVAL_MIN=\"1\" NAME=\"'testNAME'\" HOLD_FLAG=\"'testHOLD_FLAG'\" PRIORITY=\"1\" SCHEDULING_YEAR=\"1\" TRIGGER_COMMAND=\"'testTRIGGER_COMMAND'\" LAST_EXEC_TIME_MS=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_schedulings>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_schedulings ( VERSION_NUMBER, ID, EXECUTION_TYPE, OS_COMMAND, SCHEDULING_MONTH, SCHEDULING_DAY, SCHEDULING_HOUR, SCHEDULING_MIN, COLLECTION_SET_ID, COLLECTION_ID, MON_FLAG, TUE_FLAG, WED_FLAG, THU_FLAG, FRI_FLAG, SAT_FLAG, SUN_FLAG, STATUS, LAST_EXECUTION_TIME, INTERVAL_HOUR, INTERVAL_MIN, NAME, HOLD_FLAG, PRIORITY, SCHEDULING_YEAR, TRIGGER_COMMAND, LAST_EXEC_TIME_MS ) values "
      + "( 'testVERSION_NUMBER', 1, 'testEXECUTION_TYPE', 'testOS_COMMAND', 1, 1, 1, 1, 1, 1, 'testMON_FLAG', 'testTUE_FLAG', 'testWED_FLAG', 'testTHU_FLAG', 'testFRI_FLAG', 'testSAT_FLAG', 'testSUN_FLAG', 'testSTATUS', '2000-01-01 00:00:00.0', 1, 1, 'testNAME', 'testHOLD_FLAG', 1, 1, 'testTRIGGER_COMMAND', 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_schedulingsTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
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
  public void testSetAndGetExecution_type() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setExecution_type(Meta_schedulingsTest.testStringGenerator("anotherEXECUTION_TYPE", 15));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherEXECUTION_TYPE", 15), EXECUTION_TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetOs_command() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setOs_command(Meta_schedulingsTest.testStringGenerator("anotherOS_COMMAND", 2000));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherOS_COMMAND", 2000), OS_COMMAND.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetScheduling_month() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setScheduling_month(555L);
    assertEquals(555L, SCHEDULING_MONTH.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetScheduling_day() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setScheduling_day(555L);
    assertEquals(555L, SCHEDULING_DAY.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetScheduling_hour() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setScheduling_hour(555L);
    assertEquals(555L, SCHEDULING_HOUR.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetScheduling_min() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setScheduling_min(555L);
    assertEquals(555L, SCHEDULING_MIN.get(objUnderTest));
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
  public void testSetAndGetMon_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMon_flag(Meta_schedulingsTest.testStringGenerator("anotherMON_FLAG", 1));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherMON_FLAG", 1), MON_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTue_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTue_flag(Meta_schedulingsTest.testStringGenerator("anotherTUE_FLAG", 1));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherTUE_FLAG", 1), TUE_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetWed_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setWed_flag(Meta_schedulingsTest.testStringGenerator("anotherWED_FLAG", 1));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherWED_FLAG", 1), WED_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetThu_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setThu_flag(Meta_schedulingsTest.testStringGenerator("anotherTHU_FLAG", 1));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherTHU_FLAG", 1), THU_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFri_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFri_flag(Meta_schedulingsTest.testStringGenerator("anotherFRI_FLAG", 1));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherFRI_FLAG", 1), FRI_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSat_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSat_flag(Meta_schedulingsTest.testStringGenerator("anotherSAT_FLAG", 1));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherSAT_FLAG", 1), SAT_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSun_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSun_flag(Meta_schedulingsTest.testStringGenerator("anotherSUN_FLAG", 1));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherSUN_FLAG", 1), SUN_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatus() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatus(Meta_schedulingsTest.testStringGenerator("anotherSTATUS", 20));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherSTATUS", 20), STATUS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLast_execution_time() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLast_execution_time(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), LAST_EXECUTION_TIME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetInterval_hour() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInterval_hour(555L);
    assertEquals(555L, INTERVAL_HOUR.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetInterval_min() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInterval_min(555L);
    assertEquals(555L, INTERVAL_MIN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetName() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setName(Meta_schedulingsTest.testStringGenerator("anotherNAME", 128));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherNAME", 128), NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetHold_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setHold_flag(Meta_schedulingsTest.testStringGenerator("anotherHOLD_FLAG", 1));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherHOLD_FLAG", 1), HOLD_FLAG.get(objUnderTest));
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
  public void testSetAndGetScheduling_year() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setScheduling_year(555L);
    assertEquals(555L, SCHEDULING_YEAR.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTrigger_command() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTrigger_command(Meta_schedulingsTest.testStringGenerator("anotherTRIGGER_COMMAND", 2000));
    assertEquals(Meta_schedulingsTest.testStringGenerator("anotherTRIGGER_COMMAND", 2000), TRIGGER_COMMAND.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLast_exec_time_ms() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLast_exec_time_ms(555L);
    assertEquals(555L, LAST_EXEC_TIME_MS.get(objUnderTest));
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
    String[] expectedKeys = { "ID"};

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
    objUnderTest = new Meta_schedulings(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSION_NUMBER.get(objUnderTest)  + ", " + ID.get(objUnderTest)  + ", " + EXECUTION_TYPE.get(objUnderTest)  + ", " + OS_COMMAND.get(objUnderTest)  + ", " + SCHEDULING_MONTH.get(objUnderTest)  + ", " + SCHEDULING_DAY.get(objUnderTest)  + ", " + SCHEDULING_HOUR.get(objUnderTest)  + ", " + SCHEDULING_MIN.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + MON_FLAG.get(objUnderTest)  + ", " + TUE_FLAG.get(objUnderTest)  + ", " + WED_FLAG.get(objUnderTest)  + ", " + THU_FLAG.get(objUnderTest)  + ", " + FRI_FLAG.get(objUnderTest)  + ", " + SAT_FLAG.get(objUnderTest)  + ", " + SUN_FLAG.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + LAST_EXECUTION_TIME.get(objUnderTest)  + ", " + INTERVAL_HOUR.get(objUnderTest)  + ", " + INTERVAL_MIN.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + HOLD_FLAG.get(objUnderTest)  + ", " + PRIORITY.get(objUnderTest)  + ", " + SCHEDULING_YEAR.get(objUnderTest)  + ", " + TRIGGER_COMMAND.get(objUnderTest)  + ", " + LAST_EXEC_TIME_MS.get(objUnderTest) ;
    String expected =  ""  + ", 0"  + ", "  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", " + new Timestamp(0)  + ", 0"  + ", 0"  + ", "  + ", "  + ", 0"  + ", 0"  + ", "  + ", 0" ;
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
    Meta_schedulings compareObj = new Meta_schedulings(rockFactory ,  1L );

    /* Testing first with null primary key value */
    ID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    ID.set(objUnderTest,  7L );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    ID.set(objUnderTest,  1L );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_schedulings with our current one. If the two
   * Meta_schedulingss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_schedulings() throws Exception {

    /* Creating another Meta_schedulings which will be compared to the tested one */
    Meta_schedulings comparedObj = new Meta_schedulings(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_schedulings with our current one. If the two
   * Meta_schedulingss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_schedulings() throws Exception {

    /* Creating another Meta_schedulings which will be compared to the tested one */
    Meta_schedulings comparedObj = new Meta_schedulings(rockFactory ,  1L );
    comparedObj.setVersion_number( "DifferentVERSION_NUMBER");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_schedulings with our current one. If the two
   * Meta_schedulingss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_schedulings() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_schedulings comparedObj = new Meta_schedulings(rockFactory ,  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_schedulings with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_schedulings() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_schedulings comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_schedulings was null \n");
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
    assertEquals(Meta_schedulings.class, actualObject.getClass());
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
    Meta_schedulings testAgg = new Meta_schedulings(rockFactory ,  1L );
    VERSION_NUMBER.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
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
   * Testing columnsize retrieving for Execution_type.
   */
  @Test
  public void testGetExecution_typeColumnSize() throws Exception {
    
     assertEquals(15, objUnderTest.getExecution_typeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Execution_type.
  */
  @Test
  public void testGetExecution_typeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getExecution_typeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Execution_type.
  */
  @Test
  public void testGetExecution_typeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getExecution_typeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Os_command.
   */
  @Test
  public void testGetOs_commandColumnSize() throws Exception {
    
     assertEquals(2000, objUnderTest.getOs_commandColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Os_command.
  */
  @Test
  public void testGetOs_commandDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getOs_commandDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Os_command.
  */
  @Test
  public void testGetOs_commandSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getOs_commandSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Scheduling_month.
   */
  @Test
  public void testGetScheduling_monthColumnSize() throws Exception {
    
     assertEquals(2, objUnderTest.getScheduling_monthColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Scheduling_month.
  */
  @Test
  public void testGetScheduling_monthDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getScheduling_monthDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Scheduling_month.
  */
  @Test
  public void testGetScheduling_monthSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getScheduling_monthSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Scheduling_day.
   */
  @Test
  public void testGetScheduling_dayColumnSize() throws Exception {
    
     assertEquals(2, objUnderTest.getScheduling_dayColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Scheduling_day.
  */
  @Test
  public void testGetScheduling_dayDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getScheduling_dayDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Scheduling_day.
  */
  @Test
  public void testGetScheduling_daySQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getScheduling_daySQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Scheduling_hour.
   */
  @Test
  public void testGetScheduling_hourColumnSize() throws Exception {
    
     assertEquals(2, objUnderTest.getScheduling_hourColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Scheduling_hour.
  */
  @Test
  public void testGetScheduling_hourDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getScheduling_hourDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Scheduling_hour.
  */
  @Test
  public void testGetScheduling_hourSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getScheduling_hourSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Scheduling_min.
   */
  @Test
  public void testGetScheduling_minColumnSize() throws Exception {
    
     assertEquals(2, objUnderTest.getScheduling_minColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Scheduling_min.
  */
  @Test
  public void testGetScheduling_minDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getScheduling_minDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Scheduling_min.
  */
  @Test
  public void testGetScheduling_minSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getScheduling_minSQLType());    
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
   * Testing columnsize retrieving for Mon_flag.
   */
  @Test
  public void testGetMon_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getMon_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Mon_flag.
  */
  @Test
  public void testGetMon_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMon_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Mon_flag.
  */
  @Test
  public void testGetMon_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMon_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Tue_flag.
   */
  @Test
  public void testGetTue_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getTue_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Tue_flag.
  */
  @Test
  public void testGetTue_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTue_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Tue_flag.
  */
  @Test
  public void testGetTue_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTue_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Wed_flag.
   */
  @Test
  public void testGetWed_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getWed_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Wed_flag.
  */
  @Test
  public void testGetWed_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getWed_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Wed_flag.
  */
  @Test
  public void testGetWed_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getWed_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Thu_flag.
   */
  @Test
  public void testGetThu_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getThu_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Thu_flag.
  */
  @Test
  public void testGetThu_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getThu_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Thu_flag.
  */
  @Test
  public void testGetThu_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getThu_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Fri_flag.
   */
  @Test
  public void testGetFri_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getFri_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Fri_flag.
  */
  @Test
  public void testGetFri_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFri_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Fri_flag.
  */
  @Test
  public void testGetFri_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getFri_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Sat_flag.
   */
  @Test
  public void testGetSat_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getSat_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Sat_flag.
  */
  @Test
  public void testGetSat_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSat_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Sat_flag.
  */
  @Test
  public void testGetSat_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSat_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Sun_flag.
   */
  @Test
  public void testGetSun_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getSun_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Sun_flag.
  */
  @Test
  public void testGetSun_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSun_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Sun_flag.
  */
  @Test
  public void testGetSun_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSun_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Status.
   */
  @Test
  public void testGetStatusColumnSize() throws Exception {
    
     assertEquals(20, objUnderTest.getStatusColumnSize());   
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
   * Testing columnsize retrieving for Last_execution_time.
   */
  @Test
  public void testGetLast_execution_timeColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getLast_execution_timeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Last_execution_time.
  */
  @Test
  public void testGetLast_execution_timeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLast_execution_timeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Last_execution_time.
  */
  @Test
  public void testGetLast_execution_timeSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getLast_execution_timeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Interval_hour.
   */
  @Test
  public void testGetInterval_hourColumnSize() throws Exception {
    
     assertEquals(2, objUnderTest.getInterval_hourColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Interval_hour.
  */
  @Test
  public void testGetInterval_hourDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getInterval_hourDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Interval_hour.
  */
  @Test
  public void testGetInterval_hourSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getInterval_hourSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Interval_min.
   */
  @Test
  public void testGetInterval_minColumnSize() throws Exception {
    
     assertEquals(2, objUnderTest.getInterval_minColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Interval_min.
  */
  @Test
  public void testGetInterval_minDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getInterval_minDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Interval_min.
  */
  @Test
  public void testGetInterval_minSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getInterval_minSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Name.
   */
  @Test
  public void testGetNameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getNameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Name.
  */
  @Test
  public void testGetNameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getNameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Name.
  */
  @Test
  public void testGetNameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getNameSQLType());    
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
   * Testing columnsize retrieving for Scheduling_year.
   */
  @Test
  public void testGetScheduling_yearColumnSize() throws Exception {
    
     assertEquals(4, objUnderTest.getScheduling_yearColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Scheduling_year.
  */
  @Test
  public void testGetScheduling_yearDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getScheduling_yearDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Scheduling_year.
  */
  @Test
  public void testGetScheduling_yearSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getScheduling_yearSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Trigger_command.
   */
  @Test
  public void testGetTrigger_commandColumnSize() throws Exception {
    
     assertEquals(2000, objUnderTest.getTrigger_commandColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Trigger_command.
  */
  @Test
  public void testGetTrigger_commandDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTrigger_commandDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Trigger_command.
  */
  @Test
  public void testGetTrigger_commandSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTrigger_commandSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Last_exec_time_ms.
   */
  @Test
  public void testGetLast_exec_time_msColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getLast_exec_time_msColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Last_exec_time_ms.
  */
  @Test
  public void testGetLast_exec_time_msDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLast_exec_time_msDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Last_exec_time_ms.
  */
  @Test
  public void testGetLast_exec_time_msSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getLast_exec_time_msSQLType());    
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
   * Testing original Meta_schedulings object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_schedulings(rockFactory, false);
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
    Meta_schedulings changedOriginal = new Meta_schedulings(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_schedulings(rockFactory, false);
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
    Meta_schedulings changedOriginal = new Meta_schedulings(rockFactory, false);
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