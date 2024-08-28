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
 * Test class for Meta_databases. Changes to Meta_databases table are made via
 * this class.
 */
public class Meta_databasesTest {

  private static Meta_databases objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field USERNAME;
  
  private static Field VERSION_NUMBER;
  
  private static Field TYPE_NAME;
  
  private static Field CONNECTION_ID;
  
  private static Field CONNECTION_NAME;
  
  private static Field CONNECTION_STRING;
  
  private static Field PASSWORD;
  
  private static Field DESCRIPTION;
  
  private static Field DRIVER_NAME;
  
  private static Field DB_LINK_NAME;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_databases.class.getDeclaredField("newItem");
		USERNAME = Meta_databases.class.getDeclaredField("USERNAME");
		VERSION_NUMBER = Meta_databases.class.getDeclaredField("VERSION_NUMBER");
		TYPE_NAME = Meta_databases.class.getDeclaredField("TYPE_NAME");
		CONNECTION_ID = Meta_databases.class.getDeclaredField("CONNECTION_ID");
		CONNECTION_NAME = Meta_databases.class.getDeclaredField("CONNECTION_NAME");
		CONNECTION_STRING = Meta_databases.class.getDeclaredField("CONNECTION_STRING");
		PASSWORD = Meta_databases.class.getDeclaredField("PASSWORD");
		DESCRIPTION = Meta_databases.class.getDeclaredField("DESCRIPTION");
		DRIVER_NAME = Meta_databases.class.getDeclaredField("DRIVER_NAME");
		DB_LINK_NAME = Meta_databases.class.getDeclaredField("DB_LINK_NAME");
		newItem.setAccessible(true);
		USERNAME.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		TYPE_NAME.setAccessible(true);
		CONNECTION_ID.setAccessible(true);
		CONNECTION_NAME.setAccessible(true);
		CONNECTION_STRING.setAccessible(true);
		PASSWORD.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		DRIVER_NAME.setAccessible(true);
		DB_LINK_NAME.setAccessible(true);
	  timeStampName = Meta_databases.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_databases ( USERNAME VARCHAR(31)  ,VERSION_NUMBER VARCHAR(31) ,TYPE_NAME VARCHAR(31) ,CONNECTION_ID BIGINT  ,CONNECTION_NAME VARCHAR(31) ,CONNECTION_STRING VARCHAR(31) ,PASSWORD VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,DRIVER_NAME VARCHAR(31) ,DB_LINK_NAME VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_databases");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_databases VALUES( 'testUSERNAME'  ,'testVERSION_NUMBER'  ,'testTYPE_NAME'  ,1  ,'testCONNECTION_NAME'  ,'testCONNECTION_STRING'  ,'testPASSWORD'  ,'testDESCRIPTION'  ,'testDRIVER_NAME'  ,'testDB_LINK_NAME' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_databases(rockFactory ,  "testVERSION_NUMBER",  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_databases");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_databases constructor variable initialization with null values.
   */
  @Test
  public void testMeta_databasesConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_databases(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  USERNAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TYPE_NAME.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + CONNECTION_NAME.get(objUnderTest)  + ", " + CONNECTION_STRING.get(objUnderTest)  + ", " + PASSWORD.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + DRIVER_NAME.get(objUnderTest)  + ", " + DB_LINK_NAME.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_databases constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_databasesConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_databases(rockFactory ,  "testVERSION_NUMBER",  1L );

    /* Asserting that variables are initialized */
    String actual =  USERNAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TYPE_NAME.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + CONNECTION_NAME.get(objUnderTest)  + ", " + CONNECTION_STRING.get(objUnderTest)  + ", " + PASSWORD.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + DRIVER_NAME.get(objUnderTest)  + ", " + DB_LINK_NAME.get(objUnderTest) ;
    String expected =  "testUSERNAME"  + ", testVERSION_NUMBER"  + ", testTYPE_NAME"  + ", 1"  + ", testCONNECTION_NAME"  + ", testCONNECTION_STRING"  + ", testPASSWORD"  + ", testDESCRIPTION"  + ", testDRIVER_NAME"  + ", testDB_LINK_NAME" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_databasesConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_databases(null ,  "testVERSION_NUMBER",  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_databases constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_databasesConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_databases whereObject = new Meta_databases(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_databases(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  USERNAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TYPE_NAME.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + CONNECTION_NAME.get(objUnderTest)  + ", " + CONNECTION_STRING.get(objUnderTest)  + ", " + PASSWORD.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + DRIVER_NAME.get(objUnderTest)  + ", " + DB_LINK_NAME.get(objUnderTest) ;
    String expected =  "testUSERNAME"  + ", testVERSION_NUMBER"  + ", testTYPE_NAME"  + ", 1"  + ", testCONNECTION_NAME"  + ", testCONNECTION_STRING"  + ", testPASSWORD"  + ", testDESCRIPTION"  + ", testDRIVER_NAME"  + ", testDB_LINK_NAME" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_databasesConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_databases whereObject = new Meta_databases(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_databases(null, whereObject);
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
    assertEquals("Meta_databases", objUnderTest.getTableName());
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
    Meta_databases whereObject = new Meta_databases(rockFactory);

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
    Meta_databases whereObject = new Meta_databases(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_databases");
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
    USERNAME.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("USERNAME");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_databases");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the USERNAME column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT USERNAME FROM Meta_databases");
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
    
    String expected = "<Meta_databases USERNAME=\"'testUSERNAME'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" TYPE_NAME=\"'testTYPE_NAME'\" CONNECTION_ID=\"1\" CONNECTION_NAME=\"'testCONNECTION_NAME'\" CONNECTION_STRING=\"'testCONNECTION_STRING'\" PASSWORD=\"'testPASSWORD'\" DESCRIPTION=\"'testDESCRIPTION'\" DRIVER_NAME=\"'testDRIVER_NAME'\" DB_LINK_NAME=\"'testDB_LINK_NAME'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_databases USERNAME=\"'testUSERNAME'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" TYPE_NAME=\"'testTYPE_NAME'\" CONNECTION_ID=\"1\" CONNECTION_NAME=\"'testCONNECTION_NAME'\" CONNECTION_STRING=\"'testCONNECTION_STRING'\" PASSWORD=\"'testPASSWORD'\" DESCRIPTION=\"'testDESCRIPTION'\" DRIVER_NAME=\"'testDRIVER_NAME'\" DB_LINK_NAME=\"'testDB_LINK_NAME'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_databases>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_databases ( USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME, DB_LINK_NAME ) values "
      + "( 'testUSERNAME', 'testVERSION_NUMBER', 'testTYPE_NAME', 1, 'testCONNECTION_NAME', 'testCONNECTION_STRING', 'testPASSWORD', 'testDESCRIPTION', 'testDRIVER_NAME', 'testDB_LINK_NAME' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUsername() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUsername(Meta_databasesTest.testStringGenerator("anotherUSERNAME", 30));
    assertEquals(Meta_databasesTest.testStringGenerator("anotherUSERNAME", 30), USERNAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_databasesTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_databasesTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetType_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setType_name(Meta_databasesTest.testStringGenerator("anotherTYPE_NAME", 15));
    assertEquals(Meta_databasesTest.testStringGenerator("anotherTYPE_NAME", 15), TYPE_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetConnection_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setConnection_id(555L);
    assertEquals(555L, CONNECTION_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetConnection_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setConnection_name(Meta_databasesTest.testStringGenerator("anotherCONNECTION_NAME", 30));
    assertEquals(Meta_databasesTest.testStringGenerator("anotherCONNECTION_NAME", 30), CONNECTION_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetConnection_string() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setConnection_string(Meta_databasesTest.testStringGenerator("anotherCONNECTION_STRING", 400));
    assertEquals(Meta_databasesTest.testStringGenerator("anotherCONNECTION_STRING", 400), CONNECTION_STRING.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPassword() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPassword(Meta_databasesTest.testStringGenerator("anotherPASSWORD", 30));
    assertEquals(Meta_databasesTest.testStringGenerator("anotherPASSWORD", 30), PASSWORD.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(Meta_databasesTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(Meta_databasesTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDriver_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDriver_name(Meta_databasesTest.testStringGenerator("anotherDRIVER_NAME", 100));
    assertEquals(Meta_databasesTest.testStringGenerator("anotherDRIVER_NAME", 100), DRIVER_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDb_link_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDb_link_name(Meta_databasesTest.testStringGenerator("anotherDB_LINK_NAME", 128));
    assertEquals(Meta_databasesTest.testStringGenerator("anotherDB_LINK_NAME", 128), DB_LINK_NAME.get(objUnderTest));
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
    String[] expectedKeys = { "VERSION_NUMBER","CONNECTION_ID"};

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
    objUnderTest = new Meta_databases(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  USERNAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TYPE_NAME.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + CONNECTION_NAME.get(objUnderTest)  + ", " + CONNECTION_STRING.get(objUnderTest)  + ", " + PASSWORD.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + DRIVER_NAME.get(objUnderTest)  + ", " + DB_LINK_NAME.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", 0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", " ;
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
    Meta_databases compareObj = new Meta_databases(rockFactory ,  "testVERSION_NUMBER",  1L );

    /* Testing first with null primary key value */
    VERSION_NUMBER.set(objUnderTest, null);
  	CONNECTION_ID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSION_NUMBER.set(objUnderTest,  "different");
  	CONNECTION_ID.set(objUnderTest,  7L );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSION_NUMBER.set(objUnderTest,  "testVERSION_NUMBER");
  	CONNECTION_ID.set(objUnderTest,  1L );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_databases with our current one. If the two
   * Meta_databasess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_databases() throws Exception {

    /* Creating another Meta_databases which will be compared to the tested one */
    Meta_databases comparedObj = new Meta_databases(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_databases with our current one. If the two
   * Meta_databasess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_databases() throws Exception {

    /* Creating another Meta_databases which will be compared to the tested one */
    Meta_databases comparedObj = new Meta_databases(rockFactory ,  "testVERSION_NUMBER",  1L );
    comparedObj.setUsername( "DifferentUSERNAME");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_databases with our current one. If the two
   * Meta_databasess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_databases() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_databases comparedObj = new Meta_databases(rockFactory ,  "testVERSION_NUMBER",  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_databases with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_databases() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_databases comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_databases was null \n");
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
    assertEquals(Meta_databases.class, actualObject.getClass());
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
    Meta_databases testAgg = new Meta_databases(rockFactory ,  "testVERSION_NUMBER",  1L );
    USERNAME.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Username.
   */
  @Test
  public void testGetUsernameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getUsernameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Username.
  */
  @Test
  public void testGetUsernameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUsernameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Username.
  */
  @Test
  public void testGetUsernameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUsernameSQLType());    
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
   * Testing columnsize retrieving for Type_name.
   */
  @Test
  public void testGetType_nameColumnSize() throws Exception {
    
     assertEquals(15, objUnderTest.getType_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Type_name.
  */
  @Test
  public void testGetType_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getType_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Type_name.
  */
  @Test
  public void testGetType_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getType_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Connection_id.
   */
  @Test
  public void testGetConnection_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getConnection_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Connection_id.
  */
  @Test
  public void testGetConnection_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getConnection_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Connection_id.
  */
  @Test
  public void testGetConnection_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getConnection_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Connection_name.
   */
  @Test
  public void testGetConnection_nameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getConnection_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Connection_name.
  */
  @Test
  public void testGetConnection_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getConnection_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Connection_name.
  */
  @Test
  public void testGetConnection_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getConnection_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Connection_string.
   */
  @Test
  public void testGetConnection_stringColumnSize() throws Exception {
    
     assertEquals(400, objUnderTest.getConnection_stringColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Connection_string.
  */
  @Test
  public void testGetConnection_stringDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getConnection_stringDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Connection_string.
  */
  @Test
  public void testGetConnection_stringSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getConnection_stringSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Password.
   */
  @Test
  public void testGetPasswordColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getPasswordColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Password.
  */
  @Test
  public void testGetPasswordDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPasswordDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Password.
  */
  @Test
  public void testGetPasswordSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPasswordSQLType());    
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
   * Testing columnsize retrieving for Driver_name.
   */
  @Test
  public void testGetDriver_nameColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getDriver_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Driver_name.
  */
  @Test
  public void testGetDriver_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDriver_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Driver_name.
  */
  @Test
  public void testGetDriver_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDriver_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Db_link_name.
   */
  @Test
  public void testGetDb_link_nameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getDb_link_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Db_link_name.
  */
  @Test
  public void testGetDb_link_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDb_link_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Db_link_name.
  */
  @Test
  public void testGetDb_link_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDb_link_nameSQLType());    
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
   * Testing original Meta_databases object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_databases(rockFactory, false);
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
    Meta_databases changedOriginal = new Meta_databases(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_databases(rockFactory, false);
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
    Meta_databases changedOriginal = new Meta_databases(rockFactory, false);
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