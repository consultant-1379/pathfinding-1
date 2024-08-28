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
 * Test class for Meta_versions. Changes to Meta_versions table are made via
 * this class.
 */
public class Meta_versionsTest {

  private static Meta_versions objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSION_NUMBER;
  
  private static Field DESCRIPTION;
  
  private static Field CURRENT_FLAG;
  
  private static Field IS_PREDEFINED;
  
  private static Field ENGINE_SERVER;
  
  private static Field MAIL_SERVER;
  
  private static Field SCHEDULER_SERVER;
  
  private static Field MAIL_SERVER_PORT;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_versions.class.getDeclaredField("newItem");
		VERSION_NUMBER = Meta_versions.class.getDeclaredField("VERSION_NUMBER");
		DESCRIPTION = Meta_versions.class.getDeclaredField("DESCRIPTION");
		CURRENT_FLAG = Meta_versions.class.getDeclaredField("CURRENT_FLAG");
		IS_PREDEFINED = Meta_versions.class.getDeclaredField("IS_PREDEFINED");
		ENGINE_SERVER = Meta_versions.class.getDeclaredField("ENGINE_SERVER");
		MAIL_SERVER = Meta_versions.class.getDeclaredField("MAIL_SERVER");
		SCHEDULER_SERVER = Meta_versions.class.getDeclaredField("SCHEDULER_SERVER");
		MAIL_SERVER_PORT = Meta_versions.class.getDeclaredField("MAIL_SERVER_PORT");
		newItem.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		CURRENT_FLAG.setAccessible(true);
		IS_PREDEFINED.setAccessible(true);
		ENGINE_SERVER.setAccessible(true);
		MAIL_SERVER.setAccessible(true);
		SCHEDULER_SERVER.setAccessible(true);
		MAIL_SERVER_PORT.setAccessible(true);
	  timeStampName = Meta_versions.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_versions ( VERSION_NUMBER VARCHAR(31)  ,DESCRIPTION VARCHAR(31) ,CURRENT_FLAG VARCHAR(31) ,IS_PREDEFINED VARCHAR(31) ,ENGINE_SERVER VARCHAR(31) ,MAIL_SERVER VARCHAR(31) ,SCHEDULER_SERVER VARCHAR(31) ,MAIL_SERVER_PORT BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_versions");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_versions VALUES( 'testVERSION_NUMBER'  ,'testDESCRIPTION'  ,'testCURRENT_FLAG'  ,'testIS_PREDEFINED'  ,'testENGINE_SERVER'  ,'testMAIL_SERVER'  ,'testSCHEDULER_SERVER'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_versions(rockFactory ,  "testVERSION_NUMBER");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_versions");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_versions constructor variable initialization with null values.
   */
  @Test
  public void testMeta_versionsConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_versions(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSION_NUMBER.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + CURRENT_FLAG.get(objUnderTest)  + ", " + IS_PREDEFINED.get(objUnderTest)  + ", " + ENGINE_SERVER.get(objUnderTest)  + ", " + MAIL_SERVER.get(objUnderTest)  + ", " + SCHEDULER_SERVER.get(objUnderTest)  + ", " + MAIL_SERVER_PORT.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_versions constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_versionsConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_versions(rockFactory ,  "testVERSION_NUMBER");

    /* Asserting that variables are initialized */
    String actual =  VERSION_NUMBER.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + CURRENT_FLAG.get(objUnderTest)  + ", " + IS_PREDEFINED.get(objUnderTest)  + ", " + ENGINE_SERVER.get(objUnderTest)  + ", " + MAIL_SERVER.get(objUnderTest)  + ", " + SCHEDULER_SERVER.get(objUnderTest)  + ", " + MAIL_SERVER_PORT.get(objUnderTest) ;
    String expected =  "testVERSION_NUMBER"  + ", testDESCRIPTION"  + ", testCURRENT_FLAG"  + ", testIS_PREDEFINED"  + ", testENGINE_SERVER"  + ", testMAIL_SERVER"  + ", testSCHEDULER_SERVER"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_versionsConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_versions(null ,  "testVERSION_NUMBER");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_versions constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_versionsConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_versions whereObject = new Meta_versions(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_versions(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSION_NUMBER.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + CURRENT_FLAG.get(objUnderTest)  + ", " + IS_PREDEFINED.get(objUnderTest)  + ", " + ENGINE_SERVER.get(objUnderTest)  + ", " + MAIL_SERVER.get(objUnderTest)  + ", " + SCHEDULER_SERVER.get(objUnderTest)  + ", " + MAIL_SERVER_PORT.get(objUnderTest) ;
    String expected =  "testVERSION_NUMBER"  + ", testDESCRIPTION"  + ", testCURRENT_FLAG"  + ", testIS_PREDEFINED"  + ", testENGINE_SERVER"  + ", testMAIL_SERVER"  + ", testSCHEDULER_SERVER"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_versionsConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_versions whereObject = new Meta_versions(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_versions(null, whereObject);
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
    assertEquals("Meta_versions", objUnderTest.getTableName());
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
    Meta_versions whereObject = new Meta_versions(rockFactory);

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
    Meta_versions whereObject = new Meta_versions(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_versions");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_versions");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSION_NUMBER column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSION_NUMBER FROM Meta_versions");
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
    
    String expected = "<Meta_versions VERSION_NUMBER=\"'testVERSION_NUMBER'\" DESCRIPTION=\"'testDESCRIPTION'\" CURRENT_FLAG=\"'testCURRENT_FLAG'\" IS_PREDEFINED=\"'testIS_PREDEFINED'\" ENGINE_SERVER=\"'testENGINE_SERVER'\" MAIL_SERVER=\"'testMAIL_SERVER'\" SCHEDULER_SERVER=\"'testSCHEDULER_SERVER'\" MAIL_SERVER_PORT=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_versions VERSION_NUMBER=\"'testVERSION_NUMBER'\" DESCRIPTION=\"'testDESCRIPTION'\" CURRENT_FLAG=\"'testCURRENT_FLAG'\" IS_PREDEFINED=\"'testIS_PREDEFINED'\" ENGINE_SERVER=\"'testENGINE_SERVER'\" MAIL_SERVER=\"'testMAIL_SERVER'\" SCHEDULER_SERVER=\"'testSCHEDULER_SERVER'\" MAIL_SERVER_PORT=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_versions>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_versions ( VERSION_NUMBER, DESCRIPTION, CURRENT_FLAG, IS_PREDEFINED, ENGINE_SERVER, MAIL_SERVER, SCHEDULER_SERVER, MAIL_SERVER_PORT ) values "
      + "( 'testVERSION_NUMBER', 'testDESCRIPTION', 'testCURRENT_FLAG', 'testIS_PREDEFINED', 'testENGINE_SERVER', 'testMAIL_SERVER', 'testSCHEDULER_SERVER', 1 );\n";
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
    objUnderTest.setVersion_number(Meta_versionsTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_versionsTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(Meta_versionsTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(Meta_versionsTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCurrent_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCurrent_flag(Meta_versionsTest.testStringGenerator("anotherCURRENT_FLAG", 1));
    assertEquals(Meta_versionsTest.testStringGenerator("anotherCURRENT_FLAG", 1), CURRENT_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIs_predefined() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIs_predefined(Meta_versionsTest.testStringGenerator("anotherIS_PREDEFINED", 1));
    assertEquals(Meta_versionsTest.testStringGenerator("anotherIS_PREDEFINED", 1), IS_PREDEFINED.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetEngine_server() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setEngine_server(Meta_versionsTest.testStringGenerator("anotherENGINE_SERVER", 50));
    assertEquals(Meta_versionsTest.testStringGenerator("anotherENGINE_SERVER", 50), ENGINE_SERVER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMail_server() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMail_server(Meta_versionsTest.testStringGenerator("anotherMAIL_SERVER", 100));
    assertEquals(Meta_versionsTest.testStringGenerator("anotherMAIL_SERVER", 100), MAIL_SERVER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetScheduler_server() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setScheduler_server(Meta_versionsTest.testStringGenerator("anotherSCHEDULER_SERVER", 50));
    assertEquals(Meta_versionsTest.testStringGenerator("anotherSCHEDULER_SERVER", 50), SCHEDULER_SERVER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMail_server_port() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMail_server_port(555L);
    assertEquals(555L, MAIL_SERVER_PORT.get(objUnderTest));
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
    objUnderTest = new Meta_versions(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSION_NUMBER.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + CURRENT_FLAG.get(objUnderTest)  + ", " + IS_PREDEFINED.get(objUnderTest)  + ", " + ENGINE_SERVER.get(objUnderTest)  + ", " + MAIL_SERVER.get(objUnderTest)  + ", " + SCHEDULER_SERVER.get(objUnderTest)  + ", " + MAIL_SERVER_PORT.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0" ;
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
    Meta_versions compareObj = new Meta_versions(rockFactory ,  "testVERSION_NUMBER");

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
   * Testing comparing another Meta_versions with our current one. If the two
   * Meta_versionss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_versions() throws Exception {

    /* Creating another Meta_versions which will be compared to the tested one */
    Meta_versions comparedObj = new Meta_versions(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_versions with our current one. If the two
   * Meta_versionss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_versions() throws Exception {

    /* Creating another Meta_versions which will be compared to the tested one */
    Meta_versions comparedObj = new Meta_versions(rockFactory ,  "testVERSION_NUMBER");
    comparedObj.setVersion_number( "DifferentVERSION_NUMBER");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_versions with our current one. If the two
   * Meta_versionss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_versions() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_versions comparedObj = new Meta_versions(rockFactory ,  "testVERSION_NUMBER");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_versions with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_versions() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_versions comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_versions was null \n");
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
    assertEquals(Meta_versions.class, actualObject.getClass());
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
    Meta_versions testAgg = new Meta_versions(rockFactory ,  "testVERSION_NUMBER");
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
   * Testing columnsize retrieving for Current_flag.
   */
  @Test
  public void testGetCurrent_flagColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getCurrent_flagColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Current_flag.
  */
  @Test
  public void testGetCurrent_flagDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCurrent_flagDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Current_flag.
  */
  @Test
  public void testGetCurrent_flagSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getCurrent_flagSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Is_predefined.
   */
  @Test
  public void testGetIs_predefinedColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getIs_predefinedColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Is_predefined.
  */
  @Test
  public void testGetIs_predefinedDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIs_predefinedDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Is_predefined.
  */
  @Test
  public void testGetIs_predefinedSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getIs_predefinedSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Engine_server.
   */
  @Test
  public void testGetEngine_serverColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getEngine_serverColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Engine_server.
  */
  @Test
  public void testGetEngine_serverDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getEngine_serverDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Engine_server.
  */
  @Test
  public void testGetEngine_serverSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getEngine_serverSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Mail_server.
   */
  @Test
  public void testGetMail_serverColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getMail_serverColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Mail_server.
  */
  @Test
  public void testGetMail_serverDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMail_serverDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Mail_server.
  */
  @Test
  public void testGetMail_serverSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMail_serverSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Scheduler_server.
   */
  @Test
  public void testGetScheduler_serverColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getScheduler_serverColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Scheduler_server.
  */
  @Test
  public void testGetScheduler_serverDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getScheduler_serverDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Scheduler_server.
  */
  @Test
  public void testGetScheduler_serverSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getScheduler_serverSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Mail_server_port.
   */
  @Test
  public void testGetMail_server_portColumnSize() throws Exception {
    
     assertEquals(5, objUnderTest.getMail_server_portColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Mail_server_port.
  */
  @Test
  public void testGetMail_server_portDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMail_server_portDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Mail_server_port.
  */
  @Test
  public void testGetMail_server_portSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getMail_server_portSQLType());    
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
   * Testing original Meta_versions object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_versions(rockFactory, false);
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
    Meta_versions changedOriginal = new Meta_versions(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_versions(rockFactory, false);
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
    Meta_versions changedOriginal = new Meta_versions(rockFactory, false);
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