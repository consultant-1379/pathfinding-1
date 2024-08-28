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
 * Test class for Meta_collection_sets. Changes to Meta_collection_sets table are made via
 * this class.
 */
public class Meta_collection_setsTest {

  private static Meta_collection_sets objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field COLLECTION_SET_ID;
  
  private static Field COLLECTION_SET_NAME;
  
  private static Field DESCRIPTION;
  
  private static Field VERSION_NUMBER;
  
  private static Field ENABLED_FLAG;
  
  private static Field TYPE;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_collection_sets.class.getDeclaredField("newItem");
		COLLECTION_SET_ID = Meta_collection_sets.class.getDeclaredField("COLLECTION_SET_ID");
		COLLECTION_SET_NAME = Meta_collection_sets.class.getDeclaredField("COLLECTION_SET_NAME");
		DESCRIPTION = Meta_collection_sets.class.getDeclaredField("DESCRIPTION");
		VERSION_NUMBER = Meta_collection_sets.class.getDeclaredField("VERSION_NUMBER");
		ENABLED_FLAG = Meta_collection_sets.class.getDeclaredField("ENABLED_FLAG");
		TYPE = Meta_collection_sets.class.getDeclaredField("TYPE");
		newItem.setAccessible(true);
		COLLECTION_SET_ID.setAccessible(true);
		COLLECTION_SET_NAME.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		ENABLED_FLAG.setAccessible(true);
		TYPE.setAccessible(true);
	  timeStampName = Meta_collection_sets.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_collection_sets ( COLLECTION_SET_ID BIGINT  ,COLLECTION_SET_NAME VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,VERSION_NUMBER VARCHAR(31) ,ENABLED_FLAG VARCHAR(31) ,TYPE VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_collection_sets");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES( 1  ,'testCOLLECTION_SET_NAME'  ,'testDESCRIPTION'  ,'testVERSION_NUMBER'  ,'testENABLED_FLAG'  ,'testTYPE' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_collection_sets(rockFactory ,  1L ,  "testVERSION_NUMBER");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_collection_sets");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_collection_sets constructor variable initialization with null values.
   */
  @Test
  public void testMeta_collection_setsConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_collection_sets(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_SET_NAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + ENABLED_FLAG.get(objUnderTest)  + ", " + TYPE.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_collection_sets constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_collection_setsConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_collection_sets(rockFactory ,  1L ,  "testVERSION_NUMBER");

    /* Asserting that variables are initialized */
    String actual =  COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_SET_NAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + ENABLED_FLAG.get(objUnderTest)  + ", " + TYPE.get(objUnderTest) ;
    String expected =  "1"  + ", testCOLLECTION_SET_NAME"  + ", testDESCRIPTION"  + ", testVERSION_NUMBER"  + ", testENABLED_FLAG"  + ", testTYPE" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_collection_setsConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_collection_sets(null ,  1L ,  "testVERSION_NUMBER");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_collection_sets constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_collection_setsConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_collection_sets whereObject = new Meta_collection_sets(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_collection_sets(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_SET_NAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + ENABLED_FLAG.get(objUnderTest)  + ", " + TYPE.get(objUnderTest) ;
    String expected =  "1"  + ", testCOLLECTION_SET_NAME"  + ", testDESCRIPTION"  + ", testVERSION_NUMBER"  + ", testENABLED_FLAG"  + ", testTYPE" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_collection_setsConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_collection_sets whereObject = new Meta_collection_sets(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_collection_sets(null, whereObject);
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
    assertEquals("Meta_collection_sets", objUnderTest.getTableName());
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
    Meta_collection_sets whereObject = new Meta_collection_sets(rockFactory);

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
    Meta_collection_sets whereObject = new Meta_collection_sets(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_collection_sets");
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
    COLLECTION_SET_ID.set(objUnderTest, 1L);
    HashSet testSet = new HashSet();
    testSet.add("COLLECTION_SET_ID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_collection_sets");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the COLLECTION_SET_ID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT COLLECTION_SET_ID FROM Meta_collection_sets");
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
    
    String expected = "<Meta_collection_sets COLLECTION_SET_ID=\"1\" COLLECTION_SET_NAME=\"'testCOLLECTION_SET_NAME'\" DESCRIPTION=\"'testDESCRIPTION'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" ENABLED_FLAG=\"'testENABLED_FLAG'\" TYPE=\"'testTYPE'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_collection_sets COLLECTION_SET_ID=\"1\" COLLECTION_SET_NAME=\"'testCOLLECTION_SET_NAME'\" DESCRIPTION=\"'testDESCRIPTION'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" ENABLED_FLAG=\"'testENABLED_FLAG'\" TYPE=\"'testTYPE'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_collection_sets>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_collection_sets ( COLLECTION_SET_ID, COLLECTION_SET_NAME, DESCRIPTION, VERSION_NUMBER, ENABLED_FLAG, TYPE ) values "
      + "( 1, 'testCOLLECTION_SET_NAME', 'testDESCRIPTION', 'testVERSION_NUMBER', 'testENABLED_FLAG', 'testTYPE' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
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
  public void testSetAndGetCollection_set_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCollection_set_name(Meta_collection_setsTest.testStringGenerator("anotherCOLLECTION_SET_NAME", 128));
    assertEquals(Meta_collection_setsTest.testStringGenerator("anotherCOLLECTION_SET_NAME", 128), COLLECTION_SET_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(Meta_collection_setsTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(Meta_collection_setsTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_collection_setsTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_collection_setsTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetEnabled_flag() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setEnabled_flag(Meta_collection_setsTest.testStringGenerator("anotherENABLED_FLAG", 1));
    assertEquals(Meta_collection_setsTest.testStringGenerator("anotherENABLED_FLAG", 1), ENABLED_FLAG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetType() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setType(Meta_collection_setsTest.testStringGenerator("anotherTYPE", 32));
    assertEquals(Meta_collection_setsTest.testStringGenerator("anotherTYPE", 32), TYPE.get(objUnderTest));
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
    String[] expectedKeys = { "COLLECTION_SET_ID","VERSION_NUMBER"};

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
    objUnderTest = new Meta_collection_sets(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_SET_NAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + ENABLED_FLAG.get(objUnderTest)  + ", " + TYPE.get(objUnderTest) ;
    String expected =  "0"  + ", "  + ", "  + ", "  + ", "  + ", " ;
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
    Meta_collection_sets compareObj = new Meta_collection_sets(rockFactory ,  1L ,  "testVERSION_NUMBER");

    /* Testing first with null primary key value */
    COLLECTION_SET_ID.set(objUnderTest, null);
  	VERSION_NUMBER.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    COLLECTION_SET_ID.set(objUnderTest,  7L );
  	VERSION_NUMBER.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    COLLECTION_SET_ID.set(objUnderTest,  1L );
  	VERSION_NUMBER.set(objUnderTest,  "testVERSION_NUMBER");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_collection_sets with our current one. If the two
   * Meta_collection_setss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_collection_sets() throws Exception {

    /* Creating another Meta_collection_sets which will be compared to the tested one */
    Meta_collection_sets comparedObj = new Meta_collection_sets(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_collection_sets with our current one. If the two
   * Meta_collection_setss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_collection_sets() throws Exception {

    /* Creating another Meta_collection_sets which will be compared to the tested one */
    Meta_collection_sets comparedObj = new Meta_collection_sets(rockFactory ,  1L ,  "testVERSION_NUMBER");
    comparedObj.setCollection_set_id( 7L );

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_collection_sets with our current one. If the two
   * Meta_collection_setss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_collection_sets() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_collection_sets comparedObj = new Meta_collection_sets(rockFactory ,  1L ,  "testVERSION_NUMBER");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_collection_sets with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_collection_sets() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_collection_sets comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_collection_sets was null \n");
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
    assertEquals(Meta_collection_sets.class, actualObject.getClass());
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
    Meta_collection_sets testAgg = new Meta_collection_sets(rockFactory ,  1L ,  "testVERSION_NUMBER");
    COLLECTION_SET_ID.set(objUnderTest, 7L);

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
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
   * Testing columnsize retrieving for Collection_set_name.
   */
  @Test
  public void testGetCollection_set_nameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getCollection_set_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Collection_set_name.
  */
  @Test
  public void testGetCollection_set_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCollection_set_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Collection_set_name.
  */
  @Test
  public void testGetCollection_set_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getCollection_set_nameSQLType());    
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
   * Testing columnsize retrieving for Type.
   */
  @Test
  public void testGetTypeColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getTypeColumnSize());   
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
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Meta_collection_sets object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_collection_sets(rockFactory, false);
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
    Meta_collection_sets changedOriginal = new Meta_collection_sets(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_collection_sets(rockFactory, false);
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
    Meta_collection_sets changedOriginal = new Meta_collection_sets(rockFactory, false);
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