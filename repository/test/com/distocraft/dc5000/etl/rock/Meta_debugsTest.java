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
 * Test class for Meta_debugs. Changes to Meta_debugs table are made via
 * this class.
 */
public class Meta_debugsTest {

  private static Meta_debugs objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field ID;
  
  private static Field TEXT;
  
  private static Field LAST_UPDATED;
  
  private static Field VERSION_NUMBER;
  
  private static Field COLLECTION_SET_ID;
  
  private static Field COLLECTION_ID;
  
  private static Field TRANSFER_BATCH_ID;
  
  private static Field TRANSFER_ACTION_ID;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_debugs.class.getDeclaredField("newItem");
		ID = Meta_debugs.class.getDeclaredField("ID");
		TEXT = Meta_debugs.class.getDeclaredField("TEXT");
		LAST_UPDATED = Meta_debugs.class.getDeclaredField("LAST_UPDATED");
		VERSION_NUMBER = Meta_debugs.class.getDeclaredField("VERSION_NUMBER");
		COLLECTION_SET_ID = Meta_debugs.class.getDeclaredField("COLLECTION_SET_ID");
		COLLECTION_ID = Meta_debugs.class.getDeclaredField("COLLECTION_ID");
		TRANSFER_BATCH_ID = Meta_debugs.class.getDeclaredField("TRANSFER_BATCH_ID");
		TRANSFER_ACTION_ID = Meta_debugs.class.getDeclaredField("TRANSFER_ACTION_ID");
		newItem.setAccessible(true);
		ID.setAccessible(true);
		TEXT.setAccessible(true);
		LAST_UPDATED.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		COLLECTION_SET_ID.setAccessible(true);
		COLLECTION_ID.setAccessible(true);
		TRANSFER_BATCH_ID.setAccessible(true);
		TRANSFER_ACTION_ID.setAccessible(true);
	  timeStampName = Meta_debugs.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_debugs ( ID BIGINT  ,TEXT VARCHAR(31) ,LAST_UPDATED TIMESTAMP  ,VERSION_NUMBER VARCHAR(31) ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,TRANSFER_BATCH_ID BIGINT  ,TRANSFER_ACTION_ID BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_debugs");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_debugs VALUES( 1  ,'testTEXT'  ,'2000-01-01 00:00:00.0'  ,'testVERSION_NUMBER'  ,1  ,1  ,1  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_debugs(rockFactory ,  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_debugs");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_debugs constructor variable initialization with null values.
   */
  @Test
  public void testMeta_debugsConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_debugs(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  ID.get(objUnderTest)  + ", " + TEXT.get(objUnderTest)  + ", " + LAST_UPDATED.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + TRANSFER_BATCH_ID.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_debugs constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_debugsConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_debugs(rockFactory ,  1L );

    /* Asserting that variables are initialized */
    String actual =  ID.get(objUnderTest)  + ", " + TEXT.get(objUnderTest)  + ", " + LAST_UPDATED.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + TRANSFER_BATCH_ID.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest) ;
    String expected =  "1"  + ", testTEXT"  + ", 2000-01-01 00:00:00.0"  + ", testVERSION_NUMBER"  + ", 1"  + ", 1"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_debugsConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_debugs(null ,  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_debugs constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_debugsConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_debugs whereObject = new Meta_debugs(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_debugs(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  ID.get(objUnderTest)  + ", " + TEXT.get(objUnderTest)  + ", " + LAST_UPDATED.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + TRANSFER_BATCH_ID.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest) ;
    String expected =  "1"  + ", testTEXT"  + ", 2000-01-01 00:00:00.0"  + ", testVERSION_NUMBER"  + ", 1"  + ", 1"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_debugsConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_debugs whereObject = new Meta_debugs(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_debugs(null, whereObject);
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
    assertEquals("Meta_debugs", objUnderTest.getTableName());
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
    Meta_debugs whereObject = new Meta_debugs(rockFactory);

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
    Meta_debugs whereObject = new Meta_debugs(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_debugs");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_debugs");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the ID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT ID FROM Meta_debugs");
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
    
    String expected = "<Meta_debugs ID=\"1\" TEXT=\"'testTEXT'\" LAST_UPDATED=\"'2000-01-01 00:00:00.0'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" COLLECTION_SET_ID=\"1\" COLLECTION_ID=\"1\" TRANSFER_BATCH_ID=\"1\" TRANSFER_ACTION_ID=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_debugs ID=\"1\" TEXT=\"'testTEXT'\" LAST_UPDATED=\"'2000-01-01 00:00:00.0'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" COLLECTION_SET_ID=\"1\" COLLECTION_ID=\"1\" TRANSFER_BATCH_ID=\"1\" TRANSFER_ACTION_ID=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_debugs>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_debugs ( ID, TEXT, LAST_UPDATED, VERSION_NUMBER, COLLECTION_SET_ID, COLLECTION_ID, TRANSFER_BATCH_ID, TRANSFER_ACTION_ID ) values "
      + "( 1, 'testTEXT', '2000-01-01 00:00:00.0', 'testVERSION_NUMBER', 1, 1, 1, 1 );\n";
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
  public void testSetAndGetText() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setText(Meta_debugsTest.testStringGenerator("anotherTEXT", 2000));
    assertEquals(Meta_debugsTest.testStringGenerator("anotherTEXT", 2000), TEXT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLast_updated() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLast_updated(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), LAST_UPDATED.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_debugsTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_debugsTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
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
  public void testSetAndGetTransfer_batch_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTransfer_batch_id(555L);
    assertEquals(555L, TRANSFER_BATCH_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTransfer_action_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTransfer_action_id(555L);
    assertEquals(555L, TRANSFER_ACTION_ID.get(objUnderTest));
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
    objUnderTest = new Meta_debugs(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  ID.get(objUnderTest)  + ", " + TEXT.get(objUnderTest)  + ", " + LAST_UPDATED.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + TRANSFER_BATCH_ID.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest) ;
    String expected =  "0"  + ", "  + ", " + new Timestamp(0)  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0" ;
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
    Meta_debugs compareObj = new Meta_debugs(rockFactory ,  1L );

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
   * Testing comparing another Meta_debugs with our current one. If the two
   * Meta_debugss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_debugs() throws Exception {

    /* Creating another Meta_debugs which will be compared to the tested one */
    Meta_debugs comparedObj = new Meta_debugs(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_debugs with our current one. If the two
   * Meta_debugss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_debugs() throws Exception {

    /* Creating another Meta_debugs which will be compared to the tested one */
    Meta_debugs comparedObj = new Meta_debugs(rockFactory ,  1L );
    comparedObj.setId( 7L );

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_debugs with our current one. If the two
   * Meta_debugss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_debugs() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_debugs comparedObj = new Meta_debugs(rockFactory ,  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_debugs with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_debugs() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_debugs comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_debugs was null \n");
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
    assertEquals(Meta_debugs.class, actualObject.getClass());
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
    Meta_debugs testAgg = new Meta_debugs(rockFactory ,  1L );
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
   * Testing columnsize retrieving for Text.
   */
  @Test
  public void testGetTextColumnSize() throws Exception {
    
     assertEquals(2000, objUnderTest.getTextColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Text.
  */
  @Test
  public void testGetTextDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTextDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Text.
  */
  @Test
  public void testGetTextSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTextSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Last_updated.
   */
  @Test
  public void testGetLast_updatedColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getLast_updatedColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Last_updated.
  */
  @Test
  public void testGetLast_updatedDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLast_updatedDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Last_updated.
  */
  @Test
  public void testGetLast_updatedSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getLast_updatedSQLType());    
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
   * Testing columnsize retrieving for Transfer_batch_id.
   */
  @Test
  public void testGetTransfer_batch_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getTransfer_batch_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Transfer_batch_id.
  */
  @Test
  public void testGetTransfer_batch_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTransfer_batch_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Transfer_batch_id.
  */
  @Test
  public void testGetTransfer_batch_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getTransfer_batch_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Transfer_action_id.
   */
  @Test
  public void testGetTransfer_action_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getTransfer_action_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Transfer_action_id.
  */
  @Test
  public void testGetTransfer_action_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTransfer_action_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Transfer_action_id.
  */
  @Test
  public void testGetTransfer_action_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getTransfer_action_idSQLType());    
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
   * Testing original Meta_debugs object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_debugs(rockFactory, false);
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
    Meta_debugs changedOriginal = new Meta_debugs(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_debugs(rockFactory, false);
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
    Meta_debugs changedOriginal = new Meta_debugs(rockFactory, false);
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