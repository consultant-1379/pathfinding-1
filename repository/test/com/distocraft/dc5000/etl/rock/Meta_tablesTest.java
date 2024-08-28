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
 * Test class for Meta_tables. Changes to Meta_tables table are made via
 * this class.
 */
public class Meta_tablesTest {

  private static Meta_tables objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field TABLE_ID;
  
  private static Field TABLE_NAME;
  
  private static Field VERSION_NUMBER;
  
  private static Field IS_JOIN;
  
  private static Field JOIN_CLAUSE;
  
  private static Field TABLES_AND_ALIASES;
  
  private static Field CONNECTION_ID;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_tables.class.getDeclaredField("newItem");
		TABLE_ID = Meta_tables.class.getDeclaredField("TABLE_ID");
		TABLE_NAME = Meta_tables.class.getDeclaredField("TABLE_NAME");
		VERSION_NUMBER = Meta_tables.class.getDeclaredField("VERSION_NUMBER");
		IS_JOIN = Meta_tables.class.getDeclaredField("IS_JOIN");
		JOIN_CLAUSE = Meta_tables.class.getDeclaredField("JOIN_CLAUSE");
		TABLES_AND_ALIASES = Meta_tables.class.getDeclaredField("TABLES_AND_ALIASES");
		CONNECTION_ID = Meta_tables.class.getDeclaredField("CONNECTION_ID");
		newItem.setAccessible(true);
		TABLE_ID.setAccessible(true);
		TABLE_NAME.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		IS_JOIN.setAccessible(true);
		JOIN_CLAUSE.setAccessible(true);
		TABLES_AND_ALIASES.setAccessible(true);
		CONNECTION_ID.setAccessible(true);
	  timeStampName = Meta_tables.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_tables ( TABLE_ID BIGINT  ,TABLE_NAME VARCHAR(31) ,VERSION_NUMBER VARCHAR(31) ,IS_JOIN VARCHAR(31) ,JOIN_CLAUSE VARCHAR(31) ,TABLES_AND_ALIASES VARCHAR(31) ,CONNECTION_ID BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_tables");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_tables VALUES( 1  ,'testTABLE_NAME'  ,'testVERSION_NUMBER'  ,'testIS_JOIN'  ,'testJOIN_CLAUSE'  ,'testTABLES_AND_ALIASES'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_tables(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_tables");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_tables constructor variable initialization with null values.
   */
  @Test
  public void testMeta_tablesConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_tables(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  TABLE_ID.get(objUnderTest)  + ", " + TABLE_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + IS_JOIN.get(objUnderTest)  + ", " + JOIN_CLAUSE.get(objUnderTest)  + ", " + TABLES_AND_ALIASES.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_tables constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_tablesConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_tables(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );

    /* Asserting that variables are initialized */
    String actual =  TABLE_ID.get(objUnderTest)  + ", " + TABLE_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + IS_JOIN.get(objUnderTest)  + ", " + JOIN_CLAUSE.get(objUnderTest)  + ", " + TABLES_AND_ALIASES.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest) ;
    String expected =  "1"  + ", testTABLE_NAME"  + ", testVERSION_NUMBER"  + ", testIS_JOIN"  + ", testJOIN_CLAUSE"  + ", testTABLES_AND_ALIASES"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_tablesConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_tables(null ,  1L ,  "testVERSION_NUMBER",  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_tables constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_tablesConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_tables whereObject = new Meta_tables(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_tables(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  TABLE_ID.get(objUnderTest)  + ", " + TABLE_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + IS_JOIN.get(objUnderTest)  + ", " + JOIN_CLAUSE.get(objUnderTest)  + ", " + TABLES_AND_ALIASES.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest) ;
    String expected =  "1"  + ", testTABLE_NAME"  + ", testVERSION_NUMBER"  + ", testIS_JOIN"  + ", testJOIN_CLAUSE"  + ", testTABLES_AND_ALIASES"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_tablesConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_tables whereObject = new Meta_tables(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_tables(null, whereObject);
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
    assertEquals("Meta_tables", objUnderTest.getTableName());
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
    Meta_tables whereObject = new Meta_tables(rockFactory);

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
    Meta_tables whereObject = new Meta_tables(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_tables");
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
    TABLE_ID.set(objUnderTest, 1L);
    HashSet testSet = new HashSet();
    testSet.add("TABLE_ID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_tables");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the TABLE_ID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT TABLE_ID FROM Meta_tables");
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
    
    String expected = "<Meta_tables TABLE_ID=\"1\" TABLE_NAME=\"'testTABLE_NAME'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" IS_JOIN=\"'testIS_JOIN'\" JOIN_CLAUSE=\"'testJOIN_CLAUSE'\" TABLES_AND_ALIASES=\"'testTABLES_AND_ALIASES'\" CONNECTION_ID=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_tables TABLE_ID=\"1\" TABLE_NAME=\"'testTABLE_NAME'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" IS_JOIN=\"'testIS_JOIN'\" JOIN_CLAUSE=\"'testJOIN_CLAUSE'\" TABLES_AND_ALIASES=\"'testTABLES_AND_ALIASES'\" CONNECTION_ID=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_tables>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_tables ( TABLE_ID, TABLE_NAME, VERSION_NUMBER, IS_JOIN, JOIN_CLAUSE, TABLES_AND_ALIASES, CONNECTION_ID ) values "
      + "( 1, 'testTABLE_NAME', 'testVERSION_NUMBER', 'testIS_JOIN', 'testJOIN_CLAUSE', 'testTABLES_AND_ALIASES', 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTable_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTable_id(555L);
    assertEquals(555L, TABLE_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTable_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTable_name(Meta_tablesTest.testStringGenerator("anotherTABLE_NAME", 60));
    assertEquals(Meta_tablesTest.testStringGenerator("anotherTABLE_NAME", 60), TABLE_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_tablesTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_tablesTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIs_join() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIs_join(Meta_tablesTest.testStringGenerator("anotherIS_JOIN", 1));
    assertEquals(Meta_tablesTest.testStringGenerator("anotherIS_JOIN", 1), IS_JOIN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetJoin_clause() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setJoin_clause(Meta_tablesTest.testStringGenerator("anotherJOIN_CLAUSE", 2000));
    assertEquals(Meta_tablesTest.testStringGenerator("anotherJOIN_CLAUSE", 2000), JOIN_CLAUSE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTables_and_aliases() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTables_and_aliases(Meta_tablesTest.testStringGenerator("anotherTABLES_AND_ALIASES", 2000));
    assertEquals(Meta_tablesTest.testStringGenerator("anotherTABLES_AND_ALIASES", 2000), TABLES_AND_ALIASES.get(objUnderTest));
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
    String[] expectedKeys = { "TABLE_ID","VERSION_NUMBER","CONNECTION_ID"};

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
    objUnderTest = new Meta_tables(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  TABLE_ID.get(objUnderTest)  + ", " + TABLE_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + IS_JOIN.get(objUnderTest)  + ", " + JOIN_CLAUSE.get(objUnderTest)  + ", " + TABLES_AND_ALIASES.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest) ;
    String expected =  "0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0" ;
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
    Meta_tables compareObj = new Meta_tables(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );

    /* Testing first with null primary key value */
    TABLE_ID.set(objUnderTest, null);
  	VERSION_NUMBER.set(objUnderTest, null);
  	CONNECTION_ID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    TABLE_ID.set(objUnderTest,  7L );
  	VERSION_NUMBER.set(objUnderTest,  "different");
  	CONNECTION_ID.set(objUnderTest,  7L );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    TABLE_ID.set(objUnderTest,  1L );
  	VERSION_NUMBER.set(objUnderTest,  "testVERSION_NUMBER");
  	CONNECTION_ID.set(objUnderTest,  1L );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_tables with our current one. If the two
   * Meta_tabless are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_tables() throws Exception {

    /* Creating another Meta_tables which will be compared to the tested one */
    Meta_tables comparedObj = new Meta_tables(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_tables with our current one. If the two
   * Meta_tabless are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_tables() throws Exception {

    /* Creating another Meta_tables which will be compared to the tested one */
    Meta_tables comparedObj = new Meta_tables(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );
    comparedObj.setTable_id( 7L );

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_tables with our current one. If the two
   * Meta_tabless are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_tables() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_tables comparedObj = new Meta_tables(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_tables with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_tables() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_tables comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_tables was null \n");
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
    assertEquals(Meta_tables.class, actualObject.getClass());
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
    Meta_tables testAgg = new Meta_tables(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L );
    TABLE_ID.set(objUnderTest, 7L);

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Table_id.
   */
  @Test
  public void testGetTable_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getTable_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Table_id.
  */
  @Test
  public void testGetTable_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTable_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Table_id.
  */
  @Test
  public void testGetTable_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getTable_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Table_name.
   */
  @Test
  public void testGetTable_nameColumnSize() throws Exception {
    
     assertEquals(60, objUnderTest.getTable_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Table_name.
  */
  @Test
  public void testGetTable_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTable_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Table_name.
  */
  @Test
  public void testGetTable_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTable_nameSQLType());    
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
   * Testing columnsize retrieving for Is_join.
   */
  @Test
  public void testGetIs_joinColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getIs_joinColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Is_join.
  */
  @Test
  public void testGetIs_joinDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIs_joinDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Is_join.
  */
  @Test
  public void testGetIs_joinSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getIs_joinSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Join_clause.
   */
  @Test
  public void testGetJoin_clauseColumnSize() throws Exception {
    
     assertEquals(2000, objUnderTest.getJoin_clauseColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Join_clause.
  */
  @Test
  public void testGetJoin_clauseDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getJoin_clauseDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Join_clause.
  */
  @Test
  public void testGetJoin_clauseSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getJoin_clauseSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Tables_and_aliases.
   */
  @Test
  public void testGetTables_and_aliasesColumnSize() throws Exception {
    
     assertEquals(2000, objUnderTest.getTables_and_aliasesColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Tables_and_aliases.
  */
  @Test
  public void testGetTables_and_aliasesDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTables_and_aliasesDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Tables_and_aliases.
  */
  @Test
  public void testGetTables_and_aliasesSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTables_and_aliasesSQLType());    
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
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Meta_tables object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_tables(rockFactory, false);
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
    Meta_tables changedOriginal = new Meta_tables(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_tables(rockFactory, false);
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
    Meta_tables changedOriginal = new Meta_tables(rockFactory, false);
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