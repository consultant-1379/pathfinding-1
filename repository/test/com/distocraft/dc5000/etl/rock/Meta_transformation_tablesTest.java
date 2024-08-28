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
 * Test class for Meta_transformation_tables. Changes to Meta_transformation_tables table are made via
 * this class.
 */
public class Meta_transformation_tablesTest {

  private static Meta_transformation_tables objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field TRANSF_TABLE_ID;
  
  private static Field TRANSF_TABLE_NAME;
  
  private static Field DESCRIPTION;
  
  private static Field VERSION_NUMBER;
  
  private static Field IS_LOOKUP;
  
  private static Field CONNECTION_ID;
  
  private static Field TABLE_ID;
  
  private static Field KEY_COLUMN_ID;
  
  private static Field VALUE_COLUMN_ID;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_transformation_tables.class.getDeclaredField("newItem");
		TRANSF_TABLE_ID = Meta_transformation_tables.class.getDeclaredField("TRANSF_TABLE_ID");
		TRANSF_TABLE_NAME = Meta_transformation_tables.class.getDeclaredField("TRANSF_TABLE_NAME");
		DESCRIPTION = Meta_transformation_tables.class.getDeclaredField("DESCRIPTION");
		VERSION_NUMBER = Meta_transformation_tables.class.getDeclaredField("VERSION_NUMBER");
		IS_LOOKUP = Meta_transformation_tables.class.getDeclaredField("IS_LOOKUP");
		CONNECTION_ID = Meta_transformation_tables.class.getDeclaredField("CONNECTION_ID");
		TABLE_ID = Meta_transformation_tables.class.getDeclaredField("TABLE_ID");
		KEY_COLUMN_ID = Meta_transformation_tables.class.getDeclaredField("KEY_COLUMN_ID");
		VALUE_COLUMN_ID = Meta_transformation_tables.class.getDeclaredField("VALUE_COLUMN_ID");
		newItem.setAccessible(true);
		TRANSF_TABLE_ID.setAccessible(true);
		TRANSF_TABLE_NAME.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		IS_LOOKUP.setAccessible(true);
		CONNECTION_ID.setAccessible(true);
		TABLE_ID.setAccessible(true);
		KEY_COLUMN_ID.setAccessible(true);
		VALUE_COLUMN_ID.setAccessible(true);
	  timeStampName = Meta_transformation_tables.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_transformation_tables ( TRANSF_TABLE_ID BIGINT  ,TRANSF_TABLE_NAME VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,VERSION_NUMBER VARCHAR(31) ,IS_LOOKUP VARCHAR(31) ,CONNECTION_ID BIGINT  ,TABLE_ID BIGINT  ,KEY_COLUMN_ID BIGINT  ,VALUE_COLUMN_ID BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_transformation_tables");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_transformation_tables VALUES( 1  ,'testTRANSF_TABLE_NAME'  ,'testDESCRIPTION'  ,'testVERSION_NUMBER'  ,'testIS_LOOKUP'  ,1  ,1  ,1  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_transformation_tables(rockFactory ,  1L ,  "testVERSION_NUMBER");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_transformation_tables");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_transformation_tables constructor variable initialization with null values.
   */
  @Test
  public void testMeta_transformation_tablesConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transformation_tables(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  TRANSF_TABLE_ID.get(objUnderTest)  + ", " + TRANSF_TABLE_NAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + IS_LOOKUP.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest)  + ", " + KEY_COLUMN_ID.get(objUnderTest)  + ", " + VALUE_COLUMN_ID.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_transformation_tables constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_transformation_tablesConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transformation_tables(rockFactory ,  1L ,  "testVERSION_NUMBER");

    /* Asserting that variables are initialized */
    String actual =  TRANSF_TABLE_ID.get(objUnderTest)  + ", " + TRANSF_TABLE_NAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + IS_LOOKUP.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest)  + ", " + KEY_COLUMN_ID.get(objUnderTest)  + ", " + VALUE_COLUMN_ID.get(objUnderTest) ;
    String expected =  "1"  + ", testTRANSF_TABLE_NAME"  + ", testDESCRIPTION"  + ", testVERSION_NUMBER"  + ", testIS_LOOKUP"  + ", 1"  + ", 1"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_transformation_tablesConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_transformation_tables(null ,  1L ,  "testVERSION_NUMBER");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_transformation_tables constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_transformation_tablesConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_transformation_tables whereObject = new Meta_transformation_tables(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_transformation_tables(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  TRANSF_TABLE_ID.get(objUnderTest)  + ", " + TRANSF_TABLE_NAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + IS_LOOKUP.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest)  + ", " + KEY_COLUMN_ID.get(objUnderTest)  + ", " + VALUE_COLUMN_ID.get(objUnderTest) ;
    String expected =  "1"  + ", testTRANSF_TABLE_NAME"  + ", testDESCRIPTION"  + ", testVERSION_NUMBER"  + ", testIS_LOOKUP"  + ", 1"  + ", 1"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_transformation_tablesConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_transformation_tables whereObject = new Meta_transformation_tables(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_transformation_tables(null, whereObject);
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
    assertEquals("Meta_transformation_tables", objUnderTest.getTableName());
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
    Meta_transformation_tables whereObject = new Meta_transformation_tables(rockFactory);

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
    Meta_transformation_tables whereObject = new Meta_transformation_tables(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_transformation_tables");
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
    TRANSF_TABLE_ID.set(objUnderTest, 1L);
    HashSet testSet = new HashSet();
    testSet.add("TRANSF_TABLE_ID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_transformation_tables");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the TRANSF_TABLE_ID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT TRANSF_TABLE_ID FROM Meta_transformation_tables");
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
    
    String expected = "<Meta_transformation_tables TRANSF_TABLE_ID=\"1\" TRANSF_TABLE_NAME=\"'testTRANSF_TABLE_NAME'\" DESCRIPTION=\"'testDESCRIPTION'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" IS_LOOKUP=\"'testIS_LOOKUP'\" CONNECTION_ID=\"1\" TABLE_ID=\"1\" KEY_COLUMN_ID=\"1\" VALUE_COLUMN_ID=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_transformation_tables TRANSF_TABLE_ID=\"1\" TRANSF_TABLE_NAME=\"'testTRANSF_TABLE_NAME'\" DESCRIPTION=\"'testDESCRIPTION'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" IS_LOOKUP=\"'testIS_LOOKUP'\" CONNECTION_ID=\"1\" TABLE_ID=\"1\" KEY_COLUMN_ID=\"1\" VALUE_COLUMN_ID=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_transformation_tables>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_transformation_tables ( TRANSF_TABLE_ID, TRANSF_TABLE_NAME, DESCRIPTION, VERSION_NUMBER, IS_LOOKUP, CONNECTION_ID, TABLE_ID, KEY_COLUMN_ID, VALUE_COLUMN_ID ) values "
      + "( 1, 'testTRANSF_TABLE_NAME', 'testDESCRIPTION', 'testVERSION_NUMBER', 'testIS_LOOKUP', 1, 1, 1, 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTransf_table_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTransf_table_id(555L);
    assertEquals(555L, TRANSF_TABLE_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTransf_table_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTransf_table_name(Meta_transformation_tablesTest.testStringGenerator("anotherTRANSF_TABLE_NAME", 30));
    assertEquals(Meta_transformation_tablesTest.testStringGenerator("anotherTRANSF_TABLE_NAME", 30), TRANSF_TABLE_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(Meta_transformation_tablesTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(Meta_transformation_tablesTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_transformation_tablesTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_transformation_tablesTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIs_lookup() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIs_lookup(Meta_transformation_tablesTest.testStringGenerator("anotherIS_LOOKUP", 1));
    assertEquals(Meta_transformation_tablesTest.testStringGenerator("anotherIS_LOOKUP", 1), IS_LOOKUP.get(objUnderTest));
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
  public void testSetAndGetKey_column_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setKey_column_id(555L);
    assertEquals(555L, KEY_COLUMN_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetValue_column_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setValue_column_id(555L);
    assertEquals(555L, VALUE_COLUMN_ID.get(objUnderTest));
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
    String[] expectedKeys = { "TRANSF_TABLE_ID","VERSION_NUMBER"};

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
    objUnderTest = new Meta_transformation_tables(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  TRANSF_TABLE_ID.get(objUnderTest)  + ", " + TRANSF_TABLE_NAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + IS_LOOKUP.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest)  + ", " + KEY_COLUMN_ID.get(objUnderTest)  + ", " + VALUE_COLUMN_ID.get(objUnderTest) ;
    String expected =  "0"  + ", "  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0" ;
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
    Meta_transformation_tables compareObj = new Meta_transformation_tables(rockFactory ,  1L ,  "testVERSION_NUMBER");

    /* Testing first with null primary key value */
    TRANSF_TABLE_ID.set(objUnderTest, null);
  	VERSION_NUMBER.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    TRANSF_TABLE_ID.set(objUnderTest,  7L );
  	VERSION_NUMBER.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    TRANSF_TABLE_ID.set(objUnderTest,  1L );
  	VERSION_NUMBER.set(objUnderTest,  "testVERSION_NUMBER");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_transformation_tables with our current one. If the two
   * Meta_transformation_tabless are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_transformation_tables() throws Exception {

    /* Creating another Meta_transformation_tables which will be compared to the tested one */
    Meta_transformation_tables comparedObj = new Meta_transformation_tables(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_transformation_tables with our current one. If the two
   * Meta_transformation_tabless are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_transformation_tables() throws Exception {

    /* Creating another Meta_transformation_tables which will be compared to the tested one */
    Meta_transformation_tables comparedObj = new Meta_transformation_tables(rockFactory ,  1L ,  "testVERSION_NUMBER");
    comparedObj.setTransf_table_id( 7L );

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_transformation_tables with our current one. If the two
   * Meta_transformation_tabless are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_transformation_tables() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_transformation_tables comparedObj = new Meta_transformation_tables(rockFactory ,  1L ,  "testVERSION_NUMBER");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_transformation_tables with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_transformation_tables() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_transformation_tables comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_transformation_tables was null \n");
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
    assertEquals(Meta_transformation_tables.class, actualObject.getClass());
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
    Meta_transformation_tables testAgg = new Meta_transformation_tables(rockFactory ,  1L ,  "testVERSION_NUMBER");
    TRANSF_TABLE_ID.set(objUnderTest, 7L);

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Transf_table_id.
   */
  @Test
  public void testGetTransf_table_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getTransf_table_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Transf_table_id.
  */
  @Test
  public void testGetTransf_table_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTransf_table_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Transf_table_id.
  */
  @Test
  public void testGetTransf_table_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getTransf_table_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Transf_table_name.
   */
  @Test
  public void testGetTransf_table_nameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getTransf_table_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Transf_table_name.
  */
  @Test
  public void testGetTransf_table_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTransf_table_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Transf_table_name.
  */
  @Test
  public void testGetTransf_table_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTransf_table_nameSQLType());    
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
   * Testing columnsize retrieving for Is_lookup.
   */
  @Test
  public void testGetIs_lookupColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getIs_lookupColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Is_lookup.
  */
  @Test
  public void testGetIs_lookupDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIs_lookupDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Is_lookup.
  */
  @Test
  public void testGetIs_lookupSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getIs_lookupSQLType());    
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
   * Testing columnsize retrieving for Key_column_id.
   */
  @Test
  public void testGetKey_column_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getKey_column_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Key_column_id.
  */
  @Test
  public void testGetKey_column_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getKey_column_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Key_column_id.
  */
  @Test
  public void testGetKey_column_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getKey_column_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Value_column_id.
   */
  @Test
  public void testGetValue_column_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getValue_column_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Value_column_id.
  */
  @Test
  public void testGetValue_column_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getValue_column_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Value_column_id.
  */
  @Test
  public void testGetValue_column_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getValue_column_idSQLType());    
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
   * Testing original Meta_transformation_tables object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_transformation_tables(rockFactory, false);
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
    Meta_transformation_tables changedOriginal = new Meta_transformation_tables(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_transformation_tables(rockFactory, false);
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
    Meta_transformation_tables changedOriginal = new Meta_transformation_tables(rockFactory, false);
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