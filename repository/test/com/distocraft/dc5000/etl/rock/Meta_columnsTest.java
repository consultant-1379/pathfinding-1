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
 * Test class for Meta_columns. Changes to Meta_columns table are made via
 * this class.
 */
public class Meta_columnsTest {

  private static Meta_columns objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field COLUMN_ID;
  
  private static Field COLUMN_NAME;
  
  private static Field COLUMN_ALIAS_NAME;
  
  private static Field COLUMN_TYPE;
  
  private static Field COLUMN_LENGTH;
  
  private static Field IS_PK_COLUMN;
  
  private static Field VERSION_NUMBER;
  
  private static Field CONNECTION_ID;
  
  private static Field TABLE_ID;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_columns.class.getDeclaredField("newItem");
		COLUMN_ID = Meta_columns.class.getDeclaredField("COLUMN_ID");
		COLUMN_NAME = Meta_columns.class.getDeclaredField("COLUMN_NAME");
		COLUMN_ALIAS_NAME = Meta_columns.class.getDeclaredField("COLUMN_ALIAS_NAME");
		COLUMN_TYPE = Meta_columns.class.getDeclaredField("COLUMN_TYPE");
		COLUMN_LENGTH = Meta_columns.class.getDeclaredField("COLUMN_LENGTH");
		IS_PK_COLUMN = Meta_columns.class.getDeclaredField("IS_PK_COLUMN");
		VERSION_NUMBER = Meta_columns.class.getDeclaredField("VERSION_NUMBER");
		CONNECTION_ID = Meta_columns.class.getDeclaredField("CONNECTION_ID");
		TABLE_ID = Meta_columns.class.getDeclaredField("TABLE_ID");
		newItem.setAccessible(true);
		COLUMN_ID.setAccessible(true);
		COLUMN_NAME.setAccessible(true);
		COLUMN_ALIAS_NAME.setAccessible(true);
		COLUMN_TYPE.setAccessible(true);
		COLUMN_LENGTH.setAccessible(true);
		IS_PK_COLUMN.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		CONNECTION_ID.setAccessible(true);
		TABLE_ID.setAccessible(true);
	  timeStampName = Meta_columns.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_columns ( COLUMN_ID BIGINT  ,COLUMN_NAME VARCHAR(31) ,COLUMN_ALIAS_NAME VARCHAR(31) ,COLUMN_TYPE VARCHAR(31) ,COLUMN_LENGTH BIGINT  ,IS_PK_COLUMN VARCHAR(31) ,VERSION_NUMBER VARCHAR(31) ,CONNECTION_ID BIGINT  ,TABLE_ID BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_columns");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'testCOLUMN_NAME'  ,'testCOLUMN_ALIAS_NAME'  ,'testCOLUMN_TYPE'  ,1  ,'testIS_PK_COLUMN'  ,'testVERSION_NUMBER'  ,1  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_columns(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_columns");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_columns constructor variable initialization with null values.
   */
  @Test
  public void testMeta_columnsConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_columns(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  COLUMN_ID.get(objUnderTest)  + ", " + COLUMN_NAME.get(objUnderTest)  + ", " + COLUMN_ALIAS_NAME.get(objUnderTest)  + ", " + COLUMN_TYPE.get(objUnderTest)  + ", " + COLUMN_LENGTH.get(objUnderTest)  + ", " + IS_PK_COLUMN.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_columns constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_columnsConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_columns(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );

    /* Asserting that variables are initialized */
    String actual =  COLUMN_ID.get(objUnderTest)  + ", " + COLUMN_NAME.get(objUnderTest)  + ", " + COLUMN_ALIAS_NAME.get(objUnderTest)  + ", " + COLUMN_TYPE.get(objUnderTest)  + ", " + COLUMN_LENGTH.get(objUnderTest)  + ", " + IS_PK_COLUMN.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest) ;
    String expected =  "1"  + ", testCOLUMN_NAME"  + ", testCOLUMN_ALIAS_NAME"  + ", testCOLUMN_TYPE"  + ", 1"  + ", testIS_PK_COLUMN"  + ", testVERSION_NUMBER"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_columnsConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_columns(null ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_columns constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_columnsConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_columns whereObject = new Meta_columns(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_columns(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  COLUMN_ID.get(objUnderTest)  + ", " + COLUMN_NAME.get(objUnderTest)  + ", " + COLUMN_ALIAS_NAME.get(objUnderTest)  + ", " + COLUMN_TYPE.get(objUnderTest)  + ", " + COLUMN_LENGTH.get(objUnderTest)  + ", " + IS_PK_COLUMN.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest) ;
    String expected =  "1"  + ", testCOLUMN_NAME"  + ", testCOLUMN_ALIAS_NAME"  + ", testCOLUMN_TYPE"  + ", 1"  + ", testIS_PK_COLUMN"  + ", testVERSION_NUMBER"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_columnsConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_columns whereObject = new Meta_columns(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_columns(null, whereObject);
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
    assertEquals("Meta_columns", objUnderTest.getTableName());
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
    Meta_columns whereObject = new Meta_columns(rockFactory);

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
    Meta_columns whereObject = new Meta_columns(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_columns");
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
    COLUMN_ID.set(objUnderTest, 1L);
    HashSet testSet = new HashSet();
    testSet.add("COLUMN_ID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_columns");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the COLUMN_ID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT COLUMN_ID FROM Meta_columns");
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
    
    String expected = "<Meta_columns COLUMN_ID=\"1\" COLUMN_NAME=\"'testCOLUMN_NAME'\" COLUMN_ALIAS_NAME=\"'testCOLUMN_ALIAS_NAME'\" COLUMN_TYPE=\"'testCOLUMN_TYPE'\" COLUMN_LENGTH=\"1\" IS_PK_COLUMN=\"'testIS_PK_COLUMN'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" CONNECTION_ID=\"1\" TABLE_ID=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_columns COLUMN_ID=\"1\" COLUMN_NAME=\"'testCOLUMN_NAME'\" COLUMN_ALIAS_NAME=\"'testCOLUMN_ALIAS_NAME'\" COLUMN_TYPE=\"'testCOLUMN_TYPE'\" COLUMN_LENGTH=\"1\" IS_PK_COLUMN=\"'testIS_PK_COLUMN'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" CONNECTION_ID=\"1\" TABLE_ID=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_columns>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_columns ( COLUMN_ID, COLUMN_NAME, COLUMN_ALIAS_NAME, COLUMN_TYPE, COLUMN_LENGTH, IS_PK_COLUMN, VERSION_NUMBER, CONNECTION_ID, TABLE_ID ) values "
      + "( 1, 'testCOLUMN_NAME', 'testCOLUMN_ALIAS_NAME', 'testCOLUMN_TYPE', 1, 'testIS_PK_COLUMN', 'testVERSION_NUMBER', 1, 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColumn_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColumn_id(555L);
    assertEquals(555L, COLUMN_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColumn_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColumn_name(Meta_columnsTest.testStringGenerator("anotherCOLUMN_NAME", 30));
    assertEquals(Meta_columnsTest.testStringGenerator("anotherCOLUMN_NAME", 30), COLUMN_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColumn_alias_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColumn_alias_name(Meta_columnsTest.testStringGenerator("anotherCOLUMN_ALIAS_NAME", 60));
    assertEquals(Meta_columnsTest.testStringGenerator("anotherCOLUMN_ALIAS_NAME", 60), COLUMN_ALIAS_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColumn_type() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColumn_type(Meta_columnsTest.testStringGenerator("anotherCOLUMN_TYPE", 30));
    assertEquals(Meta_columnsTest.testStringGenerator("anotherCOLUMN_TYPE", 30), COLUMN_TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColumn_length() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColumn_length(555L);
    assertEquals(555L, COLUMN_LENGTH.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIs_pk_column() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIs_pk_column(Meta_columnsTest.testStringGenerator("anotherIS_PK_COLUMN", 1));
    assertEquals(Meta_columnsTest.testStringGenerator("anotherIS_PK_COLUMN", 1), IS_PK_COLUMN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_columnsTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_columnsTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
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
    String[] expectedKeys = { "COLUMN_ID","VERSION_NUMBER","CONNECTION_ID","TABLE_ID"};

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
    objUnderTest = new Meta_columns(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  COLUMN_ID.get(objUnderTest)  + ", " + COLUMN_NAME.get(objUnderTest)  + ", " + COLUMN_ALIAS_NAME.get(objUnderTest)  + ", " + COLUMN_TYPE.get(objUnderTest)  + ", " + COLUMN_LENGTH.get(objUnderTest)  + ", " + IS_PK_COLUMN.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest) ;
    String expected =  "0"  + ", "  + ", "  + ", "  + ", 0"  + ", "  + ", "  + ", 0"  + ", 0" ;
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
    Meta_columns compareObj = new Meta_columns(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );

    /* Testing first with null primary key value */
    COLUMN_ID.set(objUnderTest, null);
  	VERSION_NUMBER.set(objUnderTest, null);
  	CONNECTION_ID.set(objUnderTest, null);
  	TABLE_ID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    COLUMN_ID.set(objUnderTest,  7L );
  	VERSION_NUMBER.set(objUnderTest,  "different");
  	CONNECTION_ID.set(objUnderTest,  7L );
  	TABLE_ID.set(objUnderTest,  7L );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    COLUMN_ID.set(objUnderTest,  1L );
  	VERSION_NUMBER.set(objUnderTest,  "testVERSION_NUMBER");
  	CONNECTION_ID.set(objUnderTest,  1L );
  	TABLE_ID.set(objUnderTest,  1L );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_columns with our current one. If the two
   * Meta_columnss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_columns() throws Exception {

    /* Creating another Meta_columns which will be compared to the tested one */
    Meta_columns comparedObj = new Meta_columns(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_columns with our current one. If the two
   * Meta_columnss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_columns() throws Exception {

    /* Creating another Meta_columns which will be compared to the tested one */
    Meta_columns comparedObj = new Meta_columns(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );
    comparedObj.setColumn_id( 7L );

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_columns with our current one. If the two
   * Meta_columnss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_columns() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_columns comparedObj = new Meta_columns(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_columns with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_columns() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_columns comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_columns was null \n");
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
    assertEquals(Meta_columns.class, actualObject.getClass());
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
    Meta_columns testAgg = new Meta_columns(rockFactory ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );
    COLUMN_ID.set(objUnderTest, 7L);

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Column_id.
   */
  @Test
  public void testGetColumn_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getColumn_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Column_id.
  */
  @Test
  public void testGetColumn_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColumn_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Column_id.
  */
  @Test
  public void testGetColumn_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getColumn_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Column_name.
   */
  @Test
  public void testGetColumn_nameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getColumn_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Column_name.
  */
  @Test
  public void testGetColumn_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColumn_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Column_name.
  */
  @Test
  public void testGetColumn_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getColumn_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Column_alias_name.
   */
  @Test
  public void testGetColumn_alias_nameColumnSize() throws Exception {
    
     assertEquals(60, objUnderTest.getColumn_alias_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Column_alias_name.
  */
  @Test
  public void testGetColumn_alias_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColumn_alias_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Column_alias_name.
  */
  @Test
  public void testGetColumn_alias_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getColumn_alias_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Column_type.
   */
  @Test
  public void testGetColumn_typeColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getColumn_typeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Column_type.
  */
  @Test
  public void testGetColumn_typeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColumn_typeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Column_type.
  */
  @Test
  public void testGetColumn_typeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getColumn_typeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Column_length.
   */
  @Test
  public void testGetColumn_lengthColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getColumn_lengthColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Column_length.
  */
  @Test
  public void testGetColumn_lengthDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColumn_lengthDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Column_length.
  */
  @Test
  public void testGetColumn_lengthSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getColumn_lengthSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Is_pk_column.
   */
  @Test
  public void testGetIs_pk_columnColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getIs_pk_columnColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Is_pk_column.
  */
  @Test
  public void testGetIs_pk_columnDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIs_pk_columnDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Is_pk_column.
  */
  @Test
  public void testGetIs_pk_columnSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getIs_pk_columnSQLType());    
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
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Meta_columns object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_columns(rockFactory, false);
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
    Meta_columns changedOriginal = new Meta_columns(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_columns(rockFactory, false);
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
    Meta_columns changedOriginal = new Meta_columns(rockFactory, false);
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