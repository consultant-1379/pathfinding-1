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
 * Test class for Meta_sql_loads. Changes to Meta_sql_loads table are made via
 * this class.
 */
public class Meta_sql_loadsTest {

  private static Meta_sql_loads objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field INPUT_FILE;
  
  private static Field CTL_FILE;
  
  private static Field COLLECTION_SET_ID;
  
  private static Field COLLECTION_ID;
  
  private static Field CONNECTION_ID;
  
  private static Field DIS_FILE;
  
  private static Field BAD_FILE;
  
  private static Field LOAD_TYPE;
  
  private static Field TEXT;
  
  private static Field DELIM;
  
  private static Field SQLLDR_CMD;
  
  private static Field LOAD_OPTION;
  
  private static Field VERSION_NUMBER;
  
  private static Field TRANSFER_ACTION_ID;
  
  private static Field TABLE_ID;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_sql_loads.class.getDeclaredField("newItem");
		INPUT_FILE = Meta_sql_loads.class.getDeclaredField("INPUT_FILE");
		CTL_FILE = Meta_sql_loads.class.getDeclaredField("CTL_FILE");
		COLLECTION_SET_ID = Meta_sql_loads.class.getDeclaredField("COLLECTION_SET_ID");
		COLLECTION_ID = Meta_sql_loads.class.getDeclaredField("COLLECTION_ID");
		CONNECTION_ID = Meta_sql_loads.class.getDeclaredField("CONNECTION_ID");
		DIS_FILE = Meta_sql_loads.class.getDeclaredField("DIS_FILE");
		BAD_FILE = Meta_sql_loads.class.getDeclaredField("BAD_FILE");
		LOAD_TYPE = Meta_sql_loads.class.getDeclaredField("LOAD_TYPE");
		TEXT = Meta_sql_loads.class.getDeclaredField("TEXT");
		DELIM = Meta_sql_loads.class.getDeclaredField("DELIM");
		SQLLDR_CMD = Meta_sql_loads.class.getDeclaredField("SQLLDR_CMD");
		LOAD_OPTION = Meta_sql_loads.class.getDeclaredField("LOAD_OPTION");
		VERSION_NUMBER = Meta_sql_loads.class.getDeclaredField("VERSION_NUMBER");
		TRANSFER_ACTION_ID = Meta_sql_loads.class.getDeclaredField("TRANSFER_ACTION_ID");
		TABLE_ID = Meta_sql_loads.class.getDeclaredField("TABLE_ID");
		newItem.setAccessible(true);
		INPUT_FILE.setAccessible(true);
		CTL_FILE.setAccessible(true);
		COLLECTION_SET_ID.setAccessible(true);
		COLLECTION_ID.setAccessible(true);
		CONNECTION_ID.setAccessible(true);
		DIS_FILE.setAccessible(true);
		BAD_FILE.setAccessible(true);
		LOAD_TYPE.setAccessible(true);
		TEXT.setAccessible(true);
		DELIM.setAccessible(true);
		SQLLDR_CMD.setAccessible(true);
		LOAD_OPTION.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		TRANSFER_ACTION_ID.setAccessible(true);
		TABLE_ID.setAccessible(true);
	  timeStampName = Meta_sql_loads.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_sql_loads ( INPUT_FILE VARCHAR(31)  ,CTL_FILE VARCHAR(31) ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,CONNECTION_ID BIGINT  ,DIS_FILE VARCHAR(31) ,BAD_FILE VARCHAR(31) ,LOAD_TYPE VARCHAR(31) ,TEXT VARCHAR(31) ,DELIM VARCHAR(31) ,SQLLDR_CMD VARCHAR(31) ,LOAD_OPTION VARCHAR(31) ,VERSION_NUMBER VARCHAR(31) ,TRANSFER_ACTION_ID BIGINT  ,TABLE_ID BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_sql_loads");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_sql_loads VALUES( 'testINPUT_FILE'  ,'testCTL_FILE'  ,1  ,1  ,1  ,'testDIS_FILE'  ,'testBAD_FILE'  ,'testLOAD_TYPE'  ,'testTEXT'  ,'testDELIM'  ,'testSQLLDR_CMD'  ,'testLOAD_OPTION'  ,'testVERSION_NUMBER'  ,1  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_sql_loads(rockFactory ,  1L ,  1L ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_sql_loads");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_sql_loads constructor variable initialization with null values.
   */
  @Test
  public void testMeta_sql_loadsConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_sql_loads(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  INPUT_FILE.get(objUnderTest)  + ", " + CTL_FILE.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + DIS_FILE.get(objUnderTest)  + ", " + BAD_FILE.get(objUnderTest)  + ", " + LOAD_TYPE.get(objUnderTest)  + ", " + TEXT.get(objUnderTest)  + ", " + DELIM.get(objUnderTest)  + ", " + SQLLDR_CMD.get(objUnderTest)  + ", " + LOAD_OPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_sql_loads constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_sql_loadsConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_sql_loads(rockFactory ,  1L ,  1L ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );

    /* Asserting that variables are initialized */
    String actual =  INPUT_FILE.get(objUnderTest)  + ", " + CTL_FILE.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + DIS_FILE.get(objUnderTest)  + ", " + BAD_FILE.get(objUnderTest)  + ", " + LOAD_TYPE.get(objUnderTest)  + ", " + TEXT.get(objUnderTest)  + ", " + DELIM.get(objUnderTest)  + ", " + SQLLDR_CMD.get(objUnderTest)  + ", " + LOAD_OPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest) ;
    String expected =  "testINPUT_FILE"  + ", testCTL_FILE"  + ", 1"  + ", 1"  + ", 1"  + ", testDIS_FILE"  + ", testBAD_FILE"  + ", testLOAD_TYPE"  + ", testTEXT"  + ", testDELIM"  + ", testSQLLDR_CMD"  + ", testLOAD_OPTION"  + ", testVERSION_NUMBER"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_sql_loadsConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_sql_loads(null ,  1L ,  1L ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_sql_loads constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_sql_loadsConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_sql_loads whereObject = new Meta_sql_loads(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_sql_loads(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  INPUT_FILE.get(objUnderTest)  + ", " + CTL_FILE.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + DIS_FILE.get(objUnderTest)  + ", " + BAD_FILE.get(objUnderTest)  + ", " + LOAD_TYPE.get(objUnderTest)  + ", " + TEXT.get(objUnderTest)  + ", " + DELIM.get(objUnderTest)  + ", " + SQLLDR_CMD.get(objUnderTest)  + ", " + LOAD_OPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest) ;
    String expected =  "testINPUT_FILE"  + ", testCTL_FILE"  + ", 1"  + ", 1"  + ", 1"  + ", testDIS_FILE"  + ", testBAD_FILE"  + ", testLOAD_TYPE"  + ", testTEXT"  + ", testDELIM"  + ", testSQLLDR_CMD"  + ", testLOAD_OPTION"  + ", testVERSION_NUMBER"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_sql_loadsConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_sql_loads whereObject = new Meta_sql_loads(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_sql_loads(null, whereObject);
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
    assertEquals("Meta_sql_loads", objUnderTest.getTableName());
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
    Meta_sql_loads whereObject = new Meta_sql_loads(rockFactory);

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
    Meta_sql_loads whereObject = new Meta_sql_loads(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_sql_loads");
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
    INPUT_FILE.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("INPUT_FILE");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_sql_loads");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the INPUT_FILE column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT INPUT_FILE FROM Meta_sql_loads");
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
    
    String expected = "<Meta_sql_loads INPUT_FILE=\"'testINPUT_FILE'\" CTL_FILE=\"'testCTL_FILE'\" COLLECTION_SET_ID=\"1\" COLLECTION_ID=\"1\" CONNECTION_ID=\"1\" DIS_FILE=\"'testDIS_FILE'\" BAD_FILE=\"'testBAD_FILE'\" LOAD_TYPE=\"'testLOAD_TYPE'\" TEXT=\"'testTEXT'\" DELIM=\"'testDELIM'\" SQLLDR_CMD=\"'testSQLLDR_CMD'\" LOAD_OPTION=\"'testLOAD_OPTION'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" TRANSFER_ACTION_ID=\"1\" TABLE_ID=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_sql_loads INPUT_FILE=\"'testINPUT_FILE'\" CTL_FILE=\"'testCTL_FILE'\" COLLECTION_SET_ID=\"1\" COLLECTION_ID=\"1\" CONNECTION_ID=\"1\" DIS_FILE=\"'testDIS_FILE'\" BAD_FILE=\"'testBAD_FILE'\" LOAD_TYPE=\"'testLOAD_TYPE'\" TEXT=\"'testTEXT'\" DELIM=\"'testDELIM'\" SQLLDR_CMD=\"'testSQLLDR_CMD'\" LOAD_OPTION=\"'testLOAD_OPTION'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" TRANSFER_ACTION_ID=\"1\" TABLE_ID=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_sql_loads>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_sql_loads ( INPUT_FILE, CTL_FILE, COLLECTION_SET_ID, COLLECTION_ID, CONNECTION_ID, DIS_FILE, BAD_FILE, LOAD_TYPE, TEXT, DELIM, SQLLDR_CMD, LOAD_OPTION, VERSION_NUMBER, TRANSFER_ACTION_ID, TABLE_ID ) values "
      + "( 'testINPUT_FILE', 'testCTL_FILE', 1, 1, 1, 'testDIS_FILE', 'testBAD_FILE', 'testLOAD_TYPE', 'testTEXT', 'testDELIM', 'testSQLLDR_CMD', 'testLOAD_OPTION', 'testVERSION_NUMBER', 1, 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetInput_file() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInput_file(Meta_sql_loadsTest.testStringGenerator("anotherINPUT_FILE", 200));
    assertEquals(Meta_sql_loadsTest.testStringGenerator("anotherINPUT_FILE", 200), INPUT_FILE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCtl_file() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCtl_file(Meta_sql_loadsTest.testStringGenerator("anotherCTL_FILE", 200));
    assertEquals(Meta_sql_loadsTest.testStringGenerator("anotherCTL_FILE", 200), CTL_FILE.get(objUnderTest));
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
  public void testSetAndGetDis_file() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDis_file(Meta_sql_loadsTest.testStringGenerator("anotherDIS_FILE", 200));
    assertEquals(Meta_sql_loadsTest.testStringGenerator("anotherDIS_FILE", 200), DIS_FILE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBad_file() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBad_file(Meta_sql_loadsTest.testStringGenerator("anotherBAD_FILE", 200));
    assertEquals(Meta_sql_loadsTest.testStringGenerator("anotherBAD_FILE", 200), BAD_FILE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLoad_type() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLoad_type(Meta_sql_loadsTest.testStringGenerator("anotherLOAD_TYPE", 30));
    assertEquals(Meta_sql_loadsTest.testStringGenerator("anotherLOAD_TYPE", 30), LOAD_TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetText() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setText(Meta_sql_loadsTest.testStringGenerator("anotherTEXT", 2000));
    assertEquals(Meta_sql_loadsTest.testStringGenerator("anotherTEXT", 2000), TEXT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDelim() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDelim(Meta_sql_loadsTest.testStringGenerator("anotherDELIM", 1));
    assertEquals(Meta_sql_loadsTest.testStringGenerator("anotherDELIM", 1), DELIM.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSqlldr_cmd() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSqlldr_cmd(Meta_sql_loadsTest.testStringGenerator("anotherSQLLDR_CMD", 200));
    assertEquals(Meta_sql_loadsTest.testStringGenerator("anotherSQLLDR_CMD", 200), SQLLDR_CMD.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLoad_option() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLoad_option(Meta_sql_loadsTest.testStringGenerator("anotherLOAD_OPTION", 30));
    assertEquals(Meta_sql_loadsTest.testStringGenerator("anotherLOAD_OPTION", 30), LOAD_OPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_sql_loadsTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_sql_loadsTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
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
    String[] expectedKeys = { "COLLECTION_SET_ID","COLLECTION_ID","CONNECTION_ID","VERSION_NUMBER","TRANSFER_ACTION_ID","TABLE_ID"};

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
    objUnderTest = new Meta_sql_loads(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  INPUT_FILE.get(objUnderTest)  + ", " + CTL_FILE.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + CONNECTION_ID.get(objUnderTest)  + ", " + DIS_FILE.get(objUnderTest)  + ", " + BAD_FILE.get(objUnderTest)  + ", " + LOAD_TYPE.get(objUnderTest)  + ", " + TEXT.get(objUnderTest)  + ", " + DELIM.get(objUnderTest)  + ", " + SQLLDR_CMD.get(objUnderTest)  + ", " + LOAD_OPTION.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest)  + ", " + TABLE_ID.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0"  + ", 0" ;
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
    Meta_sql_loads compareObj = new Meta_sql_loads(rockFactory ,  1L ,  1L ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );

    /* Testing first with null primary key value */
    COLLECTION_SET_ID.set(objUnderTest, null);
  	COLLECTION_ID.set(objUnderTest, null);
  	CONNECTION_ID.set(objUnderTest, null);
  	VERSION_NUMBER.set(objUnderTest, null);
  	TRANSFER_ACTION_ID.set(objUnderTest, null);
  	TABLE_ID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    COLLECTION_SET_ID.set(objUnderTest,  7L );
  	COLLECTION_ID.set(objUnderTest,  7L );
  	CONNECTION_ID.set(objUnderTest,  7L );
  	VERSION_NUMBER.set(objUnderTest,  "different");
  	TRANSFER_ACTION_ID.set(objUnderTest,  7L );
  	TABLE_ID.set(objUnderTest,  7L );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    COLLECTION_SET_ID.set(objUnderTest,  1L );
  	COLLECTION_ID.set(objUnderTest,  1L );
  	CONNECTION_ID.set(objUnderTest,  1L );
  	VERSION_NUMBER.set(objUnderTest,  "testVERSION_NUMBER");
  	TRANSFER_ACTION_ID.set(objUnderTest,  1L );
  	TABLE_ID.set(objUnderTest,  1L );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_sql_loads with our current one. If the two
   * Meta_sql_loadss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_sql_loads() throws Exception {

    /* Creating another Meta_sql_loads which will be compared to the tested one */
    Meta_sql_loads comparedObj = new Meta_sql_loads(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_sql_loads with our current one. If the two
   * Meta_sql_loadss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_sql_loads() throws Exception {

    /* Creating another Meta_sql_loads which will be compared to the tested one */
    Meta_sql_loads comparedObj = new Meta_sql_loads(rockFactory ,  1L ,  1L ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );
    comparedObj.setInput_file( "DifferentINPUT_FILE");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_sql_loads with our current one. If the two
   * Meta_sql_loadss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_sql_loads() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_sql_loads comparedObj = new Meta_sql_loads(rockFactory ,  1L ,  1L ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_sql_loads with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_sql_loads() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_sql_loads comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_sql_loads was null \n");
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
    assertEquals(Meta_sql_loads.class, actualObject.getClass());
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
    Meta_sql_loads testAgg = new Meta_sql_loads(rockFactory ,  1L ,  1L ,  1L ,  "testVERSION_NUMBER",  1L ,  1L );
    INPUT_FILE.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Input_file.
   */
  @Test
  public void testGetInput_fileColumnSize() throws Exception {
    
     assertEquals(200, objUnderTest.getInput_fileColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Input_file.
  */
  @Test
  public void testGetInput_fileDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getInput_fileDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Input_file.
  */
  @Test
  public void testGetInput_fileSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getInput_fileSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Ctl_file.
   */
  @Test
  public void testGetCtl_fileColumnSize() throws Exception {
    
     assertEquals(200, objUnderTest.getCtl_fileColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Ctl_file.
  */
  @Test
  public void testGetCtl_fileDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCtl_fileDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Ctl_file.
  */
  @Test
  public void testGetCtl_fileSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getCtl_fileSQLType());    
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
   * Testing columnsize retrieving for Dis_file.
   */
  @Test
  public void testGetDis_fileColumnSize() throws Exception {
    
     assertEquals(200, objUnderTest.getDis_fileColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dis_file.
  */
  @Test
  public void testGetDis_fileDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDis_fileDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dis_file.
  */
  @Test
  public void testGetDis_fileSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDis_fileSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Bad_file.
   */
  @Test
  public void testGetBad_fileColumnSize() throws Exception {
    
     assertEquals(200, objUnderTest.getBad_fileColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Bad_file.
  */
  @Test
  public void testGetBad_fileDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBad_fileDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Bad_file.
  */
  @Test
  public void testGetBad_fileSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBad_fileSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Load_type.
   */
  @Test
  public void testGetLoad_typeColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getLoad_typeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Load_type.
  */
  @Test
  public void testGetLoad_typeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLoad_typeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Load_type.
  */
  @Test
  public void testGetLoad_typeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getLoad_typeSQLType());    
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
   * Testing columnsize retrieving for Delim.
   */
  @Test
  public void testGetDelimColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getDelimColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Delim.
  */
  @Test
  public void testGetDelimDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDelimDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Delim.
  */
  @Test
  public void testGetDelimSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDelimSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Sqlldr_cmd.
   */
  @Test
  public void testGetSqlldr_cmdColumnSize() throws Exception {
    
     assertEquals(200, objUnderTest.getSqlldr_cmdColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Sqlldr_cmd.
  */
  @Test
  public void testGetSqlldr_cmdDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSqlldr_cmdDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Sqlldr_cmd.
  */
  @Test
  public void testGetSqlldr_cmdSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSqlldr_cmdSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Load_option.
   */
  @Test
  public void testGetLoad_optionColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getLoad_optionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Load_option.
  */
  @Test
  public void testGetLoad_optionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLoad_optionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Load_option.
  */
  @Test
  public void testGetLoad_optionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getLoad_optionSQLType());    
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
   * Testing original Meta_sql_loads object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_sql_loads(rockFactory, false);
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
    Meta_sql_loads changedOriginal = new Meta_sql_loads(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_sql_loads(rockFactory, false);
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
    Meta_sql_loads changedOriginal = new Meta_sql_loads(rockFactory, false);
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