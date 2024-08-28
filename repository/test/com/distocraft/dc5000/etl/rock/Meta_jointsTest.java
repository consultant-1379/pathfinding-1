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
 * Test class for Meta_joints. Changes to Meta_joints table are made via
 * this class.
 */
public class Meta_jointsTest {

  private static Meta_joints objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field ID;
  
  private static Field IS_PK_COLUMN;
  
  private static Field IS_SUM_COLUMN;
  
  private static Field IS_GROUP_BY_COLUMN;
  
  private static Field COLUMN_SPACE_AT_FILE;
  
  private static Field FILE_ORDER_BY;
  
  private static Field PLUGIN_METHOD_NAME;
  
  private static Field VERSION_NUMBER;
  
  private static Field COLLECTION_SET_ID;
  
  private static Field COLLECTION_ID;
  
  private static Field TRANSFER_ACTION_ID;
  
  private static Field TARGET_CONNECTION_ID;
  
  private static Field TARGET_TABLE_ID;
  
  private static Field COLUMN_ID_TARGET_COLUMN;
  
  private static Field SOURCE_CONNECTION_ID;
  
  private static Field SOURCE_TABLE_ID;
  
  private static Field COLUMN_ID_SOURCE_COLUMN;
  
  private static Field TRANSFORMATION_ID;
  
  private static Field TRANSF_TABLE_ID;
  
  private static Field PAR_NAME;
  
  private static Field FILE_ID;
  
  private static Field PLUGIN_ID;
  
  private static Field FREE_FORMAT_TRANSFORMAT;
  
  private static Field METHOD_PARAMETER;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_joints.class.getDeclaredField("newItem");
		ID = Meta_joints.class.getDeclaredField("ID");
		IS_PK_COLUMN = Meta_joints.class.getDeclaredField("IS_PK_COLUMN");
		IS_SUM_COLUMN = Meta_joints.class.getDeclaredField("IS_SUM_COLUMN");
		IS_GROUP_BY_COLUMN = Meta_joints.class.getDeclaredField("IS_GROUP_BY_COLUMN");
		COLUMN_SPACE_AT_FILE = Meta_joints.class.getDeclaredField("COLUMN_SPACE_AT_FILE");
		FILE_ORDER_BY = Meta_joints.class.getDeclaredField("FILE_ORDER_BY");
		PLUGIN_METHOD_NAME = Meta_joints.class.getDeclaredField("PLUGIN_METHOD_NAME");
		VERSION_NUMBER = Meta_joints.class.getDeclaredField("VERSION_NUMBER");
		COLLECTION_SET_ID = Meta_joints.class.getDeclaredField("COLLECTION_SET_ID");
		COLLECTION_ID = Meta_joints.class.getDeclaredField("COLLECTION_ID");
		TRANSFER_ACTION_ID = Meta_joints.class.getDeclaredField("TRANSFER_ACTION_ID");
		TARGET_CONNECTION_ID = Meta_joints.class.getDeclaredField("TARGET_CONNECTION_ID");
		TARGET_TABLE_ID = Meta_joints.class.getDeclaredField("TARGET_TABLE_ID");
		COLUMN_ID_TARGET_COLUMN = Meta_joints.class.getDeclaredField("COLUMN_ID_TARGET_COLUMN");
		SOURCE_CONNECTION_ID = Meta_joints.class.getDeclaredField("SOURCE_CONNECTION_ID");
		SOURCE_TABLE_ID = Meta_joints.class.getDeclaredField("SOURCE_TABLE_ID");
		COLUMN_ID_SOURCE_COLUMN = Meta_joints.class.getDeclaredField("COLUMN_ID_SOURCE_COLUMN");
		TRANSFORMATION_ID = Meta_joints.class.getDeclaredField("TRANSFORMATION_ID");
		TRANSF_TABLE_ID = Meta_joints.class.getDeclaredField("TRANSF_TABLE_ID");
		PAR_NAME = Meta_joints.class.getDeclaredField("PAR_NAME");
		FILE_ID = Meta_joints.class.getDeclaredField("FILE_ID");
		PLUGIN_ID = Meta_joints.class.getDeclaredField("PLUGIN_ID");
		FREE_FORMAT_TRANSFORMAT = Meta_joints.class.getDeclaredField("FREE_FORMAT_TRANSFORMAT");
		METHOD_PARAMETER = Meta_joints.class.getDeclaredField("METHOD_PARAMETER");
		newItem.setAccessible(true);
		ID.setAccessible(true);
		IS_PK_COLUMN.setAccessible(true);
		IS_SUM_COLUMN.setAccessible(true);
		IS_GROUP_BY_COLUMN.setAccessible(true);
		COLUMN_SPACE_AT_FILE.setAccessible(true);
		FILE_ORDER_BY.setAccessible(true);
		PLUGIN_METHOD_NAME.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		COLLECTION_SET_ID.setAccessible(true);
		COLLECTION_ID.setAccessible(true);
		TRANSFER_ACTION_ID.setAccessible(true);
		TARGET_CONNECTION_ID.setAccessible(true);
		TARGET_TABLE_ID.setAccessible(true);
		COLUMN_ID_TARGET_COLUMN.setAccessible(true);
		SOURCE_CONNECTION_ID.setAccessible(true);
		SOURCE_TABLE_ID.setAccessible(true);
		COLUMN_ID_SOURCE_COLUMN.setAccessible(true);
		TRANSFORMATION_ID.setAccessible(true);
		TRANSF_TABLE_ID.setAccessible(true);
		PAR_NAME.setAccessible(true);
		FILE_ID.setAccessible(true);
		PLUGIN_ID.setAccessible(true);
		FREE_FORMAT_TRANSFORMAT.setAccessible(true);
		METHOD_PARAMETER.setAccessible(true);
	  timeStampName = Meta_joints.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_joints ( ID BIGINT  ,IS_PK_COLUMN VARCHAR(31) ,IS_SUM_COLUMN VARCHAR(31) ,IS_GROUP_BY_COLUMN VARCHAR(31) ,COLUMN_SPACE_AT_FILE BIGINT  ,FILE_ORDER_BY BIGINT  ,PLUGIN_METHOD_NAME VARCHAR(31) ,VERSION_NUMBER VARCHAR(31) ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,TRANSFER_ACTION_ID BIGINT  ,TARGET_CONNECTION_ID BIGINT  ,TARGET_TABLE_ID BIGINT  ,COLUMN_ID_TARGET_COLUMN BIGINT  ,SOURCE_CONNECTION_ID BIGINT  ,SOURCE_TABLE_ID BIGINT  ,COLUMN_ID_SOURCE_COLUMN BIGINT  ,TRANSFORMATION_ID BIGINT  ,TRANSF_TABLE_ID BIGINT  ,PAR_NAME VARCHAR(31) ,FILE_ID BIGINT  ,PLUGIN_ID BIGINT  ,FREE_FORMAT_TRANSFORMAT VARCHAR(31) ,METHOD_PARAMETER VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_joints");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_joints VALUES( 1  ,'testIS_PK_COLUMN'  ,'testIS_SUM_COLUMN'  ,'testIS_GROUP_BY_COLUMN'  ,1  ,1  ,'testPLUGIN_METHOD_NAME'  ,'testVERSION_NUMBER'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,'testPAR_NAME'  ,1  ,1  ,'testFREE_FORMAT_TRANSFORMAT'  ,'testMETHOD_PARAMETER' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_joints(rockFactory ,  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_joints");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_joints constructor variable initialization with null values.
   */
  @Test
  public void testMeta_jointsConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_joints(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  ID.get(objUnderTest)  + ", " + IS_PK_COLUMN.get(objUnderTest)  + ", " + IS_SUM_COLUMN.get(objUnderTest)  + ", " + IS_GROUP_BY_COLUMN.get(objUnderTest)  + ", " + COLUMN_SPACE_AT_FILE.get(objUnderTest)  + ", " + FILE_ORDER_BY.get(objUnderTest)  + ", " + PLUGIN_METHOD_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest)  + ", " + TARGET_CONNECTION_ID.get(objUnderTest)  + ", " + TARGET_TABLE_ID.get(objUnderTest)  + ", " + COLUMN_ID_TARGET_COLUMN.get(objUnderTest)  + ", " + SOURCE_CONNECTION_ID.get(objUnderTest)  + ", " + SOURCE_TABLE_ID.get(objUnderTest)  + ", " + COLUMN_ID_SOURCE_COLUMN.get(objUnderTest)  + ", " + TRANSFORMATION_ID.get(objUnderTest)  + ", " + TRANSF_TABLE_ID.get(objUnderTest)  + ", " + PAR_NAME.get(objUnderTest)  + ", " + FILE_ID.get(objUnderTest)  + ", " + PLUGIN_ID.get(objUnderTest)  + ", " + FREE_FORMAT_TRANSFORMAT.get(objUnderTest)  + ", " + METHOD_PARAMETER.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_joints constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_jointsConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_joints(rockFactory ,  1L );

    /* Asserting that variables are initialized */
    String actual =  ID.get(objUnderTest)  + ", " + IS_PK_COLUMN.get(objUnderTest)  + ", " + IS_SUM_COLUMN.get(objUnderTest)  + ", " + IS_GROUP_BY_COLUMN.get(objUnderTest)  + ", " + COLUMN_SPACE_AT_FILE.get(objUnderTest)  + ", " + FILE_ORDER_BY.get(objUnderTest)  + ", " + PLUGIN_METHOD_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest)  + ", " + TARGET_CONNECTION_ID.get(objUnderTest)  + ", " + TARGET_TABLE_ID.get(objUnderTest)  + ", " + COLUMN_ID_TARGET_COLUMN.get(objUnderTest)  + ", " + SOURCE_CONNECTION_ID.get(objUnderTest)  + ", " + SOURCE_TABLE_ID.get(objUnderTest)  + ", " + COLUMN_ID_SOURCE_COLUMN.get(objUnderTest)  + ", " + TRANSFORMATION_ID.get(objUnderTest)  + ", " + TRANSF_TABLE_ID.get(objUnderTest)  + ", " + PAR_NAME.get(objUnderTest)  + ", " + FILE_ID.get(objUnderTest)  + ", " + PLUGIN_ID.get(objUnderTest)  + ", " + FREE_FORMAT_TRANSFORMAT.get(objUnderTest)  + ", " + METHOD_PARAMETER.get(objUnderTest) ;
    String expected =  "1"  + ", testIS_PK_COLUMN"  + ", testIS_SUM_COLUMN"  + ", testIS_GROUP_BY_COLUMN"  + ", 1"  + ", 1"  + ", testPLUGIN_METHOD_NAME"  + ", testVERSION_NUMBER"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testPAR_NAME"  + ", 1"  + ", 1"  + ", testFREE_FORMAT_TRANSFORMAT"  + ", testMETHOD_PARAMETER" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_jointsConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_joints(null ,  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_joints constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_jointsConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_joints whereObject = new Meta_joints(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_joints(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  ID.get(objUnderTest)  + ", " + IS_PK_COLUMN.get(objUnderTest)  + ", " + IS_SUM_COLUMN.get(objUnderTest)  + ", " + IS_GROUP_BY_COLUMN.get(objUnderTest)  + ", " + COLUMN_SPACE_AT_FILE.get(objUnderTest)  + ", " + FILE_ORDER_BY.get(objUnderTest)  + ", " + PLUGIN_METHOD_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest)  + ", " + TARGET_CONNECTION_ID.get(objUnderTest)  + ", " + TARGET_TABLE_ID.get(objUnderTest)  + ", " + COLUMN_ID_TARGET_COLUMN.get(objUnderTest)  + ", " + SOURCE_CONNECTION_ID.get(objUnderTest)  + ", " + SOURCE_TABLE_ID.get(objUnderTest)  + ", " + COLUMN_ID_SOURCE_COLUMN.get(objUnderTest)  + ", " + TRANSFORMATION_ID.get(objUnderTest)  + ", " + TRANSF_TABLE_ID.get(objUnderTest)  + ", " + PAR_NAME.get(objUnderTest)  + ", " + FILE_ID.get(objUnderTest)  + ", " + PLUGIN_ID.get(objUnderTest)  + ", " + FREE_FORMAT_TRANSFORMAT.get(objUnderTest)  + ", " + METHOD_PARAMETER.get(objUnderTest) ;
    String expected =  "1"  + ", testIS_PK_COLUMN"  + ", testIS_SUM_COLUMN"  + ", testIS_GROUP_BY_COLUMN"  + ", 1"  + ", 1"  + ", testPLUGIN_METHOD_NAME"  + ", testVERSION_NUMBER"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testPAR_NAME"  + ", 1"  + ", 1"  + ", testFREE_FORMAT_TRANSFORMAT"  + ", testMETHOD_PARAMETER" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_jointsConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_joints whereObject = new Meta_joints(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_joints(null, whereObject);
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
    assertEquals("Meta_joints", objUnderTest.getTableName());
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
    Meta_joints whereObject = new Meta_joints(rockFactory);

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
    Meta_joints whereObject = new Meta_joints(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_joints");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_joints");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the ID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT ID FROM Meta_joints");
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
    
    String expected = "<Meta_joints ID=\"1\" IS_PK_COLUMN=\"'testIS_PK_COLUMN'\" IS_SUM_COLUMN=\"'testIS_SUM_COLUMN'\" IS_GROUP_BY_COLUMN=\"'testIS_GROUP_BY_COLUMN'\" COLUMN_SPACE_AT_FILE=\"1\" FILE_ORDER_BY=\"1\" PLUGIN_METHOD_NAME=\"'testPLUGIN_METHOD_NAME'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" COLLECTION_SET_ID=\"1\" COLLECTION_ID=\"1\" TRANSFER_ACTION_ID=\"1\" TARGET_CONNECTION_ID=\"1\" TARGET_TABLE_ID=\"1\" COLUMN_ID_TARGET_COLUMN=\"1\" SOURCE_CONNECTION_ID=\"1\" SOURCE_TABLE_ID=\"1\" COLUMN_ID_SOURCE_COLUMN=\"1\" TRANSFORMATION_ID=\"1\" TRANSF_TABLE_ID=\"1\" PAR_NAME=\"'testPAR_NAME'\" FILE_ID=\"1\" PLUGIN_ID=\"1\" FREE_FORMAT_TRANSFORMAT=\"'testFREE_FORMAT_TRANSFORMAT'\" METHOD_PARAMETER=\"'testMETHOD_PARAMETER'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_joints ID=\"1\" IS_PK_COLUMN=\"'testIS_PK_COLUMN'\" IS_SUM_COLUMN=\"'testIS_SUM_COLUMN'\" IS_GROUP_BY_COLUMN=\"'testIS_GROUP_BY_COLUMN'\" COLUMN_SPACE_AT_FILE=\"1\" FILE_ORDER_BY=\"1\" PLUGIN_METHOD_NAME=\"'testPLUGIN_METHOD_NAME'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" COLLECTION_SET_ID=\"1\" COLLECTION_ID=\"1\" TRANSFER_ACTION_ID=\"1\" TARGET_CONNECTION_ID=\"1\" TARGET_TABLE_ID=\"1\" COLUMN_ID_TARGET_COLUMN=\"1\" SOURCE_CONNECTION_ID=\"1\" SOURCE_TABLE_ID=\"1\" COLUMN_ID_SOURCE_COLUMN=\"1\" TRANSFORMATION_ID=\"1\" TRANSF_TABLE_ID=\"1\" PAR_NAME=\"'testPAR_NAME'\" FILE_ID=\"1\" PLUGIN_ID=\"1\" FREE_FORMAT_TRANSFORMAT=\"'testFREE_FORMAT_TRANSFORMAT'\" METHOD_PARAMETER=\"'testMETHOD_PARAMETER'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_joints>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_joints ( ID, IS_PK_COLUMN, IS_SUM_COLUMN, IS_GROUP_BY_COLUMN, COLUMN_SPACE_AT_FILE, FILE_ORDER_BY, PLUGIN_METHOD_NAME, VERSION_NUMBER, COLLECTION_SET_ID, COLLECTION_ID, TRANSFER_ACTION_ID, TARGET_CONNECTION_ID, TARGET_TABLE_ID, COLUMN_ID_TARGET_COLUMN, SOURCE_CONNECTION_ID, SOURCE_TABLE_ID, COLUMN_ID_SOURCE_COLUMN, TRANSFORMATION_ID, TRANSF_TABLE_ID, PAR_NAME, FILE_ID, PLUGIN_ID, FREE_FORMAT_TRANSFORMAT, METHOD_PARAMETER ) values "
      + "( 1, 'testIS_PK_COLUMN', 'testIS_SUM_COLUMN', 'testIS_GROUP_BY_COLUMN', 1, 1, 'testPLUGIN_METHOD_NAME', 'testVERSION_NUMBER', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 'testPAR_NAME', 1, 1, 'testFREE_FORMAT_TRANSFORMAT', 'testMETHOD_PARAMETER' );\n";
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
  public void testSetAndGetIs_pk_column() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIs_pk_column(Meta_jointsTest.testStringGenerator("anotherIS_PK_COLUMN", 1));
    assertEquals(Meta_jointsTest.testStringGenerator("anotherIS_PK_COLUMN", 1), IS_PK_COLUMN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIs_sum_column() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIs_sum_column(Meta_jointsTest.testStringGenerator("anotherIS_SUM_COLUMN", 1));
    assertEquals(Meta_jointsTest.testStringGenerator("anotherIS_SUM_COLUMN", 1), IS_SUM_COLUMN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIs_group_by_column() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIs_group_by_column(Meta_jointsTest.testStringGenerator("anotherIS_GROUP_BY_COLUMN", 1));
    assertEquals(Meta_jointsTest.testStringGenerator("anotherIS_GROUP_BY_COLUMN", 1), IS_GROUP_BY_COLUMN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColumn_space_at_file() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColumn_space_at_file(555L);
    assertEquals(555L, COLUMN_SPACE_AT_FILE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFile_order_by() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFile_order_by(555L);
    assertEquals(555L, FILE_ORDER_BY.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPlugin_method_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPlugin_method_name(Meta_jointsTest.testStringGenerator("anotherPLUGIN_METHOD_NAME", 100));
    assertEquals(Meta_jointsTest.testStringGenerator("anotherPLUGIN_METHOD_NAME", 100), PLUGIN_METHOD_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_jointsTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_jointsTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
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
  public void testSetAndGetTarget_connection_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTarget_connection_id(555L);
    assertEquals(555L, TARGET_CONNECTION_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTarget_table_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTarget_table_id(555L);
    assertEquals(555L, TARGET_TABLE_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColumn_id_target_column() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColumn_id_target_column(555L);
    assertEquals(555L, COLUMN_ID_TARGET_COLUMN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSource_connection_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSource_connection_id(555L);
    assertEquals(555L, SOURCE_CONNECTION_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSource_table_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSource_table_id(555L);
    assertEquals(555L, SOURCE_TABLE_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColumn_id_source_column() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColumn_id_source_column(555L);
    assertEquals(555L, COLUMN_ID_SOURCE_COLUMN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTransformation_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTransformation_id(555L);
    assertEquals(555L, TRANSFORMATION_ID.get(objUnderTest));
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
  public void testSetAndGetPar_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPar_name(Meta_jointsTest.testStringGenerator("anotherPAR_NAME", 30));
    assertEquals(Meta_jointsTest.testStringGenerator("anotherPAR_NAME", 30), PAR_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFile_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFile_id(555L);
    assertEquals(555L, FILE_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPlugin_id() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPlugin_id(555L);
    assertEquals(555L, PLUGIN_ID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFree_format_transformat() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFree_format_transformat(Meta_jointsTest.testStringGenerator("anotherFREE_FORMAT_TRANSFORMAT", 2000));
    assertEquals(Meta_jointsTest.testStringGenerator("anotherFREE_FORMAT_TRANSFORMAT", 2000), FREE_FORMAT_TRANSFORMAT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMethod_parameter() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMethod_parameter(Meta_jointsTest.testStringGenerator("anotherMETHOD_PARAMETER", 200));
    assertEquals(Meta_jointsTest.testStringGenerator("anotherMETHOD_PARAMETER", 200), METHOD_PARAMETER.get(objUnderTest));
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
    objUnderTest = new Meta_joints(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  ID.get(objUnderTest)  + ", " + IS_PK_COLUMN.get(objUnderTest)  + ", " + IS_SUM_COLUMN.get(objUnderTest)  + ", " + IS_GROUP_BY_COLUMN.get(objUnderTest)  + ", " + COLUMN_SPACE_AT_FILE.get(objUnderTest)  + ", " + FILE_ORDER_BY.get(objUnderTest)  + ", " + PLUGIN_METHOD_NAME.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + COLLECTION_SET_ID.get(objUnderTest)  + ", " + COLLECTION_ID.get(objUnderTest)  + ", " + TRANSFER_ACTION_ID.get(objUnderTest)  + ", " + TARGET_CONNECTION_ID.get(objUnderTest)  + ", " + TARGET_TABLE_ID.get(objUnderTest)  + ", " + COLUMN_ID_TARGET_COLUMN.get(objUnderTest)  + ", " + SOURCE_CONNECTION_ID.get(objUnderTest)  + ", " + SOURCE_TABLE_ID.get(objUnderTest)  + ", " + COLUMN_ID_SOURCE_COLUMN.get(objUnderTest)  + ", " + TRANSFORMATION_ID.get(objUnderTest)  + ", " + TRANSF_TABLE_ID.get(objUnderTest)  + ", " + PAR_NAME.get(objUnderTest)  + ", " + FILE_ID.get(objUnderTest)  + ", " + PLUGIN_ID.get(objUnderTest)  + ", " + FREE_FORMAT_TRANSFORMAT.get(objUnderTest)  + ", " + METHOD_PARAMETER.get(objUnderTest) ;
    String expected =  "0"  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", "  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", 0"  + ", 0"  + ", "  + ", " ;
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
    Meta_joints compareObj = new Meta_joints(rockFactory ,  1L );

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
   * Testing comparing another Meta_joints with our current one. If the two
   * Meta_jointss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_joints() throws Exception {

    /* Creating another Meta_joints which will be compared to the tested one */
    Meta_joints comparedObj = new Meta_joints(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_joints with our current one. If the two
   * Meta_jointss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_joints() throws Exception {

    /* Creating another Meta_joints which will be compared to the tested one */
    Meta_joints comparedObj = new Meta_joints(rockFactory ,  1L );
    comparedObj.setId( 7L );

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_joints with our current one. If the two
   * Meta_jointss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_joints() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_joints comparedObj = new Meta_joints(rockFactory ,  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_joints with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_joints() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_joints comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_joints was null \n");
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
    assertEquals(Meta_joints.class, actualObject.getClass());
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
    Meta_joints testAgg = new Meta_joints(rockFactory ,  1L );
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
   * Testing columnsize retrieving for Is_sum_column.
   */
  @Test
  public void testGetIs_sum_columnColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getIs_sum_columnColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Is_sum_column.
  */
  @Test
  public void testGetIs_sum_columnDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIs_sum_columnDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Is_sum_column.
  */
  @Test
  public void testGetIs_sum_columnSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getIs_sum_columnSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Is_group_by_column.
   */
  @Test
  public void testGetIs_group_by_columnColumnSize() throws Exception {
    
     assertEquals(1, objUnderTest.getIs_group_by_columnColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Is_group_by_column.
  */
  @Test
  public void testGetIs_group_by_columnDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIs_group_by_columnDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Is_group_by_column.
  */
  @Test
  public void testGetIs_group_by_columnSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getIs_group_by_columnSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Column_space_at_file.
   */
  @Test
  public void testGetColumn_space_at_fileColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getColumn_space_at_fileColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Column_space_at_file.
  */
  @Test
  public void testGetColumn_space_at_fileDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColumn_space_at_fileDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Column_space_at_file.
  */
  @Test
  public void testGetColumn_space_at_fileSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getColumn_space_at_fileSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for File_order_by.
   */
  @Test
  public void testGetFile_order_byColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getFile_order_byColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for File_order_by.
  */
  @Test
  public void testGetFile_order_byDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFile_order_byDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for File_order_by.
  */
  @Test
  public void testGetFile_order_bySQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getFile_order_bySQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Plugin_method_name.
   */
  @Test
  public void testGetPlugin_method_nameColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getPlugin_method_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Plugin_method_name.
  */
  @Test
  public void testGetPlugin_method_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPlugin_method_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Plugin_method_name.
  */
  @Test
  public void testGetPlugin_method_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPlugin_method_nameSQLType());    
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
   * Testing columnsize retrieving for Target_connection_id.
   */
  @Test
  public void testGetTarget_connection_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getTarget_connection_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Target_connection_id.
  */
  @Test
  public void testGetTarget_connection_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTarget_connection_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Target_connection_id.
  */
  @Test
  public void testGetTarget_connection_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getTarget_connection_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Target_table_id.
   */
  @Test
  public void testGetTarget_table_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getTarget_table_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Target_table_id.
  */
  @Test
  public void testGetTarget_table_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTarget_table_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Target_table_id.
  */
  @Test
  public void testGetTarget_table_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getTarget_table_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Column_id_target_column.
   */
  @Test
  public void testGetColumn_id_target_columnColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getColumn_id_target_columnColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Column_id_target_column.
  */
  @Test
  public void testGetColumn_id_target_columnDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColumn_id_target_columnDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Column_id_target_column.
  */
  @Test
  public void testGetColumn_id_target_columnSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getColumn_id_target_columnSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Source_connection_id.
   */
  @Test
  public void testGetSource_connection_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getSource_connection_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Source_connection_id.
  */
  @Test
  public void testGetSource_connection_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSource_connection_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Source_connection_id.
  */
  @Test
  public void testGetSource_connection_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getSource_connection_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Source_table_id.
   */
  @Test
  public void testGetSource_table_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getSource_table_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Source_table_id.
  */
  @Test
  public void testGetSource_table_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSource_table_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Source_table_id.
  */
  @Test
  public void testGetSource_table_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getSource_table_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Column_id_source_column.
   */
  @Test
  public void testGetColumn_id_source_columnColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getColumn_id_source_columnColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Column_id_source_column.
  */
  @Test
  public void testGetColumn_id_source_columnDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColumn_id_source_columnDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Column_id_source_column.
  */
  @Test
  public void testGetColumn_id_source_columnSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getColumn_id_source_columnSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Transformation_id.
   */
  @Test
  public void testGetTransformation_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getTransformation_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Transformation_id.
  */
  @Test
  public void testGetTransformation_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTransformation_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Transformation_id.
  */
  @Test
  public void testGetTransformation_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getTransformation_idSQLType());    
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
   * Testing columnsize retrieving for Par_name.
   */
  @Test
  public void testGetPar_nameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getPar_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Par_name.
  */
  @Test
  public void testGetPar_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPar_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Par_name.
  */
  @Test
  public void testGetPar_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPar_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for File_id.
   */
  @Test
  public void testGetFile_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getFile_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for File_id.
  */
  @Test
  public void testGetFile_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFile_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for File_id.
  */
  @Test
  public void testGetFile_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getFile_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Plugin_id.
   */
  @Test
  public void testGetPlugin_idColumnSize() throws Exception {
    
     assertEquals(38, objUnderTest.getPlugin_idColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Plugin_id.
  */
  @Test
  public void testGetPlugin_idDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPlugin_idDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Plugin_id.
  */
  @Test
  public void testGetPlugin_idSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getPlugin_idSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Free_format_transformat.
   */
  @Test
  public void testGetFree_format_transformatColumnSize() throws Exception {
    
     assertEquals(2000, objUnderTest.getFree_format_transformatColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Free_format_transformat.
  */
  @Test
  public void testGetFree_format_transformatDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFree_format_transformatDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Free_format_transformat.
  */
  @Test
  public void testGetFree_format_transformatSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getFree_format_transformatSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Method_parameter.
   */
  @Test
  public void testGetMethod_parameterColumnSize() throws Exception {
    
     assertEquals(200, objUnderTest.getMethod_parameterColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Method_parameter.
  */
  @Test
  public void testGetMethod_parameterDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMethod_parameterDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Method_parameter.
  */
  @Test
  public void testGetMethod_parameterSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMethod_parameterSQLType());    
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
   * Testing original Meta_joints object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_joints(rockFactory, false);
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
    Meta_joints changedOriginal = new Meta_joints(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_joints(rockFactory, false);
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
    Meta_joints changedOriginal = new Meta_joints(rockFactory, false);
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