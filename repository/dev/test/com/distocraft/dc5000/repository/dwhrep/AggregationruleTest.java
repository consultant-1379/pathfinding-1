package com.distocraft.dc5000.repository.dwhrep;

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
 * Test class for Aggregationrule. Changes to Aggregationrule table are made via
 * this class.
 */
public class AggregationruleTest {

  private static Aggregationrule objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field AGGREGATION;
  
  private static Field VERSIONID;
  
  private static Field RULEID;
  
  private static Field TARGET_TYPE;
  
  private static Field TARGET_LEVEL;
  
  private static Field TARGET_TABLE;
  
  private static Field TARGET_MTABLEID;
  
  private static Field SOURCE_TYPE;
  
  private static Field SOURCE_LEVEL;
  
  private static Field SOURCE_TABLE;
  
  private static Field SOURCE_MTABLEID;
  
  private static Field RULETYPE;
  
  private static Field AGGREGATIONSCOPE;
  
  private static Field BHTYPE;
  
  private static Field ENABLE;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Aggregationrule.class.getDeclaredField("newItem");
		AGGREGATION = Aggregationrule.class.getDeclaredField("AGGREGATION");
		VERSIONID = Aggregationrule.class.getDeclaredField("VERSIONID");
		RULEID = Aggregationrule.class.getDeclaredField("RULEID");
		TARGET_TYPE = Aggregationrule.class.getDeclaredField("TARGET_TYPE");
		TARGET_LEVEL = Aggregationrule.class.getDeclaredField("TARGET_LEVEL");
		TARGET_TABLE = Aggregationrule.class.getDeclaredField("TARGET_TABLE");
		TARGET_MTABLEID = Aggregationrule.class.getDeclaredField("TARGET_MTABLEID");
		SOURCE_TYPE = Aggregationrule.class.getDeclaredField("SOURCE_TYPE");
		SOURCE_LEVEL = Aggregationrule.class.getDeclaredField("SOURCE_LEVEL");
		SOURCE_TABLE = Aggregationrule.class.getDeclaredField("SOURCE_TABLE");
		SOURCE_MTABLEID = Aggregationrule.class.getDeclaredField("SOURCE_MTABLEID");
		RULETYPE = Aggregationrule.class.getDeclaredField("RULETYPE");
		AGGREGATIONSCOPE = Aggregationrule.class.getDeclaredField("AGGREGATIONSCOPE");
		BHTYPE = Aggregationrule.class.getDeclaredField("BHTYPE");
		ENABLE = Aggregationrule.class.getDeclaredField("ENABLE");
		newItem.setAccessible(true);
		AGGREGATION.setAccessible(true);
		VERSIONID.setAccessible(true);
		RULEID.setAccessible(true);
		TARGET_TYPE.setAccessible(true);
		TARGET_LEVEL.setAccessible(true);
		TARGET_TABLE.setAccessible(true);
		TARGET_MTABLEID.setAccessible(true);
		SOURCE_TYPE.setAccessible(true);
		SOURCE_LEVEL.setAccessible(true);
		SOURCE_TABLE.setAccessible(true);
		SOURCE_MTABLEID.setAccessible(true);
		RULETYPE.setAccessible(true);
		AGGREGATIONSCOPE.setAccessible(true);
		BHTYPE.setAccessible(true);
		ENABLE.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Aggregationrule ( AGGREGATION VARCHAR(31)  ,VERSIONID VARCHAR(31) ,RULEID INTEGER  ,TARGET_TYPE VARCHAR(31) ,TARGET_LEVEL VARCHAR(31) ,TARGET_TABLE VARCHAR(31) ,TARGET_MTABLEID VARCHAR(31) ,SOURCE_TYPE VARCHAR(31) ,SOURCE_LEVEL VARCHAR(31) ,SOURCE_TABLE VARCHAR(31) ,SOURCE_MTABLEID VARCHAR(31) ,RULETYPE VARCHAR(31) ,AGGREGATIONSCOPE VARCHAR(31) ,BHTYPE VARCHAR(31) ,ENABLE INTEGER )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Aggregationrule");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Aggregationrule VALUES( 'testAGGREGATION'  ,'testVERSIONID'  ,1  ,'testTARGET_TYPE'  ,'testTARGET_LEVEL'  ,'testTARGET_TABLE'  ,'testTARGET_MTABLEID'  ,'testSOURCE_TYPE'  ,'testSOURCE_LEVEL'  ,'testSOURCE_TABLE'  ,'testSOURCE_MTABLEID'  ,'testRULETYPE'  ,'testAGGREGATIONSCOPE'  ,'testBHTYPE'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Aggregationrule(rockFactory ,  "testAGGREGATION",  "testVERSIONID",  1 ,  "testRULETYPE");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Aggregationrule");
    objUnderTest = null;
  }
  
  /**
   * Testing Aggregationrule constructor variable initialization with null values.
   */
  @Test
  public void testAggregationruleConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Aggregationrule(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  AGGREGATION.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + RULEID.get(objUnderTest)  + ", " + TARGET_TYPE.get(objUnderTest)  + ", " + TARGET_LEVEL.get(objUnderTest)  + ", " + TARGET_TABLE.get(objUnderTest)  + ", " + TARGET_MTABLEID.get(objUnderTest)  + ", " + SOURCE_TYPE.get(objUnderTest)  + ", " + SOURCE_LEVEL.get(objUnderTest)  + ", " + SOURCE_TABLE.get(objUnderTest)  + ", " + SOURCE_MTABLEID.get(objUnderTest)  + ", " + RULETYPE.get(objUnderTest)  + ", " + AGGREGATIONSCOPE.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Aggregationrule constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testAggregationruleConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Aggregationrule(rockFactory ,  "testAGGREGATION",  "testVERSIONID",  1 ,  "testRULETYPE");

    /* Asserting that variables are initialized */
    String actual =  AGGREGATION.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + RULEID.get(objUnderTest)  + ", " + TARGET_TYPE.get(objUnderTest)  + ", " + TARGET_LEVEL.get(objUnderTest)  + ", " + TARGET_TABLE.get(objUnderTest)  + ", " + TARGET_MTABLEID.get(objUnderTest)  + ", " + SOURCE_TYPE.get(objUnderTest)  + ", " + SOURCE_LEVEL.get(objUnderTest)  + ", " + SOURCE_TABLE.get(objUnderTest)  + ", " + SOURCE_MTABLEID.get(objUnderTest)  + ", " + RULETYPE.get(objUnderTest)  + ", " + AGGREGATIONSCOPE.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest) ;
    String expected =  "testAGGREGATION"  + ", testVERSIONID"  + ", 1"  + ", testTARGET_TYPE"  + ", testTARGET_LEVEL"  + ", testTARGET_TABLE"  + ", testTARGET_MTABLEID"  + ", testSOURCE_TYPE"  + ", testSOURCE_LEVEL"  + ", testSOURCE_TABLE"  + ", testSOURCE_MTABLEID"  + ", testRULETYPE"  + ", testAGGREGATIONSCOPE"  + ", testBHTYPE"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAggregationruleConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Aggregationrule(null ,  "testAGGREGATION",  "testVERSIONID",  1 ,  "testRULETYPE");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Aggregationrule constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testAggregationruleConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Aggregationrule whereObject = new Aggregationrule(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Aggregationrule(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  AGGREGATION.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + RULEID.get(objUnderTest)  + ", " + TARGET_TYPE.get(objUnderTest)  + ", " + TARGET_LEVEL.get(objUnderTest)  + ", " + TARGET_TABLE.get(objUnderTest)  + ", " + TARGET_MTABLEID.get(objUnderTest)  + ", " + SOURCE_TYPE.get(objUnderTest)  + ", " + SOURCE_LEVEL.get(objUnderTest)  + ", " + SOURCE_TABLE.get(objUnderTest)  + ", " + SOURCE_MTABLEID.get(objUnderTest)  + ", " + RULETYPE.get(objUnderTest)  + ", " + AGGREGATIONSCOPE.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest) ;
    String expected =  "testAGGREGATION"  + ", testVERSIONID"  + ", 1"  + ", testTARGET_TYPE"  + ", testTARGET_LEVEL"  + ", testTARGET_TABLE"  + ", testTARGET_MTABLEID"  + ", testSOURCE_TYPE"  + ", testSOURCE_LEVEL"  + ", testSOURCE_TABLE"  + ", testSOURCE_MTABLEID"  + ", testRULETYPE"  + ", testAGGREGATIONSCOPE"  + ", testBHTYPE"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAggregationruleConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Aggregationrule whereObject = new Aggregationrule(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Aggregationrule(null, whereObject);
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
    assertEquals("Aggregationrule", objUnderTest.getTableName());
  }
  
  /**
   * Testing database updating. Affected row count is returned.
   */
  @Test
  public void testUpdateDB() throws Exception {

    /* Invoking tested method and asserting the update has been made */
    String actual = objUnderTest.updateDB() + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + false, actual);
  }
  
  /**
   * Testing database updating. Affected row count is returned.
   */
  @Test
  public void testUpdateDBwithTimestamp() throws Exception {

    /* Invoking tested method and asserting the update has been made */
    String actual = objUnderTest.updateDB(true) + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + false, actual);
  }
  
  /**
   * Testing database updating. Affected row count is returned.
   */
  @Test
  public void testUpdateDBWithConstructedWhereClause() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Aggregationrule whereObject = new Aggregationrule(rockFactory);

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
    Aggregationrule whereObject = new Aggregationrule(rockFactory);

    /* Invoking tested method and asserting the delete has been made */
    String actual = objUnderTest.deleteDB(whereObject) + ", " + newItem.get(objUnderTest);
    assertEquals("1, " + true, actual);
  }
  
  /**
   * Testing data saving to the database.
   */
  @Test
  public void testSaveDB() throws Exception {


    /* Calling the tested method twice with different setting */
    objUnderTest.saveDB();
    newItem.set(objUnderTest, true);
    objUnderTest.saveDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Aggregationrule");
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

    /* Calling the tested method twice, first insert, next update */
    newItem.set(objUnderTest, true);
    objUnderTest.saveToDB();
    AGGREGATION.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("AGGREGATION");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Aggregationrule");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the AGGREGATION column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT AGGREGATION FROM Aggregationrule");
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
    
    String expected = "<Aggregationrule AGGREGATION=\"'testAGGREGATION'\" VERSIONID=\"'testVERSIONID'\" RULEID=\"1\" TARGET_TYPE=\"'testTARGET_TYPE'\" TARGET_LEVEL=\"'testTARGET_LEVEL'\" TARGET_TABLE=\"'testTARGET_TABLE'\" TARGET_MTABLEID=\"'testTARGET_MTABLEID'\" SOURCE_TYPE=\"'testSOURCE_TYPE'\" SOURCE_LEVEL=\"'testSOURCE_LEVEL'\" SOURCE_TABLE=\"'testSOURCE_TABLE'\" SOURCE_MTABLEID=\"'testSOURCE_MTABLEID'\" RULETYPE=\"'testRULETYPE'\" AGGREGATIONSCOPE=\"'testAGGREGATIONSCOPE'\" BHTYPE=\"'testBHTYPE'\" ENABLE=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Aggregationrule AGGREGATION=\"'testAGGREGATION'\" VERSIONID=\"'testVERSIONID'\" RULEID=\"1\" TARGET_TYPE=\"'testTARGET_TYPE'\" TARGET_LEVEL=\"'testTARGET_LEVEL'\" TARGET_TABLE=\"'testTARGET_TABLE'\" TARGET_MTABLEID=\"'testTARGET_MTABLEID'\" SOURCE_TYPE=\"'testSOURCE_TYPE'\" SOURCE_LEVEL=\"'testSOURCE_LEVEL'\" SOURCE_TABLE=\"'testSOURCE_TABLE'\" SOURCE_MTABLEID=\"'testSOURCE_MTABLEID'\" RULETYPE=\"'testRULETYPE'\" AGGREGATIONSCOPE=\"'testAGGREGATIONSCOPE'\" BHTYPE=\"'testBHTYPE'\" ENABLE=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Aggregationrule>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Aggregationrule ( AGGREGATION, VERSIONID, RULEID, TARGET_TYPE, TARGET_LEVEL, TARGET_TABLE, TARGET_MTABLEID, SOURCE_TYPE, SOURCE_LEVEL, SOURCE_TABLE, SOURCE_MTABLEID, RULETYPE, AGGREGATIONSCOPE, BHTYPE, ENABLE ) values "
      + "( 'testAGGREGATION', 'testVERSIONID', 1, 'testTARGET_TYPE', 'testTARGET_LEVEL', 'testTARGET_TABLE', 'testTARGET_MTABLEID', 'testSOURCE_TYPE', 'testSOURCE_LEVEL', 'testSOURCE_TABLE', 'testSOURCE_MTABLEID', 'testRULETYPE', 'testAGGREGATIONSCOPE', 'testBHTYPE', 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetAggregation() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setAggregation(AggregationruleTest.testStringGenerator("anotherAGGREGATION", 255));
    assertEquals(AggregationruleTest.testStringGenerator("anotherAGGREGATION", 255), AGGREGATION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersionid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersionid(AggregationruleTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(AggregationruleTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetRuleid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setRuleid(555);
    assertEquals(555, RULEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTarget_type() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTarget_type(AggregationruleTest.testStringGenerator("anotherTARGET_TYPE", 50));
    assertEquals(AggregationruleTest.testStringGenerator("anotherTARGET_TYPE", 50), TARGET_TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTarget_level() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTarget_level(AggregationruleTest.testStringGenerator("anotherTARGET_LEVEL", 50));
    assertEquals(AggregationruleTest.testStringGenerator("anotherTARGET_LEVEL", 50), TARGET_LEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTarget_table() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTarget_table(AggregationruleTest.testStringGenerator("anotherTARGET_TABLE", 255));
    assertEquals(AggregationruleTest.testStringGenerator("anotherTARGET_TABLE", 255), TARGET_TABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTarget_mtableid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTarget_mtableid(AggregationruleTest.testStringGenerator("anotherTARGET_MTABLEID", 255));
    assertEquals(AggregationruleTest.testStringGenerator("anotherTARGET_MTABLEID", 255), TARGET_MTABLEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSource_type() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSource_type(AggregationruleTest.testStringGenerator("anotherSOURCE_TYPE", 50));
    assertEquals(AggregationruleTest.testStringGenerator("anotherSOURCE_TYPE", 50), SOURCE_TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSource_level() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSource_level(AggregationruleTest.testStringGenerator("anotherSOURCE_LEVEL", 50));
    assertEquals(AggregationruleTest.testStringGenerator("anotherSOURCE_LEVEL", 50), SOURCE_LEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSource_table() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSource_table(AggregationruleTest.testStringGenerator("anotherSOURCE_TABLE", 255));
    assertEquals(AggregationruleTest.testStringGenerator("anotherSOURCE_TABLE", 255), SOURCE_TABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSource_mtableid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSource_mtableid(AggregationruleTest.testStringGenerator("anotherSOURCE_MTABLEID", 255));
    assertEquals(AggregationruleTest.testStringGenerator("anotherSOURCE_MTABLEID", 255), SOURCE_MTABLEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetRuletype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setRuletype(AggregationruleTest.testStringGenerator("anotherRULETYPE", 50));
    assertEquals(AggregationruleTest.testStringGenerator("anotherRULETYPE", 50), RULETYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetAggregationscope() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setAggregationscope(AggregationruleTest.testStringGenerator("anotherAGGREGATIONSCOPE", 50));
    assertEquals(AggregationruleTest.testStringGenerator("anotherAGGREGATIONSCOPE", 50), AGGREGATIONSCOPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBhtype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBhtype(AggregationruleTest.testStringGenerator("anotherBHTYPE", 50));
    assertEquals(AggregationruleTest.testStringGenerator("anotherBHTYPE", 50), BHTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetEnable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setEnable(555);
    assertEquals(555, ENABLE.get(objUnderTest));
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
    String[] expectedKeys = { "AGGREGATION","VERSIONID","RULEID","RULETYPE"};

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
    objUnderTest = new Aggregationrule(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  AGGREGATION.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + RULEID.get(objUnderTest)  + ", " + TARGET_TYPE.get(objUnderTest)  + ", " + TARGET_LEVEL.get(objUnderTest)  + ", " + TARGET_TABLE.get(objUnderTest)  + ", " + TARGET_MTABLEID.get(objUnderTest)  + ", " + SOURCE_TYPE.get(objUnderTest)  + ", " + SOURCE_LEVEL.get(objUnderTest)  + ", " + SOURCE_TABLE.get(objUnderTest)  + ", " + SOURCE_MTABLEID.get(objUnderTest)  + ", " + RULETYPE.get(objUnderTest)  + ", " + AGGREGATIONSCOPE.get(objUnderTest)  + ", " + BHTYPE.get(objUnderTest)  + ", " + ENABLE.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", 0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0" ;
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
    Aggregationrule compareObj = new Aggregationrule(rockFactory ,  "testAGGREGATION",  "testVERSIONID",  1 ,  "testRULETYPE");

    /* Testing first with null primary key value */
    AGGREGATION.set(objUnderTest, null);
  	VERSIONID.set(objUnderTest, null);
  	RULEID.set(objUnderTest, null);
  	RULETYPE.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    AGGREGATION.set(objUnderTest,  "different");
  	VERSIONID.set(objUnderTest,  "different");
  	RULEID.set(objUnderTest,  7 );
  	RULETYPE.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    AGGREGATION.set(objUnderTest,  "testAGGREGATION");
  	VERSIONID.set(objUnderTest,  "testVERSIONID");
  	RULEID.set(objUnderTest,  1 );
  	RULETYPE.set(objUnderTest,  "testRULETYPE");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Aggregationrule with our current one. If the two
   * Aggregationrules are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnAggregationrule() throws Exception {

    /* Creating another Aggregationrule which will be compared to the tested one */
    Aggregationrule comparedObj = new Aggregationrule(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Aggregationrule with our current one. If the two
   * Aggregationrules are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAggregationrule() throws Exception {

    /* Creating another Aggregationrule which will be compared to the tested one */
    Aggregationrule comparedObj = new Aggregationrule(rockFactory ,  "testAGGREGATION",  "testVERSIONID",  1 ,  "testRULETYPE");
    comparedObj.setAggregation( "DifferentAGGREGATION");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Aggregationrule with our current one. If the two
   * Aggregationrules are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameAggregationrule() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Aggregationrule comparedObj = new Aggregationrule(rockFactory ,  "testAGGREGATION",  "testVERSIONID",  1 ,  "testRULETYPE");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Aggregationrule with our current one using null value.
   */
  @Test
  public void testEqualsWithNullAggregationrule() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Aggregationrule comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Aggregationrule was null \n");
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
    assertEquals(Aggregationrule.class, actualObject.getClass());
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
    Aggregationrule testAgg = new Aggregationrule(rockFactory ,  "testAGGREGATION",  "testVERSIONID",  1 ,  "testRULETYPE");
    AGGREGATION.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Aggregation.
   */
  @Test
  public void testGetAggregationColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getAggregationColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Aggregation.
  */
  @Test
  public void testGetAggregationDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getAggregationDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Aggregation.
  */
  @Test
  public void testGetAggregationSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getAggregationSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Versionid.
   */
  @Test
  public void testGetVersionidColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getVersionidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Versionid.
  */
  @Test
  public void testGetVersionidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getVersionidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Versionid.
  */
  @Test
  public void testGetVersionidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getVersionidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Ruleid.
   */
  @Test
  public void testGetRuleidColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getRuleidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Ruleid.
  */
  @Test
  public void testGetRuleidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getRuleidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Ruleid.
  */
  @Test
  public void testGetRuleidSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getRuleidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Target_type.
   */
  @Test
  public void testGetTarget_typeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getTarget_typeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Target_type.
  */
  @Test
  public void testGetTarget_typeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTarget_typeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Target_type.
  */
  @Test
  public void testGetTarget_typeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTarget_typeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Target_level.
   */
  @Test
  public void testGetTarget_levelColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getTarget_levelColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Target_level.
  */
  @Test
  public void testGetTarget_levelDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTarget_levelDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Target_level.
  */
  @Test
  public void testGetTarget_levelSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTarget_levelSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Target_table.
   */
  @Test
  public void testGetTarget_tableColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getTarget_tableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Target_table.
  */
  @Test
  public void testGetTarget_tableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTarget_tableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Target_table.
  */
  @Test
  public void testGetTarget_tableSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTarget_tableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Target_mtableid.
   */
  @Test
  public void testGetTarget_mtableidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getTarget_mtableidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Target_mtableid.
  */
  @Test
  public void testGetTarget_mtableidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTarget_mtableidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Target_mtableid.
  */
  @Test
  public void testGetTarget_mtableidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTarget_mtableidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Source_type.
   */
  @Test
  public void testGetSource_typeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getSource_typeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Source_type.
  */
  @Test
  public void testGetSource_typeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSource_typeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Source_type.
  */
  @Test
  public void testGetSource_typeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSource_typeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Source_level.
   */
  @Test
  public void testGetSource_levelColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getSource_levelColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Source_level.
  */
  @Test
  public void testGetSource_levelDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSource_levelDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Source_level.
  */
  @Test
  public void testGetSource_levelSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSource_levelSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Source_table.
   */
  @Test
  public void testGetSource_tableColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getSource_tableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Source_table.
  */
  @Test
  public void testGetSource_tableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSource_tableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Source_table.
  */
  @Test
  public void testGetSource_tableSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSource_tableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Source_mtableid.
   */
  @Test
  public void testGetSource_mtableidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getSource_mtableidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Source_mtableid.
  */
  @Test
  public void testGetSource_mtableidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSource_mtableidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Source_mtableid.
  */
  @Test
  public void testGetSource_mtableidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSource_mtableidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Ruletype.
   */
  @Test
  public void testGetRuletypeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getRuletypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Ruletype.
  */
  @Test
  public void testGetRuletypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getRuletypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Ruletype.
  */
  @Test
  public void testGetRuletypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getRuletypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Aggregationscope.
   */
  @Test
  public void testGetAggregationscopeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getAggregationscopeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Aggregationscope.
  */
  @Test
  public void testGetAggregationscopeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getAggregationscopeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Aggregationscope.
  */
  @Test
  public void testGetAggregationscopeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getAggregationscopeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Bhtype.
   */
  @Test
  public void testGetBhtypeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getBhtypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Bhtype.
  */
  @Test
  public void testGetBhtypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBhtypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Bhtype.
  */
  @Test
  public void testGetBhtypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBhtypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Enable.
   */
  @Test
  public void testGetEnableColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getEnableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Enable.
  */
  @Test
  public void testGetEnableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getEnableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Enable.
  */
  @Test
  public void testGetEnableSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getEnableSQLType());    
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
   * Testing original Aggregationrule object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Aggregationrule(rockFactory, false);
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
    Aggregationrule changedOriginal = new Aggregationrule(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Aggregationrule(rockFactory, false);
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
    Aggregationrule changedOriginal = new Aggregationrule(rockFactory, false);
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