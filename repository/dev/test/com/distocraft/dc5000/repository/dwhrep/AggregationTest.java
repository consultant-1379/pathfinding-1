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
 * Test class for Aggregation. Changes to Aggregation table are made via
 * this class.
 */
public class AggregationTest {

  private static Aggregation objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field AGGREGATION;
  
  private static Field VERSIONID;
  
  private static Field AGGREGATIONSET;
  
  private static Field AGGREGATIONGROUP;
  
  private static Field REAGGREGATIONSET;
  
  private static Field REAGGREGATIONGROUP;
  
  private static Field GROUPORDER;
  
  private static Field AGGREGATIONORDER;
  
  private static Field AGGREGATIONTYPE;
  
  private static Field AGGREGATIONSCOPE;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Aggregation.class.getDeclaredField("newItem");
		AGGREGATION = Aggregation.class.getDeclaredField("AGGREGATION");
		VERSIONID = Aggregation.class.getDeclaredField("VERSIONID");
		AGGREGATIONSET = Aggregation.class.getDeclaredField("AGGREGATIONSET");
		AGGREGATIONGROUP = Aggregation.class.getDeclaredField("AGGREGATIONGROUP");
		REAGGREGATIONSET = Aggregation.class.getDeclaredField("REAGGREGATIONSET");
		REAGGREGATIONGROUP = Aggregation.class.getDeclaredField("REAGGREGATIONGROUP");
		GROUPORDER = Aggregation.class.getDeclaredField("GROUPORDER");
		AGGREGATIONORDER = Aggregation.class.getDeclaredField("AGGREGATIONORDER");
		AGGREGATIONTYPE = Aggregation.class.getDeclaredField("AGGREGATIONTYPE");
		AGGREGATIONSCOPE = Aggregation.class.getDeclaredField("AGGREGATIONSCOPE");
		newItem.setAccessible(true);
		AGGREGATION.setAccessible(true);
		VERSIONID.setAccessible(true);
		AGGREGATIONSET.setAccessible(true);
		AGGREGATIONGROUP.setAccessible(true);
		REAGGREGATIONSET.setAccessible(true);
		REAGGREGATIONGROUP.setAccessible(true);
		GROUPORDER.setAccessible(true);
		AGGREGATIONORDER.setAccessible(true);
		AGGREGATIONTYPE.setAccessible(true);
		AGGREGATIONSCOPE.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Aggregation ( AGGREGATION VARCHAR(31)  ,VERSIONID VARCHAR(31) ,AGGREGATIONSET VARCHAR(31) ,AGGREGATIONGROUP VARCHAR(31) ,REAGGREGATIONSET VARCHAR(31) ,REAGGREGATIONGROUP VARCHAR(31) ,GROUPORDER INTEGER  ,AGGREGATIONORDER INTEGER  ,AGGREGATIONTYPE VARCHAR(31) ,AGGREGATIONSCOPE VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Aggregation");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Aggregation VALUES( 'testAGGREGATION'  ,'testVERSIONID'  ,'testAGGREGATIONSET'  ,'testAGGREGATIONGROUP'  ,'testREAGGREGATIONSET'  ,'testREAGGREGATIONGROUP'  ,1  ,1  ,'testAGGREGATIONTYPE'  ,'testAGGREGATIONSCOPE' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Aggregation(rockFactory ,  "testAGGREGATION",  "testVERSIONID");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Aggregation");
    objUnderTest = null;
  }
  
  /**
   * Testing Aggregation constructor variable initialization with null values.
   */
  @Test
  public void testAggregationConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Aggregation(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  AGGREGATION.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + AGGREGATIONSET.get(objUnderTest)  + ", " + AGGREGATIONGROUP.get(objUnderTest)  + ", " + REAGGREGATIONSET.get(objUnderTest)  + ", " + REAGGREGATIONGROUP.get(objUnderTest)  + ", " + GROUPORDER.get(objUnderTest)  + ", " + AGGREGATIONORDER.get(objUnderTest)  + ", " + AGGREGATIONTYPE.get(objUnderTest)  + ", " + AGGREGATIONSCOPE.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Aggregation constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testAggregationConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Aggregation(rockFactory ,  "testAGGREGATION",  "testVERSIONID");

    /* Asserting that variables are initialized */
    String actual =  AGGREGATION.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + AGGREGATIONSET.get(objUnderTest)  + ", " + AGGREGATIONGROUP.get(objUnderTest)  + ", " + REAGGREGATIONSET.get(objUnderTest)  + ", " + REAGGREGATIONGROUP.get(objUnderTest)  + ", " + GROUPORDER.get(objUnderTest)  + ", " + AGGREGATIONORDER.get(objUnderTest)  + ", " + AGGREGATIONTYPE.get(objUnderTest)  + ", " + AGGREGATIONSCOPE.get(objUnderTest) ;
    String expected =  "testAGGREGATION"  + ", testVERSIONID"  + ", testAGGREGATIONSET"  + ", testAGGREGATIONGROUP"  + ", testREAGGREGATIONSET"  + ", testREAGGREGATIONGROUP"  + ", 1"  + ", 1"  + ", testAGGREGATIONTYPE"  + ", testAGGREGATIONSCOPE" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAggregationConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Aggregation(null ,  "testAGGREGATION",  "testVERSIONID");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Aggregation constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testAggregationConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Aggregation whereObject = new Aggregation(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Aggregation(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  AGGREGATION.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + AGGREGATIONSET.get(objUnderTest)  + ", " + AGGREGATIONGROUP.get(objUnderTest)  + ", " + REAGGREGATIONSET.get(objUnderTest)  + ", " + REAGGREGATIONGROUP.get(objUnderTest)  + ", " + GROUPORDER.get(objUnderTest)  + ", " + AGGREGATIONORDER.get(objUnderTest)  + ", " + AGGREGATIONTYPE.get(objUnderTest)  + ", " + AGGREGATIONSCOPE.get(objUnderTest) ;
    String expected =  "testAGGREGATION"  + ", testVERSIONID"  + ", testAGGREGATIONSET"  + ", testAGGREGATIONGROUP"  + ", testREAGGREGATIONSET"  + ", testREAGGREGATIONGROUP"  + ", 1"  + ", 1"  + ", testAGGREGATIONTYPE"  + ", testAGGREGATIONSCOPE" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAggregationConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Aggregation whereObject = new Aggregation(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Aggregation(null, whereObject);
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
    assertEquals("Aggregation", objUnderTest.getTableName());
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
    Aggregation whereObject = new Aggregation(rockFactory);

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
    Aggregation whereObject = new Aggregation(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Aggregation");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Aggregation");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the AGGREGATION column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT AGGREGATION FROM Aggregation");
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
    
    String expected = "<Aggregation AGGREGATION=\"'testAGGREGATION'\" VERSIONID=\"'testVERSIONID'\" AGGREGATIONSET=\"'testAGGREGATIONSET'\" AGGREGATIONGROUP=\"'testAGGREGATIONGROUP'\" REAGGREGATIONSET=\"'testREAGGREGATIONSET'\" REAGGREGATIONGROUP=\"'testREAGGREGATIONGROUP'\" GROUPORDER=\"1\" AGGREGATIONORDER=\"1\" AGGREGATIONTYPE=\"'testAGGREGATIONTYPE'\" AGGREGATIONSCOPE=\"'testAGGREGATIONSCOPE'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Aggregation AGGREGATION=\"'testAGGREGATION'\" VERSIONID=\"'testVERSIONID'\" AGGREGATIONSET=\"'testAGGREGATIONSET'\" AGGREGATIONGROUP=\"'testAGGREGATIONGROUP'\" REAGGREGATIONSET=\"'testREAGGREGATIONSET'\" REAGGREGATIONGROUP=\"'testREAGGREGATIONGROUP'\" GROUPORDER=\"1\" AGGREGATIONORDER=\"1\" AGGREGATIONTYPE=\"'testAGGREGATIONTYPE'\" AGGREGATIONSCOPE=\"'testAGGREGATIONSCOPE'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Aggregation>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Aggregation ( AGGREGATION, VERSIONID, AGGREGATIONSET, AGGREGATIONGROUP, REAGGREGATIONSET, REAGGREGATIONGROUP, GROUPORDER, AGGREGATIONORDER, AGGREGATIONTYPE, AGGREGATIONSCOPE ) values "
      + "( 'testAGGREGATION', 'testVERSIONID', 'testAGGREGATIONSET', 'testAGGREGATIONGROUP', 'testREAGGREGATIONSET', 'testREAGGREGATIONGROUP', 1, 1, 'testAGGREGATIONTYPE', 'testAGGREGATIONSCOPE' );\n";
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
    objUnderTest.setAggregation(AggregationTest.testStringGenerator("anotherAGGREGATION", 255));
    assertEquals(AggregationTest.testStringGenerator("anotherAGGREGATION", 255), AGGREGATION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersionid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersionid(AggregationTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(AggregationTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetAggregationset() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setAggregationset(AggregationTest.testStringGenerator("anotherAGGREGATIONSET", 100));
    assertEquals(AggregationTest.testStringGenerator("anotherAGGREGATIONSET", 100), AGGREGATIONSET.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetAggregationgroup() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setAggregationgroup(AggregationTest.testStringGenerator("anotherAGGREGATIONGROUP", 100));
    assertEquals(AggregationTest.testStringGenerator("anotherAGGREGATIONGROUP", 100), AGGREGATIONGROUP.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetReaggregationset() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setReaggregationset(AggregationTest.testStringGenerator("anotherREAGGREGATIONSET", 100));
    assertEquals(AggregationTest.testStringGenerator("anotherREAGGREGATIONSET", 100), REAGGREGATIONSET.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetReaggregationgroup() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setReaggregationgroup(AggregationTest.testStringGenerator("anotherREAGGREGATIONGROUP", 100));
    assertEquals(AggregationTest.testStringGenerator("anotherREAGGREGATIONGROUP", 100), REAGGREGATIONGROUP.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetGrouporder() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setGrouporder(555);
    assertEquals(555, GROUPORDER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetAggregationorder() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setAggregationorder(555);
    assertEquals(555, AGGREGATIONORDER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetAggregationtype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setAggregationtype(AggregationTest.testStringGenerator("anotherAGGREGATIONTYPE", 50));
    assertEquals(AggregationTest.testStringGenerator("anotherAGGREGATIONTYPE", 50), AGGREGATIONTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetAggregationscope() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setAggregationscope(AggregationTest.testStringGenerator("anotherAGGREGATIONSCOPE", 50));
    assertEquals(AggregationTest.testStringGenerator("anotherAGGREGATIONSCOPE", 50), AGGREGATIONSCOPE.get(objUnderTest));
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
    String[] expectedKeys = { "AGGREGATION","VERSIONID"};

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
    objUnderTest = new Aggregation(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  AGGREGATION.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + AGGREGATIONSET.get(objUnderTest)  + ", " + AGGREGATIONGROUP.get(objUnderTest)  + ", " + REAGGREGATIONSET.get(objUnderTest)  + ", " + REAGGREGATIONGROUP.get(objUnderTest)  + ", " + GROUPORDER.get(objUnderTest)  + ", " + AGGREGATIONORDER.get(objUnderTest)  + ", " + AGGREGATIONTYPE.get(objUnderTest)  + ", " + AGGREGATIONSCOPE.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", "  + ", " ;
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
    Aggregation compareObj = new Aggregation(rockFactory ,  "testAGGREGATION",  "testVERSIONID");

    /* Testing first with null primary key value */
    AGGREGATION.set(objUnderTest, null);
  	VERSIONID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    AGGREGATION.set(objUnderTest,  "different");
  	VERSIONID.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    AGGREGATION.set(objUnderTest,  "testAGGREGATION");
  	VERSIONID.set(objUnderTest,  "testVERSIONID");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Aggregation with our current one. If the two
   * Aggregations are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnAggregation() throws Exception {

    /* Creating another Aggregation which will be compared to the tested one */
    Aggregation comparedObj = new Aggregation(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Aggregation with our current one. If the two
   * Aggregations are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAggregation() throws Exception {

    /* Creating another Aggregation which will be compared to the tested one */
    Aggregation comparedObj = new Aggregation(rockFactory ,  "testAGGREGATION",  "testVERSIONID");
    comparedObj.setAggregation( "DifferentAGGREGATION");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Aggregation with our current one. If the two
   * Aggregations are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameAggregation() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Aggregation comparedObj = new Aggregation(rockFactory ,  "testAGGREGATION",  "testVERSIONID");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Aggregation with our current one using null value.
   */
  @Test
  public void testEqualsWithNullAggregation() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Aggregation comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Aggregation was null \n");
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
    assertEquals(Aggregation.class, actualObject.getClass());
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
    Aggregation testAgg = new Aggregation(rockFactory ,  "testAGGREGATION",  "testVERSIONID");
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
   * Testing columnsize retrieving for Aggregationset.
   */
  @Test
  public void testGetAggregationsetColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getAggregationsetColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Aggregationset.
  */
  @Test
  public void testGetAggregationsetDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getAggregationsetDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Aggregationset.
  */
  @Test
  public void testGetAggregationsetSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getAggregationsetSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Aggregationgroup.
   */
  @Test
  public void testGetAggregationgroupColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getAggregationgroupColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Aggregationgroup.
  */
  @Test
  public void testGetAggregationgroupDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getAggregationgroupDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Aggregationgroup.
  */
  @Test
  public void testGetAggregationgroupSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getAggregationgroupSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Reaggregationset.
   */
  @Test
  public void testGetReaggregationsetColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getReaggregationsetColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Reaggregationset.
  */
  @Test
  public void testGetReaggregationsetDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getReaggregationsetDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Reaggregationset.
  */
  @Test
  public void testGetReaggregationsetSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getReaggregationsetSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Reaggregationgroup.
   */
  @Test
  public void testGetReaggregationgroupColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getReaggregationgroupColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Reaggregationgroup.
  */
  @Test
  public void testGetReaggregationgroupDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getReaggregationgroupDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Reaggregationgroup.
  */
  @Test
  public void testGetReaggregationgroupSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getReaggregationgroupSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Grouporder.
   */
  @Test
  public void testGetGrouporderColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getGrouporderColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Grouporder.
  */
  @Test
  public void testGetGrouporderDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getGrouporderDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Grouporder.
  */
  @Test
  public void testGetGrouporderSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getGrouporderSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Aggregationorder.
   */
  @Test
  public void testGetAggregationorderColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getAggregationorderColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Aggregationorder.
  */
  @Test
  public void testGetAggregationorderDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getAggregationorderDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Aggregationorder.
  */
  @Test
  public void testGetAggregationorderSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getAggregationorderSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Aggregationtype.
   */
  @Test
  public void testGetAggregationtypeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getAggregationtypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Aggregationtype.
  */
  @Test
  public void testGetAggregationtypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getAggregationtypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Aggregationtype.
  */
  @Test
  public void testGetAggregationtypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getAggregationtypeSQLType());    
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
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Aggregation object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Aggregation(rockFactory, false);
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
    Aggregation changedOriginal = new Aggregation(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Aggregation(rockFactory, false);
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
    Aggregation changedOriginal = new Aggregation(rockFactory, false);
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