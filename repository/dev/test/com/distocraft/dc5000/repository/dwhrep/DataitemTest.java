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
 * Test class for Dataitem. Changes to Dataitem table are made via
 * this class.
 */
public class DataitemTest {

  private static Dataitem objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field DATAFORMATID;
  
  private static Field DATANAME;
  
  private static Field COLNUMBER;
  
  private static Field DATAID;
  
  private static Field PROCESS_INSTRUCTION;
  
  private static Field DATATYPE;
  
  private static Field DATASIZE;
  
  private static Field DATASCALE;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Dataitem.class.getDeclaredField("newItem");
		DATAFORMATID = Dataitem.class.getDeclaredField("DATAFORMATID");
		DATANAME = Dataitem.class.getDeclaredField("DATANAME");
		COLNUMBER = Dataitem.class.getDeclaredField("COLNUMBER");
		DATAID = Dataitem.class.getDeclaredField("DATAID");
		PROCESS_INSTRUCTION = Dataitem.class.getDeclaredField("PROCESS_INSTRUCTION");
		DATATYPE = Dataitem.class.getDeclaredField("DATATYPE");
		DATASIZE = Dataitem.class.getDeclaredField("DATASIZE");
		DATASCALE = Dataitem.class.getDeclaredField("DATASCALE");
		newItem.setAccessible(true);
		DATAFORMATID.setAccessible(true);
		DATANAME.setAccessible(true);
		COLNUMBER.setAccessible(true);
		DATAID.setAccessible(true);
		PROCESS_INSTRUCTION.setAccessible(true);
		DATATYPE.setAccessible(true);
		DATASIZE.setAccessible(true);
		DATASCALE.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Dataitem ( DATAFORMATID VARCHAR(31)  ,DATANAME VARCHAR(31) ,COLNUMBER BIGINT  ,DATAID VARCHAR(31) ,PROCESS_INSTRUCTION VARCHAR(31) ,DATATYPE VARCHAR(31) ,DATASIZE INTEGER  ,DATASCALE INTEGER )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Dataitem");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Dataitem VALUES( 'testDATAFORMATID'  ,'testDATANAME'  ,1  ,'testDATAID'  ,'testPROCESS_INSTRUCTION'  ,'testDATATYPE'  ,1  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Dataitem(rockFactory ,  "testDATAFORMATID",  "testDATANAME");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Dataitem");
    objUnderTest = null;
  }
  
  /**
   * Testing Dataitem constructor variable initialization with null values.
   */
  @Test
  public void testDataitemConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Dataitem(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  DATAFORMATID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATAID.get(objUnderTest)  + ", " + PROCESS_INSTRUCTION.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Dataitem constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDataitemConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Dataitem(rockFactory ,  "testDATAFORMATID",  "testDATANAME");

    /* Asserting that variables are initialized */
    String actual =  DATAFORMATID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATAID.get(objUnderTest)  + ", " + PROCESS_INSTRUCTION.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest) ;
    String expected =  "testDATAFORMATID"  + ", testDATANAME"  + ", 1"  + ", testDATAID"  + ", testPROCESS_INSTRUCTION"  + ", testDATATYPE"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDataitemConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Dataitem(null ,  "testDATAFORMATID",  "testDATANAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Dataitem constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDataitemConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Dataitem whereObject = new Dataitem(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Dataitem(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  DATAFORMATID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATAID.get(objUnderTest)  + ", " + PROCESS_INSTRUCTION.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest) ;
    String expected =  "testDATAFORMATID"  + ", testDATANAME"  + ", 1"  + ", testDATAID"  + ", testPROCESS_INSTRUCTION"  + ", testDATATYPE"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDataitemConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Dataitem whereObject = new Dataitem(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Dataitem(null, whereObject);
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
    assertEquals("Dataitem", objUnderTest.getTableName());
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
    Dataitem whereObject = new Dataitem(rockFactory);

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
    Dataitem whereObject = new Dataitem(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dataitem");
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
    DATAFORMATID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("DATAFORMATID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dataitem");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the DATAFORMATID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT DATAFORMATID FROM Dataitem");
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
    
    String expected = "<Dataitem DATAFORMATID=\"'testDATAFORMATID'\" DATANAME=\"'testDATANAME'\" COLNUMBER=\"1\" DATAID=\"'testDATAID'\" PROCESS_INSTRUCTION=\"'testPROCESS_INSTRUCTION'\" DATATYPE=\"'testDATATYPE'\" DATASIZE=\"1\" DATASCALE=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Dataitem DATAFORMATID=\"'testDATAFORMATID'\" DATANAME=\"'testDATANAME'\" COLNUMBER=\"1\" DATAID=\"'testDATAID'\" PROCESS_INSTRUCTION=\"'testPROCESS_INSTRUCTION'\" DATATYPE=\"'testDATATYPE'\" DATASIZE=\"1\" DATASCALE=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Dataitem>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Dataitem ( DATAFORMATID, DATANAME, COLNUMBER, DATAID, PROCESS_INSTRUCTION, DATATYPE, DATASIZE, DATASCALE ) values "
      + "( 'testDATAFORMATID', 'testDATANAME', 1, 'testDATAID', 'testPROCESS_INSTRUCTION', 'testDATATYPE', 1, 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataformatid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataformatid(DataitemTest.testStringGenerator("anotherDATAFORMATID", 100));
    assertEquals(DataitemTest.testStringGenerator("anotherDATAFORMATID", 100), DATAFORMATID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataname(DataitemTest.testStringGenerator("anotherDATANAME", 128));
    assertEquals(DataitemTest.testStringGenerator("anotherDATANAME", 128), DATANAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColnumber() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColnumber(555L);
    assertEquals(555L, COLNUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataid(DataitemTest.testStringGenerator("anotherDATAID", 255));
    assertEquals(DataitemTest.testStringGenerator("anotherDATAID", 255), DATAID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetProcess_instruction() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setProcess_instruction(DataitemTest.testStringGenerator("anotherPROCESS_INSTRUCTION", 128));
    assertEquals(DataitemTest.testStringGenerator("anotherPROCESS_INSTRUCTION", 128), PROCESS_INSTRUCTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDatatype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDatatype(DataitemTest.testStringGenerator("anotherDATATYPE", 50));
    assertEquals(DataitemTest.testStringGenerator("anotherDATATYPE", 50), DATATYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDatasize() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDatasize(555);
    assertEquals(555, DATASIZE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDatascale() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDatascale(555);
    assertEquals(555, DATASCALE.get(objUnderTest));
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
    String[] expectedKeys = { "DATAFORMATID","DATANAME"};

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
    objUnderTest = new Dataitem(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  DATAFORMATID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATAID.get(objUnderTest)  + ", " + PROCESS_INSTRUCTION.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", 0"  + ", "  + ", "  + ", "  + ", 0"  + ", 0" ;
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
    Dataitem compareObj = new Dataitem(rockFactory ,  "testDATAFORMATID",  "testDATANAME");

    /* Testing first with null primary key value */
    DATAFORMATID.set(objUnderTest, null);
  	DATANAME.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    DATAFORMATID.set(objUnderTest,  "different");
  	DATANAME.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    DATAFORMATID.set(objUnderTest,  "testDATAFORMATID");
  	DATANAME.set(objUnderTest,  "testDATANAME");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Dataitem with our current one. If the two
   * Dataitems are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnDataitem() throws Exception {

    /* Creating another Dataitem which will be compared to the tested one */
    Dataitem comparedObj = new Dataitem(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dataitem with our current one. If the two
   * Dataitems are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentDataitem() throws Exception {

    /* Creating another Dataitem which will be compared to the tested one */
    Dataitem comparedObj = new Dataitem(rockFactory ,  "testDATAFORMATID",  "testDATANAME");
    comparedObj.setDataformatid( "DifferentDATAFORMATID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dataitem with our current one. If the two
   * Dataitems are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameDataitem() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Dataitem comparedObj = new Dataitem(rockFactory ,  "testDATAFORMATID",  "testDATANAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dataitem with our current one using null value.
   */
  @Test
  public void testEqualsWithNullDataitem() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Dataitem comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Dataitem was null \n");
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
    assertEquals(Dataitem.class, actualObject.getClass());
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
    Dataitem testAgg = new Dataitem(rockFactory ,  "testDATAFORMATID",  "testDATANAME");
    DATAFORMATID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Dataformatid.
   */
  @Test
  public void testGetDataformatidColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getDataformatidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dataformatid.
  */
  @Test
  public void testGetDataformatidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDataformatidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dataformatid.
  */
  @Test
  public void testGetDataformatidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDataformatidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Dataname.
   */
  @Test
  public void testGetDatanameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getDatanameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dataname.
  */
  @Test
  public void testGetDatanameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDatanameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dataname.
  */
  @Test
  public void testGetDatanameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDatanameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Colnumber.
   */
  @Test
  public void testGetColnumberColumnSize() throws Exception {
    
     assertEquals(9, objUnderTest.getColnumberColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Colnumber.
  */
  @Test
  public void testGetColnumberDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColnumberDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Colnumber.
  */
  @Test
  public void testGetColnumberSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getColnumberSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Dataid.
   */
  @Test
  public void testGetDataidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getDataidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dataid.
  */
  @Test
  public void testGetDataidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDataidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dataid.
  */
  @Test
  public void testGetDataidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDataidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Process_instruction.
   */
  @Test
  public void testGetProcess_instructionColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getProcess_instructionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Process_instruction.
  */
  @Test
  public void testGetProcess_instructionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getProcess_instructionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Process_instruction.
  */
  @Test
  public void testGetProcess_instructionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getProcess_instructionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Datatype.
   */
  @Test
  public void testGetDatatypeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getDatatypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Datatype.
  */
  @Test
  public void testGetDatatypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDatatypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Datatype.
  */
  @Test
  public void testGetDatatypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDatatypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Datasize.
   */
  @Test
  public void testGetDatasizeColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getDatasizeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Datasize.
  */
  @Test
  public void testGetDatasizeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDatasizeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Datasize.
  */
  @Test
  public void testGetDatasizeSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getDatasizeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Datascale.
   */
  @Test
  public void testGetDatascaleColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getDatascaleColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Datascale.
  */
  @Test
  public void testGetDatascaleDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDatascaleDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Datascale.
  */
  @Test
  public void testGetDatascaleSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getDatascaleSQLType());    
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
   * Testing original Dataitem object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Dataitem(rockFactory, false);
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
    Dataitem changedOriginal = new Dataitem(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Dataitem(rockFactory, false);
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
    Dataitem changedOriginal = new Dataitem(rockFactory, false);
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