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
 * Test class for Dataformat. Changes to Dataformat table are made via
 * this class.
 */
public class DataformatTest {

  private static Dataformat objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field DATAFORMATID;
  
  private static Field TYPEID;
  
  private static Field VERSIONID;
  
  private static Field OBJECTTYPE;
  
  private static Field FOLDERNAME;
  
  private static Field DATAFORMATTYPE;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Dataformat.class.getDeclaredField("newItem");
		DATAFORMATID = Dataformat.class.getDeclaredField("DATAFORMATID");
		TYPEID = Dataformat.class.getDeclaredField("TYPEID");
		VERSIONID = Dataformat.class.getDeclaredField("VERSIONID");
		OBJECTTYPE = Dataformat.class.getDeclaredField("OBJECTTYPE");
		FOLDERNAME = Dataformat.class.getDeclaredField("FOLDERNAME");
		DATAFORMATTYPE = Dataformat.class.getDeclaredField("DATAFORMATTYPE");
		newItem.setAccessible(true);
		DATAFORMATID.setAccessible(true);
		TYPEID.setAccessible(true);
		VERSIONID.setAccessible(true);
		OBJECTTYPE.setAccessible(true);
		FOLDERNAME.setAccessible(true);
		DATAFORMATTYPE.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Dataformat ( DATAFORMATID VARCHAR(31)  ,TYPEID VARCHAR(31) ,VERSIONID VARCHAR(31) ,OBJECTTYPE VARCHAR(31) ,FOLDERNAME VARCHAR(31) ,DATAFORMATTYPE VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Dataformat");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Dataformat VALUES( 'testDATAFORMATID'  ,'testTYPEID'  ,'testVERSIONID'  ,'testOBJECTTYPE'  ,'testFOLDERNAME'  ,'testDATAFORMATTYPE' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Dataformat(rockFactory ,  "testDATAFORMATID");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Dataformat");
    objUnderTest = null;
  }
  
  /**
   * Testing Dataformat constructor variable initialization with null values.
   */
  @Test
  public void testDataformatConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Dataformat(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  DATAFORMATID.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + FOLDERNAME.get(objUnderTest)  + ", " + DATAFORMATTYPE.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Dataformat constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDataformatConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Dataformat(rockFactory ,  "testDATAFORMATID");

    /* Asserting that variables are initialized */
    String actual =  DATAFORMATID.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + FOLDERNAME.get(objUnderTest)  + ", " + DATAFORMATTYPE.get(objUnderTest) ;
    String expected =  "testDATAFORMATID"  + ", testTYPEID"  + ", testVERSIONID"  + ", testOBJECTTYPE"  + ", testFOLDERNAME"  + ", testDATAFORMATTYPE" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDataformatConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Dataformat(null ,  "testDATAFORMATID");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Dataformat constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDataformatConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Dataformat whereObject = new Dataformat(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Dataformat(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  DATAFORMATID.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + FOLDERNAME.get(objUnderTest)  + ", " + DATAFORMATTYPE.get(objUnderTest) ;
    String expected =  "testDATAFORMATID"  + ", testTYPEID"  + ", testVERSIONID"  + ", testOBJECTTYPE"  + ", testFOLDERNAME"  + ", testDATAFORMATTYPE" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDataformatConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Dataformat whereObject = new Dataformat(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Dataformat(null, whereObject);
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
    assertEquals("Dataformat", objUnderTest.getTableName());
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
    Dataformat whereObject = new Dataformat(rockFactory);

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
    Dataformat whereObject = new Dataformat(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dataformat");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dataformat");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the DATAFORMATID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT DATAFORMATID FROM Dataformat");
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
    
    String expected = "<Dataformat DATAFORMATID=\"'testDATAFORMATID'\" TYPEID=\"'testTYPEID'\" VERSIONID=\"'testVERSIONID'\" OBJECTTYPE=\"'testOBJECTTYPE'\" FOLDERNAME=\"'testFOLDERNAME'\" DATAFORMATTYPE=\"'testDATAFORMATTYPE'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Dataformat DATAFORMATID=\"'testDATAFORMATID'\" TYPEID=\"'testTYPEID'\" VERSIONID=\"'testVERSIONID'\" OBJECTTYPE=\"'testOBJECTTYPE'\" FOLDERNAME=\"'testFOLDERNAME'\" DATAFORMATTYPE=\"'testDATAFORMATTYPE'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Dataformat>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Dataformat ( DATAFORMATID, TYPEID, VERSIONID, OBJECTTYPE, FOLDERNAME, DATAFORMATTYPE ) values "
      + "( 'testDATAFORMATID', 'testTYPEID', 'testVERSIONID', 'testOBJECTTYPE', 'testFOLDERNAME', 'testDATAFORMATTYPE' );\n";
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
    objUnderTest.setDataformatid(DataformatTest.testStringGenerator("anotherDATAFORMATID", 100));
    assertEquals(DataformatTest.testStringGenerator("anotherDATAFORMATID", 100), DATAFORMATID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTypeid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTypeid(DataformatTest.testStringGenerator("anotherTYPEID", 255));
    assertEquals(DataformatTest.testStringGenerator("anotherTYPEID", 255), TYPEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersionid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersionid(DataformatTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(DataformatTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjecttype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjecttype(DataformatTest.testStringGenerator("anotherOBJECTTYPE", 255));
    assertEquals(DataformatTest.testStringGenerator("anotherOBJECTTYPE", 255), OBJECTTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFoldername() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFoldername(DataformatTest.testStringGenerator("anotherFOLDERNAME", 128));
    assertEquals(DataformatTest.testStringGenerator("anotherFOLDERNAME", 128), FOLDERNAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataformattype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataformattype(DataformatTest.testStringGenerator("anotherDATAFORMATTYPE", 50));
    assertEquals(DataformatTest.testStringGenerator("anotherDATAFORMATTYPE", 50), DATAFORMATTYPE.get(objUnderTest));
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
    String[] expectedKeys = { "DATAFORMATID"};

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
    objUnderTest = new Dataformat(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  DATAFORMATID.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + FOLDERNAME.get(objUnderTest)  + ", " + DATAFORMATTYPE.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", " ;
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
    Dataformat compareObj = new Dataformat(rockFactory ,  "testDATAFORMATID");

    /* Testing first with null primary key value */
    DATAFORMATID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    DATAFORMATID.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    DATAFORMATID.set(objUnderTest,  "testDATAFORMATID");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Dataformat with our current one. If the two
   * Dataformats are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnDataformat() throws Exception {

    /* Creating another Dataformat which will be compared to the tested one */
    Dataformat comparedObj = new Dataformat(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dataformat with our current one. If the two
   * Dataformats are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentDataformat() throws Exception {

    /* Creating another Dataformat which will be compared to the tested one */
    Dataformat comparedObj = new Dataformat(rockFactory ,  "testDATAFORMATID");
    comparedObj.setDataformatid( "DifferentDATAFORMATID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dataformat with our current one. If the two
   * Dataformats are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameDataformat() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Dataformat comparedObj = new Dataformat(rockFactory ,  "testDATAFORMATID");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dataformat with our current one using null value.
   */
  @Test
  public void testEqualsWithNullDataformat() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Dataformat comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Dataformat was null \n");
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
    assertEquals(Dataformat.class, actualObject.getClass());
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
    Dataformat testAgg = new Dataformat(rockFactory ,  "testDATAFORMATID");
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
   * Testing columnsize retrieving for Typeid.
   */
  @Test
  public void testGetTypeidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getTypeidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Typeid.
  */
  @Test
  public void testGetTypeidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTypeidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Typeid.
  */
  @Test
  public void testGetTypeidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTypeidSQLType());    
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
   * Testing columnsize retrieving for Objecttype.
   */
  @Test
  public void testGetObjecttypeColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getObjecttypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objecttype.
  */
  @Test
  public void testGetObjecttypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjecttypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objecttype.
  */
  @Test
  public void testGetObjecttypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjecttypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Foldername.
   */
  @Test
  public void testGetFoldernameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getFoldernameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Foldername.
  */
  @Test
  public void testGetFoldernameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFoldernameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Foldername.
  */
  @Test
  public void testGetFoldernameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getFoldernameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Dataformattype.
   */
  @Test
  public void testGetDataformattypeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getDataformattypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dataformattype.
  */
  @Test
  public void testGetDataformattypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDataformattypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dataformattype.
  */
  @Test
  public void testGetDataformattypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDataformattypeSQLType());    
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
   * Testing original Dataformat object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Dataformat(rockFactory, false);
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
    Dataformat changedOriginal = new Dataformat(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Dataformat(rockFactory, false);
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
    Dataformat changedOriginal = new Dataformat(rockFactory, false);
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