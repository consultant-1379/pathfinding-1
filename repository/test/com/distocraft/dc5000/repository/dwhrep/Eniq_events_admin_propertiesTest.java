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
 * Test class for Eniq_events_admin_properties. Changes to Eniq_events_admin_properties table are made via
 * this class.
 */
public class Eniq_events_admin_propertiesTest {

  private static Eniq_events_admin_properties objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field PARAM_NAME;
  
  private static Field PARAM_VALUE;
  
  private static Field DATE_MODIFIED;
  
  private static Field MODIFIED_BY;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Eniq_events_admin_properties.class.getDeclaredField("newItem");
		PARAM_NAME = Eniq_events_admin_properties.class.getDeclaredField("PARAM_NAME");
		PARAM_VALUE = Eniq_events_admin_properties.class.getDeclaredField("PARAM_VALUE");
		DATE_MODIFIED = Eniq_events_admin_properties.class.getDeclaredField("DATE_MODIFIED");
		MODIFIED_BY = Eniq_events_admin_properties.class.getDeclaredField("MODIFIED_BY");
		newItem.setAccessible(true);
		PARAM_NAME.setAccessible(true);
		PARAM_VALUE.setAccessible(true);
		DATE_MODIFIED.setAccessible(true);
		MODIFIED_BY.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Eniq_events_admin_properties ( PARAM_NAME VARCHAR(31)  ,PARAM_VALUE VARCHAR(31) ,DATE_MODIFIED TIMESTAMP  ,MODIFIED_BY VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Eniq_events_admin_properties");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Eniq_events_admin_properties VALUES( 'testPARAM_NAME'  ,'testPARAM_VALUE'  ,'2000-01-01 00:00:00.0'  ,'testMODIFIED_BY' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Eniq_events_admin_properties(rockFactory ,  "testPARAM_NAME");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Eniq_events_admin_properties");
    objUnderTest = null;
  }
  
  /**
   * Testing Eniq_events_admin_properties constructor variable initialization with null values.
   */
  @Test
  public void testEniq_events_admin_propertiesConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Eniq_events_admin_properties(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  PARAM_NAME.get(objUnderTest)  + ", " + PARAM_VALUE.get(objUnderTest)  + ", " + DATE_MODIFIED.get(objUnderTest)  + ", " + MODIFIED_BY.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Eniq_events_admin_properties constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testEniq_events_admin_propertiesConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Eniq_events_admin_properties(rockFactory ,  "testPARAM_NAME");

    /* Asserting that variables are initialized */
    String actual =  PARAM_NAME.get(objUnderTest)  + ", " + PARAM_VALUE.get(objUnderTest)  + ", " + DATE_MODIFIED.get(objUnderTest)  + ", " + MODIFIED_BY.get(objUnderTest) ;
    String expected =  "testPARAM_NAME"  + ", testPARAM_VALUE"  + ", 2000-01-01 00:00:00.0"  + ", testMODIFIED_BY" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testEniq_events_admin_propertiesConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Eniq_events_admin_properties(null ,  "testPARAM_NAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Eniq_events_admin_properties constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testEniq_events_admin_propertiesConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Eniq_events_admin_properties whereObject = new Eniq_events_admin_properties(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Eniq_events_admin_properties(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  PARAM_NAME.get(objUnderTest)  + ", " + PARAM_VALUE.get(objUnderTest)  + ", " + DATE_MODIFIED.get(objUnderTest)  + ", " + MODIFIED_BY.get(objUnderTest) ;
    String expected =  "testPARAM_NAME"  + ", testPARAM_VALUE"  + ", 2000-01-01 00:00:00.0"  + ", testMODIFIED_BY" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testEniq_events_admin_propertiesConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Eniq_events_admin_properties whereObject = new Eniq_events_admin_properties(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Eniq_events_admin_properties(null, whereObject);
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
    assertEquals("Eniq_events_admin_properties", objUnderTest.getTableName());
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
    Eniq_events_admin_properties whereObject = new Eniq_events_admin_properties(rockFactory);

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
    Eniq_events_admin_properties whereObject = new Eniq_events_admin_properties(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Eniq_events_admin_properties");
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
    PARAM_NAME.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("PARAM_NAME");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Eniq_events_admin_properties");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the PARAM_NAME column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT PARAM_NAME FROM Eniq_events_admin_properties");
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
    
    String expected = "<Eniq_events_admin_properties PARAM_NAME=\"'testPARAM_NAME'\" PARAM_VALUE=\"'testPARAM_VALUE'\" DATE_MODIFIED=\"'2000-01-01 00:00:00.0'\" MODIFIED_BY=\"'testMODIFIED_BY'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Eniq_events_admin_properties PARAM_NAME=\"'testPARAM_NAME'\" PARAM_VALUE=\"'testPARAM_VALUE'\" DATE_MODIFIED=\"'2000-01-01 00:00:00.0'\" MODIFIED_BY=\"'testMODIFIED_BY'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Eniq_events_admin_properties>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Eniq_events_admin_properties ( PARAM_NAME, PARAM_VALUE, DATE_MODIFIED, MODIFIED_BY ) values "
      + "( 'testPARAM_NAME', 'testPARAM_VALUE', '2000-01-01 00:00:00.0', 'testMODIFIED_BY' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetParam_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setParam_name(Eniq_events_admin_propertiesTest.testStringGenerator("anotherPARAM_NAME", 100));
    assertEquals(Eniq_events_admin_propertiesTest.testStringGenerator("anotherPARAM_NAME", 100), PARAM_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetParam_value() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setParam_value(Eniq_events_admin_propertiesTest.testStringGenerator("anotherPARAM_VALUE", 1000));
    assertEquals(Eniq_events_admin_propertiesTest.testStringGenerator("anotherPARAM_VALUE", 1000), PARAM_VALUE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDate_modified() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDate_modified(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), DATE_MODIFIED.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetModified_by() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setModified_by(Eniq_events_admin_propertiesTest.testStringGenerator("anotherMODIFIED_BY", 100));
    assertEquals(Eniq_events_admin_propertiesTest.testStringGenerator("anotherMODIFIED_BY", 100), MODIFIED_BY.get(objUnderTest));
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
    String[] expectedKeys = { "PARAM_NAME"};

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
    objUnderTest = new Eniq_events_admin_properties(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  PARAM_NAME.get(objUnderTest)  + ", " + PARAM_VALUE.get(objUnderTest)  + ", " + DATE_MODIFIED.get(objUnderTest)  + ", " + MODIFIED_BY.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", " + new Timestamp(0)  + ", " ;
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
    Eniq_events_admin_properties compareObj = new Eniq_events_admin_properties(rockFactory ,  "testPARAM_NAME");

    /* Testing first with null primary key value */
    PARAM_NAME.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    PARAM_NAME.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    PARAM_NAME.set(objUnderTest,  "testPARAM_NAME");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Eniq_events_admin_properties with our current one. If the two
   * Eniq_events_admin_propertiess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnEniq_events_admin_properties() throws Exception {

    /* Creating another Eniq_events_admin_properties which will be compared to the tested one */
    Eniq_events_admin_properties comparedObj = new Eniq_events_admin_properties(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Eniq_events_admin_properties with our current one. If the two
   * Eniq_events_admin_propertiess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentEniq_events_admin_properties() throws Exception {

    /* Creating another Eniq_events_admin_properties which will be compared to the tested one */
    Eniq_events_admin_properties comparedObj = new Eniq_events_admin_properties(rockFactory ,  "testPARAM_NAME");
    comparedObj.setParam_name( "DifferentPARAM_NAME");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Eniq_events_admin_properties with our current one. If the two
   * Eniq_events_admin_propertiess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameEniq_events_admin_properties() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Eniq_events_admin_properties comparedObj = new Eniq_events_admin_properties(rockFactory ,  "testPARAM_NAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Eniq_events_admin_properties with our current one using null value.
   */
  @Test
  public void testEqualsWithNullEniq_events_admin_properties() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Eniq_events_admin_properties comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Eniq_events_admin_properties was null \n");
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
    assertEquals(Eniq_events_admin_properties.class, actualObject.getClass());
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
    Eniq_events_admin_properties testAgg = new Eniq_events_admin_properties(rockFactory ,  "testPARAM_NAME");
    PARAM_NAME.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Param_name.
   */
  @Test
  public void testGetParam_nameColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getParam_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Param_name.
  */
  @Test
  public void testGetParam_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getParam_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Param_name.
  */
  @Test
  public void testGetParam_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getParam_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Param_value.
   */
  @Test
  public void testGetParam_valueColumnSize() throws Exception {
    
     assertEquals(1000, objUnderTest.getParam_valueColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Param_value.
  */
  @Test
  public void testGetParam_valueDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getParam_valueDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Param_value.
  */
  @Test
  public void testGetParam_valueSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getParam_valueSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Date_modified.
   */
  @Test
  public void testGetDate_modifiedColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getDate_modifiedColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Date_modified.
  */
  @Test
  public void testGetDate_modifiedDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDate_modifiedDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Date_modified.
  */
  @Test
  public void testGetDate_modifiedSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getDate_modifiedSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Modified_by.
   */
  @Test
  public void testGetModified_byColumnSize() throws Exception {
    
     assertEquals(100, objUnderTest.getModified_byColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Modified_by.
  */
  @Test
  public void testGetModified_byDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getModified_byDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Modified_by.
  */
  @Test
  public void testGetModified_bySQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getModified_bySQLType());    
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
   * Testing original Eniq_events_admin_properties object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Eniq_events_admin_properties(rockFactory, false);
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
    Eniq_events_admin_properties changedOriginal = new Eniq_events_admin_properties(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Eniq_events_admin_properties(rockFactory, false);
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
    Eniq_events_admin_properties changedOriginal = new Eniq_events_admin_properties(rockFactory, false);
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