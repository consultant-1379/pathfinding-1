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
 * Test class for Alarmreport. Changes to Alarmreport table are made via
 * this class.
 */
public class AlarmreportTest {

  private static Alarmreport objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field INTERFACEID;
  
  private static Field REPORTID;
  
  private static Field REPORTNAME;
  
  private static Field URL;
  
  private static Field STATUS;
  
  private static Field SIMULTANEOUS;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Alarmreport.class.getDeclaredField("newItem");
		INTERFACEID = Alarmreport.class.getDeclaredField("INTERFACEID");
		REPORTID = Alarmreport.class.getDeclaredField("REPORTID");
		REPORTNAME = Alarmreport.class.getDeclaredField("REPORTNAME");
		URL = Alarmreport.class.getDeclaredField("URL");
		STATUS = Alarmreport.class.getDeclaredField("STATUS");
		SIMULTANEOUS = Alarmreport.class.getDeclaredField("SIMULTANEOUS");
		newItem.setAccessible(true);
		INTERFACEID.setAccessible(true);
		REPORTID.setAccessible(true);
		REPORTNAME.setAccessible(true);
		URL.setAccessible(true);
		STATUS.setAccessible(true);
		SIMULTANEOUS.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Alarmreport ( INTERFACEID VARCHAR(31)  ,REPORTID VARCHAR(31) ,REPORTNAME VARCHAR(31) ,URL VARCHAR(31) ,STATUS VARCHAR(31) ,SIMULTANEOUS INTEGER )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Alarmreport");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Alarmreport VALUES( 'testINTERFACEID'  ,'testREPORTID'  ,'testREPORTNAME'  ,'testURL'  ,'testSTATUS'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Alarmreport(rockFactory ,  "testREPORTID");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Alarmreport");
    objUnderTest = null;
  }
  
  /**
   * Testing Alarmreport constructor variable initialization with null values.
   */
  @Test
  public void testAlarmreportConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Alarmreport(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  INTERFACEID.get(objUnderTest)  + ", " + REPORTID.get(objUnderTest)  + ", " + REPORTNAME.get(objUnderTest)  + ", " + URL.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + SIMULTANEOUS.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Alarmreport constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testAlarmreportConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Alarmreport(rockFactory ,  "testREPORTID");

    /* Asserting that variables are initialized */
    String actual =  INTERFACEID.get(objUnderTest)  + ", " + REPORTID.get(objUnderTest)  + ", " + REPORTNAME.get(objUnderTest)  + ", " + URL.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + SIMULTANEOUS.get(objUnderTest) ;
    String expected =  "testINTERFACEID"  + ", testREPORTID"  + ", testREPORTNAME"  + ", testURL"  + ", testSTATUS"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAlarmreportConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Alarmreport(null ,  "testREPORTID");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Alarmreport constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testAlarmreportConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Alarmreport whereObject = new Alarmreport(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Alarmreport(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  INTERFACEID.get(objUnderTest)  + ", " + REPORTID.get(objUnderTest)  + ", " + REPORTNAME.get(objUnderTest)  + ", " + URL.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + SIMULTANEOUS.get(objUnderTest) ;
    String expected =  "testINTERFACEID"  + ", testREPORTID"  + ", testREPORTNAME"  + ", testURL"  + ", testSTATUS"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAlarmreportConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Alarmreport whereObject = new Alarmreport(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Alarmreport(null, whereObject);
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
    assertEquals("Alarmreport", objUnderTest.getTableName());
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
    Alarmreport whereObject = new Alarmreport(rockFactory);

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
    Alarmreport whereObject = new Alarmreport(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Alarmreport");
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
    INTERFACEID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("INTERFACEID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Alarmreport");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the INTERFACEID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT INTERFACEID FROM Alarmreport");
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
    
    String expected = "<Alarmreport INTERFACEID=\"'testINTERFACEID'\" REPORTID=\"'testREPORTID'\" REPORTNAME=\"'testREPORTNAME'\" URL=\"'testURL'\" STATUS=\"'testSTATUS'\" SIMULTANEOUS=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Alarmreport INTERFACEID=\"'testINTERFACEID'\" REPORTID=\"'testREPORTID'\" REPORTNAME=\"'testREPORTNAME'\" URL=\"'testURL'\" STATUS=\"'testSTATUS'\" SIMULTANEOUS=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Alarmreport>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Alarmreport ( INTERFACEID, REPORTID, REPORTNAME, URL, STATUS, SIMULTANEOUS ) values "
      + "( 'testINTERFACEID', 'testREPORTID', 'testREPORTNAME', 'testURL', 'testSTATUS', 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetInterfaceid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInterfaceid(AlarmreportTest.testStringGenerator("anotherINTERFACEID", 50));
    assertEquals(AlarmreportTest.testStringGenerator("anotherINTERFACEID", 50), INTERFACEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetReportid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setReportid(AlarmreportTest.testStringGenerator("anotherREPORTID", 255));
    assertEquals(AlarmreportTest.testStringGenerator("anotherREPORTID", 255), REPORTID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetReportname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setReportname(AlarmreportTest.testStringGenerator("anotherREPORTNAME", 255));
    assertEquals(AlarmreportTest.testStringGenerator("anotherREPORTNAME", 255), REPORTNAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUrl() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUrl(AlarmreportTest.testStringGenerator("anotherURL", 32000));
    assertEquals(AlarmreportTest.testStringGenerator("anotherURL", 32000), URL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatus() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatus(AlarmreportTest.testStringGenerator("anotherSTATUS", 10));
    assertEquals(AlarmreportTest.testStringGenerator("anotherSTATUS", 10), STATUS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSimultaneous() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSimultaneous(555);
    assertEquals(555, SIMULTANEOUS.get(objUnderTest));
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
    String[] expectedKeys = { "REPORTID"};

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
    objUnderTest = new Alarmreport(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  INTERFACEID.get(objUnderTest)  + ", " + REPORTID.get(objUnderTest)  + ", " + REPORTNAME.get(objUnderTest)  + ", " + URL.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + SIMULTANEOUS.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", 0" ;
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
    Alarmreport compareObj = new Alarmreport(rockFactory ,  "testREPORTID");

    /* Testing first with null primary key value */
    REPORTID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    REPORTID.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    REPORTID.set(objUnderTest,  "testREPORTID");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Alarmreport with our current one. If the two
   * Alarmreports are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnAlarmreport() throws Exception {

    /* Creating another Alarmreport which will be compared to the tested one */
    Alarmreport comparedObj = new Alarmreport(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Alarmreport with our current one. If the two
   * Alarmreports are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAlarmreport() throws Exception {

    /* Creating another Alarmreport which will be compared to the tested one */
    Alarmreport comparedObj = new Alarmreport(rockFactory ,  "testREPORTID");
    comparedObj.setInterfaceid( "DifferentINTERFACEID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Alarmreport with our current one. If the two
   * Alarmreports are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameAlarmreport() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Alarmreport comparedObj = new Alarmreport(rockFactory ,  "testREPORTID");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Alarmreport with our current one using null value.
   */
  @Test
  public void testEqualsWithNullAlarmreport() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Alarmreport comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Alarmreport was null \n");
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
    assertEquals(Alarmreport.class, actualObject.getClass());
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
    Alarmreport testAgg = new Alarmreport(rockFactory ,  "testREPORTID");
    INTERFACEID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Interfaceid.
   */
  @Test
  public void testGetInterfaceidColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getInterfaceidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Interfaceid.
  */
  @Test
  public void testGetInterfaceidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getInterfaceidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Interfaceid.
  */
  @Test
  public void testGetInterfaceidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getInterfaceidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Reportid.
   */
  @Test
  public void testGetReportidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getReportidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Reportid.
  */
  @Test
  public void testGetReportidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getReportidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Reportid.
  */
  @Test
  public void testGetReportidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getReportidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Reportname.
   */
  @Test
  public void testGetReportnameColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getReportnameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Reportname.
  */
  @Test
  public void testGetReportnameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getReportnameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Reportname.
  */
  @Test
  public void testGetReportnameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getReportnameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Url.
   */
  @Test
  public void testGetUrlColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getUrlColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Url.
  */
  @Test
  public void testGetUrlDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUrlDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Url.
  */
  @Test
  public void testGetUrlSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUrlSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Status.
   */
  @Test
  public void testGetStatusColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getStatusColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Status.
  */
  @Test
  public void testGetStatusDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getStatusDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Status.
  */
  @Test
  public void testGetStatusSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getStatusSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Simultaneous.
   */
  @Test
  public void testGetSimultaneousColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getSimultaneousColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Simultaneous.
  */
  @Test
  public void testGetSimultaneousDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSimultaneousDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Simultaneous.
  */
  @Test
  public void testGetSimultaneousSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getSimultaneousSQLType());    
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
   * Testing original Alarmreport object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Alarmreport(rockFactory, false);
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
    Alarmreport changedOriginal = new Alarmreport(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Alarmreport(rockFactory, false);
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
    Alarmreport changedOriginal = new Alarmreport(rockFactory, false);
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