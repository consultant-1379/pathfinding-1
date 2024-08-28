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
 * Test class for Alarmreportparameter. Changes to Alarmreportparameter table are made via
 * this class.
 */
public class AlarmreportparameterTest {

  private static Alarmreportparameter objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field REPORTID;
  
  private static Field NAME;
  
  private static Field VALUE;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Alarmreportparameter.class.getDeclaredField("newItem");
		REPORTID = Alarmreportparameter.class.getDeclaredField("REPORTID");
		NAME = Alarmreportparameter.class.getDeclaredField("NAME");
		VALUE = Alarmreportparameter.class.getDeclaredField("VALUE");
		newItem.setAccessible(true);
		REPORTID.setAccessible(true);
		NAME.setAccessible(true);
		VALUE.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Alarmreportparameter ( REPORTID VARCHAR(31)  ,NAME VARCHAR(31) ,VALUE VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Alarmreportparameter");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Alarmreportparameter VALUES( 'testREPORTID'  ,'testNAME'  ,'testVALUE' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Alarmreportparameter(rockFactory ,  "testREPORTID",  "testNAME");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Alarmreportparameter");
    objUnderTest = null;
  }
  
  /**
   * Testing Alarmreportparameter constructor variable initialization with null values.
   */
  @Test
  public void testAlarmreportparameterConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Alarmreportparameter(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  REPORTID.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + VALUE.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Alarmreportparameter constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testAlarmreportparameterConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Alarmreportparameter(rockFactory ,  "testREPORTID",  "testNAME");

    /* Asserting that variables are initialized */
    String actual =  REPORTID.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + VALUE.get(objUnderTest) ;
    String expected =  "testREPORTID"  + ", testNAME"  + ", testVALUE" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAlarmreportparameterConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Alarmreportparameter(null ,  "testREPORTID",  "testNAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Alarmreportparameter constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testAlarmreportparameterConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Alarmreportparameter whereObject = new Alarmreportparameter(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Alarmreportparameter(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  REPORTID.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + VALUE.get(objUnderTest) ;
    String expected =  "testREPORTID"  + ", testNAME"  + ", testVALUE" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testAlarmreportparameterConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Alarmreportparameter whereObject = new Alarmreportparameter(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Alarmreportparameter(null, whereObject);
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
    assertEquals("Alarmreportparameter", objUnderTest.getTableName());
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
    Alarmreportparameter whereObject = new Alarmreportparameter(rockFactory);

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
    Alarmreportparameter whereObject = new Alarmreportparameter(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Alarmreportparameter");
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
    REPORTID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("REPORTID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Alarmreportparameter");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the REPORTID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT REPORTID FROM Alarmreportparameter");
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
    
    String expected = "<Alarmreportparameter REPORTID=\"'testREPORTID'\" NAME=\"'testNAME'\" VALUE=\"'testVALUE'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Alarmreportparameter REPORTID=\"'testREPORTID'\" NAME=\"'testNAME'\" VALUE=\"'testVALUE'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Alarmreportparameter>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Alarmreportparameter ( REPORTID, NAME, VALUE ) values "
      + "( 'testREPORTID', 'testNAME', 'testVALUE' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetReportid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setReportid(AlarmreportparameterTest.testStringGenerator("anotherREPORTID", 255));
    assertEquals(AlarmreportparameterTest.testStringGenerator("anotherREPORTID", 255), REPORTID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetName() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setName(AlarmreportparameterTest.testStringGenerator("anotherNAME", 255));
    assertEquals(AlarmreportparameterTest.testStringGenerator("anotherNAME", 255), NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetValue() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setValue(AlarmreportparameterTest.testStringGenerator("anotherVALUE", 255));
    assertEquals(AlarmreportparameterTest.testStringGenerator("anotherVALUE", 255), VALUE.get(objUnderTest));
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
    String[] expectedKeys = { "REPORTID","NAME"};

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
    objUnderTest = new Alarmreportparameter(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  REPORTID.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + VALUE.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", " ;
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
    Alarmreportparameter compareObj = new Alarmreportparameter(rockFactory ,  "testREPORTID",  "testNAME");

    /* Testing first with null primary key value */
    REPORTID.set(objUnderTest, null);
  	NAME.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    REPORTID.set(objUnderTest,  "different");
  	NAME.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    REPORTID.set(objUnderTest,  "testREPORTID");
  	NAME.set(objUnderTest,  "testNAME");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Alarmreportparameter with our current one. If the two
   * Alarmreportparameters are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnAlarmreportparameter() throws Exception {

    /* Creating another Alarmreportparameter which will be compared to the tested one */
    Alarmreportparameter comparedObj = new Alarmreportparameter(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Alarmreportparameter with our current one. If the two
   * Alarmreportparameters are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentAlarmreportparameter() throws Exception {

    /* Creating another Alarmreportparameter which will be compared to the tested one */
    Alarmreportparameter comparedObj = new Alarmreportparameter(rockFactory ,  "testREPORTID",  "testNAME");
    comparedObj.setReportid( "DifferentREPORTID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Alarmreportparameter with our current one. If the two
   * Alarmreportparameters are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameAlarmreportparameter() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Alarmreportparameter comparedObj = new Alarmreportparameter(rockFactory ,  "testREPORTID",  "testNAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Alarmreportparameter with our current one using null value.
   */
  @Test
  public void testEqualsWithNullAlarmreportparameter() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Alarmreportparameter comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Alarmreportparameter was null \n");
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
    assertEquals(Alarmreportparameter.class, actualObject.getClass());
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
    Alarmreportparameter testAgg = new Alarmreportparameter(rockFactory ,  "testREPORTID",  "testNAME");
    REPORTID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
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
   * Testing columnsize retrieving for Name.
   */
  @Test
  public void testGetNameColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getNameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Name.
  */
  @Test
  public void testGetNameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getNameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Name.
  */
  @Test
  public void testGetNameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getNameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Value.
   */
  @Test
  public void testGetValueColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getValueColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Value.
  */
  @Test
  public void testGetValueDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getValueDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Value.
  */
  @Test
  public void testGetValueSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getValueSQLType());    
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
   * Testing original Alarmreportparameter object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Alarmreportparameter(rockFactory, false);
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
    Alarmreportparameter changedOriginal = new Alarmreportparameter(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Alarmreportparameter(rockFactory, false);
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
    Alarmreportparameter changedOriginal = new Alarmreportparameter(rockFactory, false);
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