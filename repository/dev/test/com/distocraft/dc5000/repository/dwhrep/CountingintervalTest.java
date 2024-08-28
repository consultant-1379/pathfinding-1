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
 * Test class for Countinginterval. Changes to Countinginterval table are made via
 * this class.
 */
public class CountingintervalTest {

  private static Countinginterval objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field INTERVAL;
  
  private static Field INTERVALNAME;
  
  private static Field INTERVALDESCRIPTION;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Countinginterval.class.getDeclaredField("newItem");
		INTERVAL = Countinginterval.class.getDeclaredField("INTERVAL");
		INTERVALNAME = Countinginterval.class.getDeclaredField("INTERVALNAME");
		INTERVALDESCRIPTION = Countinginterval.class.getDeclaredField("INTERVALDESCRIPTION");
		newItem.setAccessible(true);
		INTERVAL.setAccessible(true);
		INTERVALNAME.setAccessible(true);
		INTERVALDESCRIPTION.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Countinginterval ( INTERVAL INTEGER  ,INTERVALNAME VARCHAR(31) ,INTERVALDESCRIPTION VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Countinginterval");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Countinginterval VALUES( 1  ,'testINTERVALNAME'  ,'testINTERVALDESCRIPTION' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Countinginterval(rockFactory ,  1 );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Countinginterval");
    objUnderTest = null;
  }
  
  /**
   * Testing Countinginterval constructor variable initialization with null values.
   */
  @Test
  public void testCountingintervalConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Countinginterval(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  INTERVAL.get(objUnderTest)  + ", " + INTERVALNAME.get(objUnderTest)  + ", " + INTERVALDESCRIPTION.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Countinginterval constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testCountingintervalConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Countinginterval(rockFactory ,  1 );

    /* Asserting that variables are initialized */
    String actual =  INTERVAL.get(objUnderTest)  + ", " + INTERVALNAME.get(objUnderTest)  + ", " + INTERVALDESCRIPTION.get(objUnderTest) ;
    String expected =  "1"  + ", testINTERVALNAME"  + ", testINTERVALDESCRIPTION" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testCountingintervalConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Countinginterval(null ,  1 );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Countinginterval constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testCountingintervalConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Countinginterval whereObject = new Countinginterval(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Countinginterval(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  INTERVAL.get(objUnderTest)  + ", " + INTERVALNAME.get(objUnderTest)  + ", " + INTERVALDESCRIPTION.get(objUnderTest) ;
    String expected =  "1"  + ", testINTERVALNAME"  + ", testINTERVALDESCRIPTION" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testCountingintervalConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Countinginterval whereObject = new Countinginterval(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Countinginterval(null, whereObject);
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
    assertEquals("Countinginterval", objUnderTest.getTableName());
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
    Countinginterval whereObject = new Countinginterval(rockFactory);

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
    Countinginterval whereObject = new Countinginterval(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Countinginterval");
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
    INTERVAL.set(objUnderTest, 1);
    HashSet testSet = new HashSet();
    testSet.add("INTERVAL");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Countinginterval");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the INTERVAL column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT INTERVAL FROM Countinginterval");
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
    
    String expected = "<Countinginterval INTERVAL=\"1\" INTERVALNAME=\"'testINTERVALNAME'\" INTERVALDESCRIPTION=\"'testINTERVALDESCRIPTION'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Countinginterval INTERVAL=\"1\" INTERVALNAME=\"'testINTERVALNAME'\" INTERVALDESCRIPTION=\"'testINTERVALDESCRIPTION'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Countinginterval>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Countinginterval ( INTERVAL, INTERVALNAME, INTERVALDESCRIPTION ) values "
      + "( 1, 'testINTERVALNAME', 'testINTERVALDESCRIPTION' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetInterval() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInterval(555);
    assertEquals(555, INTERVAL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIntervalname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIntervalname(CountingintervalTest.testStringGenerator("anotherINTERVALNAME", 16));
    assertEquals(CountingintervalTest.testStringGenerator("anotherINTERVALNAME", 16), INTERVALNAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIntervaldescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIntervaldescription(CountingintervalTest.testStringGenerator("anotherINTERVALDESCRIPTION", 255));
    assertEquals(CountingintervalTest.testStringGenerator("anotherINTERVALDESCRIPTION", 255), INTERVALDESCRIPTION.get(objUnderTest));
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
    String[] expectedKeys = { "INTERVAL"};

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
    objUnderTest = new Countinginterval(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  INTERVAL.get(objUnderTest)  + ", " + INTERVALNAME.get(objUnderTest)  + ", " + INTERVALDESCRIPTION.get(objUnderTest) ;
    String expected =  "0"  + ", "  + ", " ;
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
    Countinginterval compareObj = new Countinginterval(rockFactory ,  1 );

    /* Testing first with null primary key value */
    INTERVAL.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    INTERVAL.set(objUnderTest,  7 );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    INTERVAL.set(objUnderTest,  1 );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Countinginterval with our current one. If the two
   * Countingintervals are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnCountinginterval() throws Exception {

    /* Creating another Countinginterval which will be compared to the tested one */
    Countinginterval comparedObj = new Countinginterval(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Countinginterval with our current one. If the two
   * Countingintervals are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentCountinginterval() throws Exception {

    /* Creating another Countinginterval which will be compared to the tested one */
    Countinginterval comparedObj = new Countinginterval(rockFactory ,  1 );
    comparedObj.setInterval( 7 );

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Countinginterval with our current one. If the two
   * Countingintervals are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameCountinginterval() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Countinginterval comparedObj = new Countinginterval(rockFactory ,  1 );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Countinginterval with our current one using null value.
   */
  @Test
  public void testEqualsWithNullCountinginterval() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Countinginterval comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Countinginterval was null \n");
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
    assertEquals(Countinginterval.class, actualObject.getClass());
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
    Countinginterval testAgg = new Countinginterval(rockFactory ,  1 );
    INTERVAL.set(objUnderTest, 7);

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Interval.
   */
  @Test
  public void testGetIntervalColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getIntervalColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Interval.
  */
  @Test
  public void testGetIntervalDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIntervalDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Interval.
  */
  @Test
  public void testGetIntervalSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getIntervalSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Intervalname.
   */
  @Test
  public void testGetIntervalnameColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getIntervalnameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Intervalname.
  */
  @Test
  public void testGetIntervalnameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIntervalnameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Intervalname.
  */
  @Test
  public void testGetIntervalnameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getIntervalnameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Intervaldescription.
   */
  @Test
  public void testGetIntervaldescriptionColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getIntervaldescriptionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Intervaldescription.
  */
  @Test
  public void testGetIntervaldescriptionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIntervaldescriptionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Intervaldescription.
  */
  @Test
  public void testGetIntervaldescriptionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getIntervaldescriptionSQLType());    
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
   * Testing original Countinginterval object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Countinginterval(rockFactory, false);
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
    Countinginterval changedOriginal = new Countinginterval(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Countinginterval(rockFactory, false);
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
    Countinginterval changedOriginal = new Countinginterval(rockFactory, false);
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