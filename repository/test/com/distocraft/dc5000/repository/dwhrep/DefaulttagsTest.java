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
 * Test class for Defaulttags. Changes to Defaulttags table are made via
 * this class.
 */
public class DefaulttagsTest {

  private static Defaulttags objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field TAGID;
  
  private static Field DATAFORMATID;
  
  private static Field DESCRIPTION;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Defaulttags.class.getDeclaredField("newItem");
		TAGID = Defaulttags.class.getDeclaredField("TAGID");
		DATAFORMATID = Defaulttags.class.getDeclaredField("DATAFORMATID");
		DESCRIPTION = Defaulttags.class.getDeclaredField("DESCRIPTION");
		newItem.setAccessible(true);
		TAGID.setAccessible(true);
		DATAFORMATID.setAccessible(true);
		DESCRIPTION.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Defaulttags ( TAGID VARCHAR(31)  ,DATAFORMATID VARCHAR(31) ,DESCRIPTION VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Defaulttags");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Defaulttags VALUES( 'testTAGID'  ,'testDATAFORMATID'  ,'testDESCRIPTION' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Defaulttags(rockFactory ,  "testTAGID",  "testDATAFORMATID");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Defaulttags");
    objUnderTest = null;
  }
  
  /**
   * Testing Defaulttags constructor variable initialization with null values.
   */
  @Test
  public void testDefaulttagsConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Defaulttags(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  TAGID.get(objUnderTest)  + ", " + DATAFORMATID.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Defaulttags constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDefaulttagsConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Defaulttags(rockFactory ,  "testTAGID",  "testDATAFORMATID");

    /* Asserting that variables are initialized */
    String actual =  TAGID.get(objUnderTest)  + ", " + DATAFORMATID.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest) ;
    String expected =  "testTAGID"  + ", testDATAFORMATID"  + ", testDESCRIPTION" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDefaulttagsConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Defaulttags(null ,  "testTAGID",  "testDATAFORMATID");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Defaulttags constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDefaulttagsConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Defaulttags whereObject = new Defaulttags(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Defaulttags(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  TAGID.get(objUnderTest)  + ", " + DATAFORMATID.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest) ;
    String expected =  "testTAGID"  + ", testDATAFORMATID"  + ", testDESCRIPTION" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDefaulttagsConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Defaulttags whereObject = new Defaulttags(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Defaulttags(null, whereObject);
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
    assertEquals("Defaulttags", objUnderTest.getTableName());
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
    Defaulttags whereObject = new Defaulttags(rockFactory);

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
    Defaulttags whereObject = new Defaulttags(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Defaulttags");
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
    TAGID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("TAGID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Defaulttags");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the TAGID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT TAGID FROM Defaulttags");
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
    
    String expected = "<Defaulttags TAGID=\"'testTAGID'\" DATAFORMATID=\"'testDATAFORMATID'\" DESCRIPTION=\"'testDESCRIPTION'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Defaulttags TAGID=\"'testTAGID'\" DATAFORMATID=\"'testDATAFORMATID'\" DESCRIPTION=\"'testDESCRIPTION'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Defaulttags>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Defaulttags ( TAGID, DATAFORMATID, DESCRIPTION ) values "
      + "( 'testTAGID', 'testDATAFORMATID', 'testDESCRIPTION' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTagid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTagid(DefaulttagsTest.testStringGenerator("anotherTAGID", 128));
    assertEquals(DefaulttagsTest.testStringGenerator("anotherTAGID", 128), TAGID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataformatid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataformatid(DefaulttagsTest.testStringGenerator("anotherDATAFORMATID", 100));
    assertEquals(DefaulttagsTest.testStringGenerator("anotherDATAFORMATID", 100), DATAFORMATID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(DefaulttagsTest.testStringGenerator("anotherDESCRIPTION", 200));
    assertEquals(DefaulttagsTest.testStringGenerator("anotherDESCRIPTION", 200), DESCRIPTION.get(objUnderTest));
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
    String[] expectedKeys = { "TAGID","DATAFORMATID"};

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
    objUnderTest = new Defaulttags(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  TAGID.get(objUnderTest)  + ", " + DATAFORMATID.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest) ;
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
    Defaulttags compareObj = new Defaulttags(rockFactory ,  "testTAGID",  "testDATAFORMATID");

    /* Testing first with null primary key value */
    TAGID.set(objUnderTest, null);
  	DATAFORMATID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    TAGID.set(objUnderTest,  "different");
  	DATAFORMATID.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    TAGID.set(objUnderTest,  "testTAGID");
  	DATAFORMATID.set(objUnderTest,  "testDATAFORMATID");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Defaulttags with our current one. If the two
   * Defaulttagss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnDefaulttags() throws Exception {

    /* Creating another Defaulttags which will be compared to the tested one */
    Defaulttags comparedObj = new Defaulttags(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Defaulttags with our current one. If the two
   * Defaulttagss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentDefaulttags() throws Exception {

    /* Creating another Defaulttags which will be compared to the tested one */
    Defaulttags comparedObj = new Defaulttags(rockFactory ,  "testTAGID",  "testDATAFORMATID");
    comparedObj.setTagid( "DifferentTAGID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Defaulttags with our current one. If the two
   * Defaulttagss are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameDefaulttags() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Defaulttags comparedObj = new Defaulttags(rockFactory ,  "testTAGID",  "testDATAFORMATID");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Defaulttags with our current one using null value.
   */
  @Test
  public void testEqualsWithNullDefaulttags() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Defaulttags comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Defaulttags was null \n");
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
    assertEquals(Defaulttags.class, actualObject.getClass());
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
    Defaulttags testAgg = new Defaulttags(rockFactory ,  "testTAGID",  "testDATAFORMATID");
    TAGID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Tagid.
   */
  @Test
  public void testGetTagidColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getTagidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Tagid.
  */
  @Test
  public void testGetTagidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTagidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Tagid.
  */
  @Test
  public void testGetTagidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTagidSQLType());    
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
   * Testing columnsize retrieving for Description.
   */
  @Test
  public void testGetDescriptionColumnSize() throws Exception {
    
     assertEquals(200, objUnderTest.getDescriptionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Description.
  */
  @Test
  public void testGetDescriptionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDescriptionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Description.
  */
  @Test
  public void testGetDescriptionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDescriptionSQLType());    
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
   * Testing original Defaulttags object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Defaulttags(rockFactory, false);
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
    Defaulttags changedOriginal = new Defaulttags(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Defaulttags(rockFactory, false);
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
    Defaulttags changedOriginal = new Defaulttags(rockFactory, false);
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