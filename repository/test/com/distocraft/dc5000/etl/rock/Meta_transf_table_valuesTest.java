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
 * Test class for Meta_transf_table_values. Changes to Meta_transf_table_values table are made via
 * this class.
 */
public class Meta_transf_table_valuesTest {

  private static Meta_transf_table_values objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field OLD_VALUE;
  
  private static Field NEW_VALUE;
  
  private static Field VERSION_NUMBER;
  
  private static Field TRANSF_TABLE_ID;
  
    private static Field timeStampName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Meta_transf_table_values.class.getDeclaredField("newItem");
		OLD_VALUE = Meta_transf_table_values.class.getDeclaredField("OLD_VALUE");
		NEW_VALUE = Meta_transf_table_values.class.getDeclaredField("NEW_VALUE");
		VERSION_NUMBER = Meta_transf_table_values.class.getDeclaredField("VERSION_NUMBER");
		TRANSF_TABLE_ID = Meta_transf_table_values.class.getDeclaredField("TRANSF_TABLE_ID");
		newItem.setAccessible(true);
		OLD_VALUE.setAccessible(true);
		NEW_VALUE.setAccessible(true);
		VERSION_NUMBER.setAccessible(true);
		TRANSF_TABLE_ID.setAccessible(true);
	  timeStampName = Meta_transf_table_values.class.getDeclaredField("timeStampName");
  timeStampName.setAccessible(true);
  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Meta_transf_table_values ( OLD_VALUE VARCHAR(31)  ,NEW_VALUE VARCHAR(31) ,VERSION_NUMBER VARCHAR(31) ,TRANSF_TABLE_ID BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Meta_transf_table_values");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Meta_transf_table_values VALUES( 'testOLD_VALUE'  ,'testNEW_VALUE'  ,'testVERSION_NUMBER'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Meta_transf_table_values(rockFactory ,  "testOLD_VALUE",  "testVERSION_NUMBER",  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Meta_transf_table_values");
    objUnderTest = null;
  }
  
  /**
   * Testing Meta_transf_table_values constructor variable initialization with null values.
   */
  @Test
  public void testMeta_transf_table_valuesConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transf_table_values(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  OLD_VALUE.get(objUnderTest)  + ", " + NEW_VALUE.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TRANSF_TABLE_ID.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Meta_transf_table_values constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_transf_table_valuesConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Meta_transf_table_values(rockFactory ,  "testOLD_VALUE",  "testVERSION_NUMBER",  1L );

    /* Asserting that variables are initialized */
    String actual =  OLD_VALUE.get(objUnderTest)  + ", " + NEW_VALUE.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TRANSF_TABLE_ID.get(objUnderTest) ;
    String expected =  "testOLD_VALUE"  + ", testNEW_VALUE"  + ", testVERSION_NUMBER"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_transf_table_valuesConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Meta_transf_table_values(null ,  "testOLD_VALUE",  "testVERSION_NUMBER",  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Meta_transf_table_values constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeta_transf_table_valuesConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_transf_table_values whereObject = new Meta_transf_table_values(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Meta_transf_table_values(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  OLD_VALUE.get(objUnderTest)  + ", " + NEW_VALUE.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TRANSF_TABLE_ID.get(objUnderTest) ;
    String expected =  "testOLD_VALUE"  + ", testNEW_VALUE"  + ", testVERSION_NUMBER"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeta_transf_table_valuesConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Meta_transf_table_values whereObject = new Meta_transf_table_values(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Meta_transf_table_values(null, whereObject);
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
    assertEquals("Meta_transf_table_values", objUnderTest.getTableName());
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
    Meta_transf_table_values whereObject = new Meta_transf_table_values(rockFactory);

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
    Meta_transf_table_values whereObject = new Meta_transf_table_values(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_transf_table_values");
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
    OLD_VALUE.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("OLD_VALUE");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Meta_transf_table_values");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the OLD_VALUE column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT OLD_VALUE FROM Meta_transf_table_values");
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
    
    String expected = "<Meta_transf_table_values OLD_VALUE=\"'testOLD_VALUE'\" NEW_VALUE=\"'testNEW_VALUE'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" TRANSF_TABLE_ID=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Meta_transf_table_values OLD_VALUE=\"'testOLD_VALUE'\" NEW_VALUE=\"'testNEW_VALUE'\" VERSION_NUMBER=\"'testVERSION_NUMBER'\" TRANSF_TABLE_ID=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Meta_transf_table_values>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Meta_transf_table_values ( OLD_VALUE, NEW_VALUE, VERSION_NUMBER, TRANSF_TABLE_ID ) values "
      + "( 'testOLD_VALUE', 'testNEW_VALUE', 'testVERSION_NUMBER', 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetOld_value() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setOld_value(Meta_transf_table_valuesTest.testStringGenerator("anotherOLD_VALUE", 30));
    assertEquals(Meta_transf_table_valuesTest.testStringGenerator("anotherOLD_VALUE", 30), OLD_VALUE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetNew_value() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setNew_value(Meta_transf_table_valuesTest.testStringGenerator("anotherNEW_VALUE", 30));
    assertEquals(Meta_transf_table_valuesTest.testStringGenerator("anotherNEW_VALUE", 30), NEW_VALUE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersion_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersion_number(Meta_transf_table_valuesTest.testStringGenerator("anotherVERSION_NUMBER", 32));
    assertEquals(Meta_transf_table_valuesTest.testStringGenerator("anotherVERSION_NUMBER", 32), VERSION_NUMBER.get(objUnderTest));
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
    String[] expectedKeys = { "OLD_VALUE","VERSION_NUMBER","TRANSF_TABLE_ID"};

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
    objUnderTest = new Meta_transf_table_values(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  OLD_VALUE.get(objUnderTest)  + ", " + NEW_VALUE.get(objUnderTest)  + ", " + VERSION_NUMBER.get(objUnderTest)  + ", " + TRANSF_TABLE_ID.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", 0" ;
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
    Meta_transf_table_values compareObj = new Meta_transf_table_values(rockFactory ,  "testOLD_VALUE",  "testVERSION_NUMBER",  1L );

    /* Testing first with null primary key value */
    OLD_VALUE.set(objUnderTest, null);
  	VERSION_NUMBER.set(objUnderTest, null);
  	TRANSF_TABLE_ID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    OLD_VALUE.set(objUnderTest,  "different");
  	VERSION_NUMBER.set(objUnderTest,  "different");
  	TRANSF_TABLE_ID.set(objUnderTest,  7L );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    OLD_VALUE.set(objUnderTest,  "testOLD_VALUE");
  	VERSION_NUMBER.set(objUnderTest,  "testVERSION_NUMBER");
  	TRANSF_TABLE_ID.set(objUnderTest,  1L );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Meta_transf_table_values with our current one. If the two
   * Meta_transf_table_valuess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeta_transf_table_values() throws Exception {

    /* Creating another Meta_transf_table_values which will be compared to the tested one */
    Meta_transf_table_values comparedObj = new Meta_transf_table_values(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_transf_table_values with our current one. If the two
   * Meta_transf_table_valuess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeta_transf_table_values() throws Exception {

    /* Creating another Meta_transf_table_values which will be compared to the tested one */
    Meta_transf_table_values comparedObj = new Meta_transf_table_values(rockFactory ,  "testOLD_VALUE",  "testVERSION_NUMBER",  1L );
    comparedObj.setOld_value( "DifferentOLD_VALUE");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_transf_table_values with our current one. If the two
   * Meta_transf_table_valuess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeta_transf_table_values() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_transf_table_values comparedObj = new Meta_transf_table_values(rockFactory ,  "testOLD_VALUE",  "testVERSION_NUMBER",  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Meta_transf_table_values with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeta_transf_table_values() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Meta_transf_table_values comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Meta_transf_table_values was null \n");
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
    assertEquals(Meta_transf_table_values.class, actualObject.getClass());
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
    Meta_transf_table_values testAgg = new Meta_transf_table_values(rockFactory ,  "testOLD_VALUE",  "testVERSION_NUMBER",  1L );
    OLD_VALUE.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Old_value.
   */
  @Test
  public void testGetOld_valueColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getOld_valueColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Old_value.
  */
  @Test
  public void testGetOld_valueDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getOld_valueDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Old_value.
  */
  @Test
  public void testGetOld_valueSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getOld_valueSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for New_value.
   */
  @Test
  public void testGetNew_valueColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getNew_valueColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for New_value.
  */
  @Test
  public void testGetNew_valueDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getNew_valueDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for New_value.
  */
  @Test
  public void testGetNew_valueSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getNew_valueSQLType());    
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
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Meta_transf_table_values object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Meta_transf_table_values(rockFactory, false);
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
    Meta_transf_table_values changedOriginal = new Meta_transf_table_values(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Meta_transf_table_values(rockFactory, false);
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
    Meta_transf_table_values changedOriginal = new Meta_transf_table_values(rockFactory, false);
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