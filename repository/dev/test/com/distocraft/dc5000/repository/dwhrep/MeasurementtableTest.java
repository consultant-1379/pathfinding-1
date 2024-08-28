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
 * Test class for Measurementtable. Changes to Measurementtable table are made via
 * this class.
 */
public class MeasurementtableTest {

  private static Measurementtable objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field MTABLEID;
  
  private static Field TABLELEVEL;
  
  private static Field TYPEID;
  
  private static Field BASETABLENAME;
  
  private static Field DEFAULT_TEMPLATE;
  
  private static Field PARTITIONPLAN;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Measurementtable.class.getDeclaredField("newItem");
		MTABLEID = Measurementtable.class.getDeclaredField("MTABLEID");
		TABLELEVEL = Measurementtable.class.getDeclaredField("TABLELEVEL");
		TYPEID = Measurementtable.class.getDeclaredField("TYPEID");
		BASETABLENAME = Measurementtable.class.getDeclaredField("BASETABLENAME");
		DEFAULT_TEMPLATE = Measurementtable.class.getDeclaredField("DEFAULT_TEMPLATE");
		PARTITIONPLAN = Measurementtable.class.getDeclaredField("PARTITIONPLAN");
		newItem.setAccessible(true);
		MTABLEID.setAccessible(true);
		TABLELEVEL.setAccessible(true);
		TYPEID.setAccessible(true);
		BASETABLENAME.setAccessible(true);
		DEFAULT_TEMPLATE.setAccessible(true);
		PARTITIONPLAN.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Measurementtable ( MTABLEID VARCHAR(31)  ,TABLELEVEL VARCHAR(31) ,TYPEID VARCHAR(31) ,BASETABLENAME VARCHAR(31) ,DEFAULT_TEMPLATE VARCHAR(31) ,PARTITIONPLAN VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Measurementtable");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Measurementtable VALUES( 'testMTABLEID'  ,'testTABLELEVEL'  ,'testTYPEID'  ,'testBASETABLENAME'  ,'testDEFAULT_TEMPLATE'  ,'testPARTITIONPLAN' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Measurementtable(rockFactory ,  "testMTABLEID");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Measurementtable");
    objUnderTest = null;
  }
  
  /**
   * Testing Measurementtable constructor variable initialization with null values.
   */
  @Test
  public void testMeasurementtableConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Measurementtable(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  MTABLEID.get(objUnderTest)  + ", " + TABLELEVEL.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + BASETABLENAME.get(objUnderTest)  + ", " + DEFAULT_TEMPLATE.get(objUnderTest)  + ", " + PARTITIONPLAN.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Measurementtable constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeasurementtableConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Measurementtable(rockFactory ,  "testMTABLEID");

    /* Asserting that variables are initialized */
    String actual =  MTABLEID.get(objUnderTest)  + ", " + TABLELEVEL.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + BASETABLENAME.get(objUnderTest)  + ", " + DEFAULT_TEMPLATE.get(objUnderTest)  + ", " + PARTITIONPLAN.get(objUnderTest) ;
    String expected =  "testMTABLEID"  + ", testTABLELEVEL"  + ", testTYPEID"  + ", testBASETABLENAME"  + ", testDEFAULT_TEMPLATE"  + ", testPARTITIONPLAN" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementtableConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Measurementtable(null ,  "testMTABLEID");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Measurementtable constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeasurementtableConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Measurementtable whereObject = new Measurementtable(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Measurementtable(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  MTABLEID.get(objUnderTest)  + ", " + TABLELEVEL.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + BASETABLENAME.get(objUnderTest)  + ", " + DEFAULT_TEMPLATE.get(objUnderTest)  + ", " + PARTITIONPLAN.get(objUnderTest) ;
    String expected =  "testMTABLEID"  + ", testTABLELEVEL"  + ", testTYPEID"  + ", testBASETABLENAME"  + ", testDEFAULT_TEMPLATE"  + ", testPARTITIONPLAN" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementtableConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Measurementtable whereObject = new Measurementtable(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Measurementtable(null, whereObject);
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
    assertEquals("Measurementtable", objUnderTest.getTableName());
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
    Measurementtable whereObject = new Measurementtable(rockFactory);

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
    Measurementtable whereObject = new Measurementtable(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementtable");
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
    MTABLEID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("MTABLEID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementtable");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the MTABLEID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT MTABLEID FROM Measurementtable");
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
    
    String expected = "<Measurementtable MTABLEID=\"'testMTABLEID'\" TABLELEVEL=\"'testTABLELEVEL'\" TYPEID=\"'testTYPEID'\" BASETABLENAME=\"'testBASETABLENAME'\" DEFAULT_TEMPLATE=\"'testDEFAULT_TEMPLATE'\" PARTITIONPLAN=\"'testPARTITIONPLAN'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Measurementtable MTABLEID=\"'testMTABLEID'\" TABLELEVEL=\"'testTABLELEVEL'\" TYPEID=\"'testTYPEID'\" BASETABLENAME=\"'testBASETABLENAME'\" DEFAULT_TEMPLATE=\"'testDEFAULT_TEMPLATE'\" PARTITIONPLAN=\"'testPARTITIONPLAN'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Measurementtable>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Measurementtable ( MTABLEID, TABLELEVEL, TYPEID, BASETABLENAME, DEFAULT_TEMPLATE, PARTITIONPLAN ) values "
      + "( 'testMTABLEID', 'testTABLELEVEL', 'testTYPEID', 'testBASETABLENAME', 'testDEFAULT_TEMPLATE', 'testPARTITIONPLAN' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMtableid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMtableid(MeasurementtableTest.testStringGenerator("anotherMTABLEID", 255));
    assertEquals(MeasurementtableTest.testStringGenerator("anotherMTABLEID", 255), MTABLEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTablelevel() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTablelevel(MeasurementtableTest.testStringGenerator("anotherTABLELEVEL", 50));
    assertEquals(MeasurementtableTest.testStringGenerator("anotherTABLELEVEL", 50), TABLELEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTypeid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTypeid(MeasurementtableTest.testStringGenerator("anotherTYPEID", 255));
    assertEquals(MeasurementtableTest.testStringGenerator("anotherTYPEID", 255), TYPEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBasetablename() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBasetablename(MeasurementtableTest.testStringGenerator("anotherBASETABLENAME", 255));
    assertEquals(MeasurementtableTest.testStringGenerator("anotherBASETABLENAME", 255), BASETABLENAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDefault_template() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDefault_template(MeasurementtableTest.testStringGenerator("anotherDEFAULT_TEMPLATE", 50));
    assertEquals(MeasurementtableTest.testStringGenerator("anotherDEFAULT_TEMPLATE", 50), DEFAULT_TEMPLATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPartitionplan() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPartitionplan(MeasurementtableTest.testStringGenerator("anotherPARTITIONPLAN", 128));
    assertEquals(MeasurementtableTest.testStringGenerator("anotherPARTITIONPLAN", 128), PARTITIONPLAN.get(objUnderTest));
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
    String[] expectedKeys = { "MTABLEID"};

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
    objUnderTest = new Measurementtable(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  MTABLEID.get(objUnderTest)  + ", " + TABLELEVEL.get(objUnderTest)  + ", " + TYPEID.get(objUnderTest)  + ", " + BASETABLENAME.get(objUnderTest)  + ", " + DEFAULT_TEMPLATE.get(objUnderTest)  + ", " + PARTITIONPLAN.get(objUnderTest) ;
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
    Measurementtable compareObj = new Measurementtable(rockFactory ,  "testMTABLEID");

    /* Testing first with null primary key value */
    MTABLEID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    MTABLEID.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    MTABLEID.set(objUnderTest,  "testMTABLEID");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Measurementtable with our current one. If the two
   * Measurementtables are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeasurementtable() throws Exception {

    /* Creating another Measurementtable which will be compared to the tested one */
    Measurementtable comparedObj = new Measurementtable(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementtable with our current one. If the two
   * Measurementtables are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeasurementtable() throws Exception {

    /* Creating another Measurementtable which will be compared to the tested one */
    Measurementtable comparedObj = new Measurementtable(rockFactory ,  "testMTABLEID");
    comparedObj.setMtableid( "DifferentMTABLEID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementtable with our current one. If the two
   * Measurementtables are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeasurementtable() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Measurementtable comparedObj = new Measurementtable(rockFactory ,  "testMTABLEID");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementtable with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeasurementtable() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Measurementtable comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Measurementtable was null \n");
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
    assertEquals(Measurementtable.class, actualObject.getClass());
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
    Measurementtable testAgg = new Measurementtable(rockFactory ,  "testMTABLEID");
    MTABLEID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Mtableid.
   */
  @Test
  public void testGetMtableidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getMtableidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Mtableid.
  */
  @Test
  public void testGetMtableidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMtableidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Mtableid.
  */
  @Test
  public void testGetMtableidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMtableidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Tablelevel.
   */
  @Test
  public void testGetTablelevelColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getTablelevelColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Tablelevel.
  */
  @Test
  public void testGetTablelevelDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTablelevelDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Tablelevel.
  */
  @Test
  public void testGetTablelevelSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTablelevelSQLType());    
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
   * Testing columnsize retrieving for Basetablename.
   */
  @Test
  public void testGetBasetablenameColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getBasetablenameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Basetablename.
  */
  @Test
  public void testGetBasetablenameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBasetablenameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Basetablename.
  */
  @Test
  public void testGetBasetablenameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBasetablenameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Default_template.
   */
  @Test
  public void testGetDefault_templateColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getDefault_templateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Default_template.
  */
  @Test
  public void testGetDefault_templateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDefault_templateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Default_template.
  */
  @Test
  public void testGetDefault_templateSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDefault_templateSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Partitionplan.
   */
  @Test
  public void testGetPartitionplanColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getPartitionplanColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Partitionplan.
  */
  @Test
  public void testGetPartitionplanDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPartitionplanDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Partitionplan.
  */
  @Test
  public void testGetPartitionplanSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPartitionplanSQLType());    
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
   * Testing original Measurementtable object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Measurementtable(rockFactory, false);
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
    Measurementtable changedOriginal = new Measurementtable(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Measurementtable(rockFactory, false);
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
    Measurementtable changedOriginal = new Measurementtable(rockFactory, false);
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