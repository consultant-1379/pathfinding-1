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
 * Test class for Externalstatement. Changes to Externalstatement table are made via
 * this class.
 */
public class ExternalstatementTest {

  private static Externalstatement objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSIONID;
  
  private static Field STATEMENTNAME;
  
  private static Field EXECUTIONORDER;
  
  private static Field DBCONNECTION;
  
  private static Field STATEMENT;
  
  private static Field BASEDEF;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Externalstatement.class.getDeclaredField("newItem");
		VERSIONID = Externalstatement.class.getDeclaredField("VERSIONID");
		STATEMENTNAME = Externalstatement.class.getDeclaredField("STATEMENTNAME");
		EXECUTIONORDER = Externalstatement.class.getDeclaredField("EXECUTIONORDER");
		DBCONNECTION = Externalstatement.class.getDeclaredField("DBCONNECTION");
		STATEMENT = Externalstatement.class.getDeclaredField("STATEMENT");
		BASEDEF = Externalstatement.class.getDeclaredField("BASEDEF");
		newItem.setAccessible(true);
		VERSIONID.setAccessible(true);
		STATEMENTNAME.setAccessible(true);
		EXECUTIONORDER.setAccessible(true);
		DBCONNECTION.setAccessible(true);
		STATEMENT.setAccessible(true);
		BASEDEF.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Externalstatement ( VERSIONID VARCHAR(31)  ,STATEMENTNAME VARCHAR(31) ,EXECUTIONORDER BIGINT  ,DBCONNECTION VARCHAR(31) ,STATEMENT VARCHAR(31) ,BASEDEF INTEGER )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Externalstatement");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Externalstatement VALUES( 'testVERSIONID'  ,'testSTATEMENTNAME'  ,1  ,'testDBCONNECTION'  ,'testSTATEMENT'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Externalstatement(rockFactory ,  "testVERSIONID",  "testSTATEMENTNAME",  1 );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Externalstatement");
    objUnderTest = null;
  }
  
  /**
   * Testing Externalstatement constructor variable initialization with null values.
   */
  @Test
  public void testExternalstatementConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Externalstatement(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + STATEMENTNAME.get(objUnderTest)  + ", " + EXECUTIONORDER.get(objUnderTest)  + ", " + DBCONNECTION.get(objUnderTest)  + ", " + STATEMENT.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Externalstatement constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testExternalstatementConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Externalstatement(rockFactory ,  "testVERSIONID",  "testSTATEMENTNAME",  1 );

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + STATEMENTNAME.get(objUnderTest)  + ", " + EXECUTIONORDER.get(objUnderTest)  + ", " + DBCONNECTION.get(objUnderTest)  + ", " + STATEMENT.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testSTATEMENTNAME"  + ", 1"  + ", testDBCONNECTION"  + ", testSTATEMENT"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testExternalstatementConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Externalstatement(null ,  "testVERSIONID",  "testSTATEMENTNAME",  1 );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Externalstatement constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testExternalstatementConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Externalstatement whereObject = new Externalstatement(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Externalstatement(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + STATEMENTNAME.get(objUnderTest)  + ", " + EXECUTIONORDER.get(objUnderTest)  + ", " + DBCONNECTION.get(objUnderTest)  + ", " + STATEMENT.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testSTATEMENTNAME"  + ", 1"  + ", testDBCONNECTION"  + ", testSTATEMENT"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testExternalstatementConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Externalstatement whereObject = new Externalstatement(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Externalstatement(null, whereObject);
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
    assertEquals("Externalstatement", objUnderTest.getTableName());
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
    Externalstatement whereObject = new Externalstatement(rockFactory);

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
    Externalstatement whereObject = new Externalstatement(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Externalstatement");
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
    VERSIONID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("VERSIONID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Externalstatement");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSIONID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSIONID FROM Externalstatement");
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
    
    String expected = "<Externalstatement VERSIONID=\"'testVERSIONID'\" STATEMENTNAME=\"'testSTATEMENTNAME'\" EXECUTIONORDER=\"1\" DBCONNECTION=\"'testDBCONNECTION'\" STATEMENT=\"'testSTATEMENT'\" BASEDEF=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Externalstatement VERSIONID=\"'testVERSIONID'\" STATEMENTNAME=\"'testSTATEMENTNAME'\" EXECUTIONORDER=\"1\" DBCONNECTION=\"'testDBCONNECTION'\" STATEMENT=\"'testSTATEMENT'\" BASEDEF=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Externalstatement>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Externalstatement ( VERSIONID, STATEMENTNAME, EXECUTIONORDER, DBCONNECTION, STATEMENT, BASEDEF ) values "
      + "( 'testVERSIONID', 'testSTATEMENTNAME', 1, 'testDBCONNECTION', 'testSTATEMENT', 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersionid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersionid(ExternalstatementTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(ExternalstatementTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatementname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatementname(ExternalstatementTest.testStringGenerator("anotherSTATEMENTNAME", 255));
    assertEquals(ExternalstatementTest.testStringGenerator("anotherSTATEMENTNAME", 255), STATEMENTNAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetExecutionorder() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setExecutionorder(555L);
    assertEquals(555L, EXECUTIONORDER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDbconnection() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDbconnection(ExternalstatementTest.testStringGenerator("anotherDBCONNECTION", 20));
    assertEquals(ExternalstatementTest.testStringGenerator("anotherDBCONNECTION", 20), DBCONNECTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatement() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatement(ExternalstatementTest.testStringGenerator("anotherSTATEMENT", 32000));
    assertEquals(ExternalstatementTest.testStringGenerator("anotherSTATEMENT", 32000), STATEMENT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBasedef() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBasedef(555);
    assertEquals(555, BASEDEF.get(objUnderTest));
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
    String[] expectedKeys = { "VERSIONID","STATEMENTNAME","BASEDEF"};

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
    objUnderTest = new Externalstatement(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSIONID.get(objUnderTest)  + ", " + STATEMENTNAME.get(objUnderTest)  + ", " + EXECUTIONORDER.get(objUnderTest)  + ", " + DBCONNECTION.get(objUnderTest)  + ", " + STATEMENT.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", 0"  + ", "  + ", "  + ", 0" ;
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
    Externalstatement compareObj = new Externalstatement(rockFactory ,  "testVERSIONID",  "testSTATEMENTNAME",  1 );

    /* Testing first with null primary key value */
    VERSIONID.set(objUnderTest, null);
  	STATEMENTNAME.set(objUnderTest, null);
  	BASEDEF.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSIONID.set(objUnderTest,  "different");
  	STATEMENTNAME.set(objUnderTest,  "different");
  	BASEDEF.set(objUnderTest,  7 );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSIONID.set(objUnderTest,  "testVERSIONID");
  	STATEMENTNAME.set(objUnderTest,  "testSTATEMENTNAME");
  	BASEDEF.set(objUnderTest,  1 );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Externalstatement with our current one. If the two
   * Externalstatements are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnExternalstatement() throws Exception {

    /* Creating another Externalstatement which will be compared to the tested one */
    Externalstatement comparedObj = new Externalstatement(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Externalstatement with our current one. If the two
   * Externalstatements are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentExternalstatement() throws Exception {

    /* Creating another Externalstatement which will be compared to the tested one */
    Externalstatement comparedObj = new Externalstatement(rockFactory ,  "testVERSIONID",  "testSTATEMENTNAME",  1 );
    comparedObj.setVersionid( "DifferentVERSIONID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Externalstatement with our current one. If the two
   * Externalstatements are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameExternalstatement() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Externalstatement comparedObj = new Externalstatement(rockFactory ,  "testVERSIONID",  "testSTATEMENTNAME",  1 );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Externalstatement with our current one using null value.
   */
  @Test
  public void testEqualsWithNullExternalstatement() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Externalstatement comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Externalstatement was null \n");
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
    assertEquals(Externalstatement.class, actualObject.getClass());
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
    Externalstatement testAgg = new Externalstatement(rockFactory ,  "testVERSIONID",  "testSTATEMENTNAME",  1 );
    VERSIONID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
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
   * Testing columnsize retrieving for Statementname.
   */
  @Test
  public void testGetStatementnameColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getStatementnameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Statementname.
  */
  @Test
  public void testGetStatementnameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getStatementnameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Statementname.
  */
  @Test
  public void testGetStatementnameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getStatementnameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Executionorder.
   */
  @Test
  public void testGetExecutionorderColumnSize() throws Exception {
    
     assertEquals(9, objUnderTest.getExecutionorderColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Executionorder.
  */
  @Test
  public void testGetExecutionorderDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getExecutionorderDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Executionorder.
  */
  @Test
  public void testGetExecutionorderSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getExecutionorderSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Dbconnection.
   */
  @Test
  public void testGetDbconnectionColumnSize() throws Exception {
    
     assertEquals(20, objUnderTest.getDbconnectionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dbconnection.
  */
  @Test
  public void testGetDbconnectionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDbconnectionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dbconnection.
  */
  @Test
  public void testGetDbconnectionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDbconnectionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Statement.
   */
  @Test
  public void testGetStatementColumnSize() throws Exception {
    
     assertEquals(2147483647, objUnderTest.getStatementColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Statement.
  */
  @Test
  public void testGetStatementDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getStatementDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Statement.
  */
  @Test
  public void testGetStatementSQLType() throws Exception {
    
    assertEquals(-1, objUnderTest.getStatementSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Basedef.
   */
  @Test
  public void testGetBasedefColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getBasedefColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Basedef.
  */
  @Test
  public void testGetBasedefDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBasedefDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Basedef.
  */
  @Test
  public void testGetBasedefSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getBasedefSQLType());    
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
   * Testing original Externalstatement object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Externalstatement(rockFactory, false);
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
    Externalstatement changedOriginal = new Externalstatement(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Externalstatement(rockFactory, false);
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
    Externalstatement changedOriginal = new Externalstatement(rockFactory, false);
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