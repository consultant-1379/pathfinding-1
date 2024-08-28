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
 * Test class for Verificationcondition. Changes to Verificationcondition table are made via
 * this class.
 */
public class VerificationconditionTest {

  private static Verificationcondition objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSIONID;
  
  private static Field FACTTABLE;
  
  private static Field VERLEVEL;
  
  private static Field CONDITIONCLASS;
  
  private static Field VERCONDITION;
  
  private static Field PROMPTNAME1;
  
  private static Field PROMPTVALUE1;
  
  private static Field PROMPTNAME2;
  
  private static Field PROMPTVALUE2;
  
  private static Field OBJECTCONDITION;
  
  private static Field PROMPTNAME3;
  
  private static Field PROMPTVALUE3;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Verificationcondition.class.getDeclaredField("newItem");
		VERSIONID = Verificationcondition.class.getDeclaredField("VERSIONID");
		FACTTABLE = Verificationcondition.class.getDeclaredField("FACTTABLE");
		VERLEVEL = Verificationcondition.class.getDeclaredField("VERLEVEL");
		CONDITIONCLASS = Verificationcondition.class.getDeclaredField("CONDITIONCLASS");
		VERCONDITION = Verificationcondition.class.getDeclaredField("VERCONDITION");
		PROMPTNAME1 = Verificationcondition.class.getDeclaredField("PROMPTNAME1");
		PROMPTVALUE1 = Verificationcondition.class.getDeclaredField("PROMPTVALUE1");
		PROMPTNAME2 = Verificationcondition.class.getDeclaredField("PROMPTNAME2");
		PROMPTVALUE2 = Verificationcondition.class.getDeclaredField("PROMPTVALUE2");
		OBJECTCONDITION = Verificationcondition.class.getDeclaredField("OBJECTCONDITION");
		PROMPTNAME3 = Verificationcondition.class.getDeclaredField("PROMPTNAME3");
		PROMPTVALUE3 = Verificationcondition.class.getDeclaredField("PROMPTVALUE3");
		newItem.setAccessible(true);
		VERSIONID.setAccessible(true);
		FACTTABLE.setAccessible(true);
		VERLEVEL.setAccessible(true);
		CONDITIONCLASS.setAccessible(true);
		VERCONDITION.setAccessible(true);
		PROMPTNAME1.setAccessible(true);
		PROMPTVALUE1.setAccessible(true);
		PROMPTNAME2.setAccessible(true);
		PROMPTVALUE2.setAccessible(true);
		OBJECTCONDITION.setAccessible(true);
		PROMPTNAME3.setAccessible(true);
		PROMPTVALUE3.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Verificationcondition ( VERSIONID VARCHAR(31)  ,FACTTABLE VARCHAR(31) ,VERLEVEL VARCHAR(31) ,CONDITIONCLASS VARCHAR(31) ,VERCONDITION VARCHAR(31) ,PROMPTNAME1 VARCHAR(31) ,PROMPTVALUE1 VARCHAR(31) ,PROMPTNAME2 VARCHAR(31) ,PROMPTVALUE2 VARCHAR(31) ,OBJECTCONDITION VARCHAR(31) ,PROMPTNAME3 VARCHAR(31) ,PROMPTVALUE3 VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Verificationcondition");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Verificationcondition VALUES( 'testVERSIONID'  ,'testFACTTABLE'  ,'testVERLEVEL'  ,'testCONDITIONCLASS'  ,'testVERCONDITION'  ,'testPROMPTNAME1'  ,'testPROMPTVALUE1'  ,'testPROMPTNAME2'  ,'testPROMPTVALUE2'  ,'testOBJECTCONDITION'  ,'testPROMPTNAME3'  ,'testPROMPTVALUE3' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Verificationcondition(rockFactory ,  "testVERSIONID",  "testVERLEVEL",  "testCONDITIONCLASS",  "testVERCONDITION");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Verificationcondition");
    objUnderTest = null;
  }
  
  /**
   * Testing Verificationcondition constructor variable initialization with null values.
   */
  @Test
  public void testVerificationconditionConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Verificationcondition(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + FACTTABLE.get(objUnderTest)  + ", " + VERLEVEL.get(objUnderTest)  + ", " + CONDITIONCLASS.get(objUnderTest)  + ", " + VERCONDITION.get(objUnderTest)  + ", " + PROMPTNAME1.get(objUnderTest)  + ", " + PROMPTVALUE1.get(objUnderTest)  + ", " + PROMPTNAME2.get(objUnderTest)  + ", " + PROMPTVALUE2.get(objUnderTest)  + ", " + OBJECTCONDITION.get(objUnderTest)  + ", " + PROMPTNAME3.get(objUnderTest)  + ", " + PROMPTVALUE3.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Verificationcondition constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testVerificationconditionConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Verificationcondition(rockFactory ,  "testVERSIONID",  "testVERLEVEL",  "testCONDITIONCLASS",  "testVERCONDITION");

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + FACTTABLE.get(objUnderTest)  + ", " + VERLEVEL.get(objUnderTest)  + ", " + CONDITIONCLASS.get(objUnderTest)  + ", " + VERCONDITION.get(objUnderTest)  + ", " + PROMPTNAME1.get(objUnderTest)  + ", " + PROMPTVALUE1.get(objUnderTest)  + ", " + PROMPTNAME2.get(objUnderTest)  + ", " + PROMPTVALUE2.get(objUnderTest)  + ", " + OBJECTCONDITION.get(objUnderTest)  + ", " + PROMPTNAME3.get(objUnderTest)  + ", " + PROMPTVALUE3.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testFACTTABLE"  + ", testVERLEVEL"  + ", testCONDITIONCLASS"  + ", testVERCONDITION"  + ", testPROMPTNAME1"  + ", testPROMPTVALUE1"  + ", testPROMPTNAME2"  + ", testPROMPTVALUE2"  + ", testOBJECTCONDITION"  + ", testPROMPTNAME3"  + ", testPROMPTVALUE3" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testVerificationconditionConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Verificationcondition(null ,  "testVERSIONID",  "testVERLEVEL",  "testCONDITIONCLASS",  "testVERCONDITION");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Verificationcondition constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testVerificationconditionConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Verificationcondition whereObject = new Verificationcondition(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Verificationcondition(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + FACTTABLE.get(objUnderTest)  + ", " + VERLEVEL.get(objUnderTest)  + ", " + CONDITIONCLASS.get(objUnderTest)  + ", " + VERCONDITION.get(objUnderTest)  + ", " + PROMPTNAME1.get(objUnderTest)  + ", " + PROMPTVALUE1.get(objUnderTest)  + ", " + PROMPTNAME2.get(objUnderTest)  + ", " + PROMPTVALUE2.get(objUnderTest)  + ", " + OBJECTCONDITION.get(objUnderTest)  + ", " + PROMPTNAME3.get(objUnderTest)  + ", " + PROMPTVALUE3.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testFACTTABLE"  + ", testVERLEVEL"  + ", testCONDITIONCLASS"  + ", testVERCONDITION"  + ", testPROMPTNAME1"  + ", testPROMPTVALUE1"  + ", testPROMPTNAME2"  + ", testPROMPTVALUE2"  + ", testOBJECTCONDITION"  + ", testPROMPTNAME3"  + ", testPROMPTVALUE3" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testVerificationconditionConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Verificationcondition whereObject = new Verificationcondition(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Verificationcondition(null, whereObject);
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
    assertEquals("Verificationcondition", objUnderTest.getTableName());
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
    Verificationcondition whereObject = new Verificationcondition(rockFactory);

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
    Verificationcondition whereObject = new Verificationcondition(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Verificationcondition");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Verificationcondition");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSIONID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSIONID FROM Verificationcondition");
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
    
    String expected = "<Verificationcondition VERSIONID=\"'testVERSIONID'\" FACTTABLE=\"'testFACTTABLE'\" VERLEVEL=\"'testVERLEVEL'\" CONDITIONCLASS=\"'testCONDITIONCLASS'\" VERCONDITION=\"'testVERCONDITION'\" PROMPTNAME1=\"'testPROMPTNAME1'\" PROMPTVALUE1=\"'testPROMPTVALUE1'\" PROMPTNAME2=\"'testPROMPTNAME2'\" PROMPTVALUE2=\"'testPROMPTVALUE2'\" OBJECTCONDITION=\"'testOBJECTCONDITION'\" PROMPTNAME3=\"'testPROMPTNAME3'\" PROMPTVALUE3=\"'testPROMPTVALUE3'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Verificationcondition VERSIONID=\"'testVERSIONID'\" FACTTABLE=\"'testFACTTABLE'\" VERLEVEL=\"'testVERLEVEL'\" CONDITIONCLASS=\"'testCONDITIONCLASS'\" VERCONDITION=\"'testVERCONDITION'\" PROMPTNAME1=\"'testPROMPTNAME1'\" PROMPTVALUE1=\"'testPROMPTVALUE1'\" PROMPTNAME2=\"'testPROMPTNAME2'\" PROMPTVALUE2=\"'testPROMPTVALUE2'\" OBJECTCONDITION=\"'testOBJECTCONDITION'\" PROMPTNAME3=\"'testPROMPTNAME3'\" PROMPTVALUE3=\"'testPROMPTVALUE3'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Verificationcondition>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Verificationcondition ( VERSIONID, FACTTABLE, VERLEVEL, CONDITIONCLASS, VERCONDITION, PROMPTNAME1, PROMPTVALUE1, PROMPTNAME2, PROMPTVALUE2, OBJECTCONDITION, PROMPTNAME3, PROMPTVALUE3 ) values "
      + "( 'testVERSIONID', 'testFACTTABLE', 'testVERLEVEL', 'testCONDITIONCLASS', 'testVERCONDITION', 'testPROMPTNAME1', 'testPROMPTVALUE1', 'testPROMPTNAME2', 'testPROMPTVALUE2', 'testOBJECTCONDITION', 'testPROMPTNAME3', 'testPROMPTVALUE3' );\n";
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
    objUnderTest.setVersionid(VerificationconditionTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFacttable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFacttable(VerificationconditionTest.testStringGenerator("anotherFACTTABLE", 2560));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherFACTTABLE", 2560), FACTTABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVerlevel() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVerlevel(VerificationconditionTest.testStringGenerator("anotherVERLEVEL", 32));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherVERLEVEL", 32), VERLEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetConditionclass() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setConditionclass(VerificationconditionTest.testStringGenerator("anotherCONDITIONCLASS", 32));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherCONDITIONCLASS", 32), CONDITIONCLASS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVercondition() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVercondition(VerificationconditionTest.testStringGenerator("anotherVERCONDITION", 128));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherVERCONDITION", 128), VERCONDITION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPromptname1() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPromptname1(VerificationconditionTest.testStringGenerator("anotherPROMPTNAME1", 255));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherPROMPTNAME1", 255), PROMPTNAME1.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPromptvalue1() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPromptvalue1(VerificationconditionTest.testStringGenerator("anotherPROMPTVALUE1", 128));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherPROMPTVALUE1", 128), PROMPTVALUE1.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPromptname2() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPromptname2(VerificationconditionTest.testStringGenerator("anotherPROMPTNAME2", 255));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherPROMPTNAME2", 255), PROMPTNAME2.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPromptvalue2() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPromptvalue2(VerificationconditionTest.testStringGenerator("anotherPROMPTVALUE2", 128));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherPROMPTVALUE2", 128), PROMPTVALUE2.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjectcondition() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjectcondition(VerificationconditionTest.testStringGenerator("anotherOBJECTCONDITION", 255));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherOBJECTCONDITION", 255), OBJECTCONDITION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPromptname3() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPromptname3(VerificationconditionTest.testStringGenerator("anotherPROMPTNAME3", 255));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherPROMPTNAME3", 255), PROMPTNAME3.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPromptvalue3() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPromptvalue3(VerificationconditionTest.testStringGenerator("anotherPROMPTVALUE3", 128));
    assertEquals(VerificationconditionTest.testStringGenerator("anotherPROMPTVALUE3", 128), PROMPTVALUE3.get(objUnderTest));
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
    String[] expectedKeys = { "VERSIONID","VERLEVEL","CONDITIONCLASS","VERCONDITION"};

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
    objUnderTest = new Verificationcondition(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSIONID.get(objUnderTest)  + ", " + FACTTABLE.get(objUnderTest)  + ", " + VERLEVEL.get(objUnderTest)  + ", " + CONDITIONCLASS.get(objUnderTest)  + ", " + VERCONDITION.get(objUnderTest)  + ", " + PROMPTNAME1.get(objUnderTest)  + ", " + PROMPTVALUE1.get(objUnderTest)  + ", " + PROMPTNAME2.get(objUnderTest)  + ", " + PROMPTVALUE2.get(objUnderTest)  + ", " + OBJECTCONDITION.get(objUnderTest)  + ", " + PROMPTNAME3.get(objUnderTest)  + ", " + PROMPTVALUE3.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", " ;
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
    Verificationcondition compareObj = new Verificationcondition(rockFactory ,  "testVERSIONID",  "testVERLEVEL",  "testCONDITIONCLASS",  "testVERCONDITION");

    /* Testing first with null primary key value */
    VERSIONID.set(objUnderTest, null);
  	VERLEVEL.set(objUnderTest, null);
  	CONDITIONCLASS.set(objUnderTest, null);
  	VERCONDITION.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSIONID.set(objUnderTest,  "different");
  	VERLEVEL.set(objUnderTest,  "different");
  	CONDITIONCLASS.set(objUnderTest,  "different");
  	VERCONDITION.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSIONID.set(objUnderTest,  "testVERSIONID");
  	VERLEVEL.set(objUnderTest,  "testVERLEVEL");
  	CONDITIONCLASS.set(objUnderTest,  "testCONDITIONCLASS");
  	VERCONDITION.set(objUnderTest,  "testVERCONDITION");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Verificationcondition with our current one. If the two
   * Verificationconditions are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnVerificationcondition() throws Exception {

    /* Creating another Verificationcondition which will be compared to the tested one */
    Verificationcondition comparedObj = new Verificationcondition(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Verificationcondition with our current one. If the two
   * Verificationconditions are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentVerificationcondition() throws Exception {

    /* Creating another Verificationcondition which will be compared to the tested one */
    Verificationcondition comparedObj = new Verificationcondition(rockFactory ,  "testVERSIONID",  "testVERLEVEL",  "testCONDITIONCLASS",  "testVERCONDITION");
    comparedObj.setVersionid( "DifferentVERSIONID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Verificationcondition with our current one. If the two
   * Verificationconditions are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameVerificationcondition() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Verificationcondition comparedObj = new Verificationcondition(rockFactory ,  "testVERSIONID",  "testVERLEVEL",  "testCONDITIONCLASS",  "testVERCONDITION");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Verificationcondition with our current one using null value.
   */
  @Test
  public void testEqualsWithNullVerificationcondition() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Verificationcondition comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Verificationcondition was null \n");
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
    assertEquals(Verificationcondition.class, actualObject.getClass());
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
    Verificationcondition testAgg = new Verificationcondition(rockFactory ,  "testVERSIONID",  "testVERLEVEL",  "testCONDITIONCLASS",  "testVERCONDITION");
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
   * Testing columnsize retrieving for Facttable.
   */
  @Test
  public void testGetFacttableColumnSize() throws Exception {
    
     assertEquals(2560, objUnderTest.getFacttableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Facttable.
  */
  @Test
  public void testGetFacttableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFacttableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Facttable.
  */
  @Test
  public void testGetFacttableSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getFacttableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Verlevel.
   */
  @Test
  public void testGetVerlevelColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getVerlevelColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Verlevel.
  */
  @Test
  public void testGetVerlevelDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getVerlevelDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Verlevel.
  */
  @Test
  public void testGetVerlevelSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getVerlevelSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Conditionclass.
   */
  @Test
  public void testGetConditionclassColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getConditionclassColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Conditionclass.
  */
  @Test
  public void testGetConditionclassDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getConditionclassDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Conditionclass.
  */
  @Test
  public void testGetConditionclassSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getConditionclassSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Vercondition.
   */
  @Test
  public void testGetVerconditionColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getVerconditionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Vercondition.
  */
  @Test
  public void testGetVerconditionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getVerconditionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Vercondition.
  */
  @Test
  public void testGetVerconditionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getVerconditionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Promptname1.
   */
  @Test
  public void testGetPromptname1ColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getPromptname1ColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Promptname1.
  */
  @Test
  public void testGetPromptname1DecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPromptname1DecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Promptname1.
  */
  @Test
  public void testGetPromptname1SQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPromptname1SQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Promptvalue1.
   */
  @Test
  public void testGetPromptvalue1ColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getPromptvalue1ColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Promptvalue1.
  */
  @Test
  public void testGetPromptvalue1DecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPromptvalue1DecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Promptvalue1.
  */
  @Test
  public void testGetPromptvalue1SQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPromptvalue1SQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Promptname2.
   */
  @Test
  public void testGetPromptname2ColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getPromptname2ColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Promptname2.
  */
  @Test
  public void testGetPromptname2DecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPromptname2DecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Promptname2.
  */
  @Test
  public void testGetPromptname2SQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPromptname2SQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Promptvalue2.
   */
  @Test
  public void testGetPromptvalue2ColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getPromptvalue2ColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Promptvalue2.
  */
  @Test
  public void testGetPromptvalue2DecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPromptvalue2DecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Promptvalue2.
  */
  @Test
  public void testGetPromptvalue2SQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPromptvalue2SQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objectcondition.
   */
  @Test
  public void testGetObjectconditionColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getObjectconditionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objectcondition.
  */
  @Test
  public void testGetObjectconditionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjectconditionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objectcondition.
  */
  @Test
  public void testGetObjectconditionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjectconditionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Promptname3.
   */
  @Test
  public void testGetPromptname3ColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getPromptname3ColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Promptname3.
  */
  @Test
  public void testGetPromptname3DecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPromptname3DecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Promptname3.
  */
  @Test
  public void testGetPromptname3SQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPromptname3SQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Promptvalue3.
   */
  @Test
  public void testGetPromptvalue3ColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getPromptvalue3ColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Promptvalue3.
  */
  @Test
  public void testGetPromptvalue3DecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPromptvalue3DecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Promptvalue3.
  */
  @Test
  public void testGetPromptvalue3SQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPromptvalue3SQLType());    
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
   * Testing original Verificationcondition object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Verificationcondition(rockFactory, false);
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
    Verificationcondition changedOriginal = new Verificationcondition(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Verificationcondition(rockFactory, false);
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
    Verificationcondition changedOriginal = new Verificationcondition(rockFactory, false);
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