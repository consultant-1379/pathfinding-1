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
 * Test class for Universejoin. Changes to Universejoin table are made via
 * this class.
 */
public class UniversejoinTest {

  private static Universejoin objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSIONID;
  
  private static Field SOURCETABLE;
  
  private static Field SOURCELEVEL;
  
  private static Field SOURCECOLUMN;
  
  private static Field TARGETTABLE;
  
  private static Field TARGETLEVEL;
  
  private static Field TARGETCOLUMN;
  
  private static Field EXPRESSION;
  
  private static Field CARDINALITY;
  
  private static Field CONTEXT;
  
  private static Field EXCLUDEDCONTEXTS;
  
  private static Field TMPCOUNTER;
  
  private static Field ORDERNRO;
  
  private static Field UNIVERSEEXTENSION;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Universejoin.class.getDeclaredField("newItem");
		VERSIONID = Universejoin.class.getDeclaredField("VERSIONID");
		SOURCETABLE = Universejoin.class.getDeclaredField("SOURCETABLE");
		SOURCELEVEL = Universejoin.class.getDeclaredField("SOURCELEVEL");
		SOURCECOLUMN = Universejoin.class.getDeclaredField("SOURCECOLUMN");
		TARGETTABLE = Universejoin.class.getDeclaredField("TARGETTABLE");
		TARGETLEVEL = Universejoin.class.getDeclaredField("TARGETLEVEL");
		TARGETCOLUMN = Universejoin.class.getDeclaredField("TARGETCOLUMN");
		EXPRESSION = Universejoin.class.getDeclaredField("EXPRESSION");
		CARDINALITY = Universejoin.class.getDeclaredField("CARDINALITY");
		CONTEXT = Universejoin.class.getDeclaredField("CONTEXT");
		EXCLUDEDCONTEXTS = Universejoin.class.getDeclaredField("EXCLUDEDCONTEXTS");
		TMPCOUNTER = Universejoin.class.getDeclaredField("TMPCOUNTER");
		ORDERNRO = Universejoin.class.getDeclaredField("ORDERNRO");
		UNIVERSEEXTENSION = Universejoin.class.getDeclaredField("UNIVERSEEXTENSION");
		newItem.setAccessible(true);
		VERSIONID.setAccessible(true);
		SOURCETABLE.setAccessible(true);
		SOURCELEVEL.setAccessible(true);
		SOURCECOLUMN.setAccessible(true);
		TARGETTABLE.setAccessible(true);
		TARGETLEVEL.setAccessible(true);
		TARGETCOLUMN.setAccessible(true);
		EXPRESSION.setAccessible(true);
		CARDINALITY.setAccessible(true);
		CONTEXT.setAccessible(true);
		EXCLUDEDCONTEXTS.setAccessible(true);
		TMPCOUNTER.setAccessible(true);
		ORDERNRO.setAccessible(true);
		UNIVERSEEXTENSION.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Universejoin ( VERSIONID VARCHAR(31)  ,SOURCETABLE VARCHAR(31) ,SOURCELEVEL VARCHAR(31) ,SOURCECOLUMN VARCHAR(31) ,TARGETTABLE VARCHAR(31) ,TARGETLEVEL VARCHAR(31) ,TARGETCOLUMN VARCHAR(31) ,EXPRESSION VARCHAR(31) ,CARDINALITY VARCHAR(31) ,CONTEXT VARCHAR(31) ,EXCLUDEDCONTEXTS VARCHAR(31) ,TMPCOUNTER INTEGER  ,ORDERNRO BIGINT, UNIVERSEEXTENSION VARCHAR(31) )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Universejoin");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Universejoin VALUES( 'testVERSIONID'  ,'testSOURCETABLE'  ,'testSOURCELEVEL'  ,'testSOURCECOLUMN'  ,'testTARGETTABLE'  ,'testTARGETLEVEL'  ,'testTARGETCOLUMN'  ,'testEXPRESSION'  ,'testCARDINALITY'  ,'testCONTEXT'  ,'testEXCLUDEDCONTEXTS'  ,1  ,1, 'a' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Universejoin(rockFactory ,  "testVERSIONID",  "testSOURCETABLE",  "testSOURCECOLUMN",  "testTARGETTABLE",  "testTARGETCOLUMN",  1 );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Universejoin");
    objUnderTest = null;
  }
  
  /**
   * Testing Universejoin constructor variable initialization with null values.
   */
  @Test
  public void testUniversejoinConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Universejoin(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + SOURCETABLE.get(objUnderTest)  + ", " + SOURCELEVEL.get(objUnderTest)  + ", " + SOURCECOLUMN.get(objUnderTest)  + ", " + TARGETTABLE.get(objUnderTest)  + ", " + TARGETLEVEL.get(objUnderTest)  + ", " + TARGETCOLUMN.get(objUnderTest)  + ", " + EXPRESSION.get(objUnderTest)  + ", " + CARDINALITY.get(objUnderTest)  + ", " + CONTEXT.get(objUnderTest)  + ", " + EXCLUDEDCONTEXTS.get(objUnderTest)  + ", " + TMPCOUNTER.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Universejoin constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testUniversejoinConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Universejoin(rockFactory ,  "testVERSIONID",  "testSOURCETABLE",  "testSOURCECOLUMN",  "testTARGETTABLE",  "testTARGETCOLUMN",  1 );

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + SOURCETABLE.get(objUnderTest)  + ", " + SOURCELEVEL.get(objUnderTest)  + ", " + SOURCECOLUMN.get(objUnderTest)  + ", " + TARGETTABLE.get(objUnderTest)  + ", " + TARGETLEVEL.get(objUnderTest)  + ", " + TARGETCOLUMN.get(objUnderTest)  + ", " + EXPRESSION.get(objUnderTest)  + ", " + CARDINALITY.get(objUnderTest)  + ", " + CONTEXT.get(objUnderTest)  + ", " + EXCLUDEDCONTEXTS.get(objUnderTest)  + ", " + TMPCOUNTER.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testSOURCETABLE"  + ", testSOURCELEVEL"  + ", testSOURCECOLUMN"  + ", testTARGETTABLE"  + ", testTARGETLEVEL"  + ", testTARGETCOLUMN"  + ", testEXPRESSION"  + ", testCARDINALITY"  + ", testCONTEXT"  + ", testEXCLUDEDCONTEXTS"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniversejoinConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Universejoin(null ,  "testVERSIONID",  "testSOURCETABLE",  "testSOURCECOLUMN",  "testTARGETTABLE",  "testTARGETCOLUMN",  1 );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Universejoin constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testUniversejoinConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Universejoin whereObject = new Universejoin(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Universejoin(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + SOURCETABLE.get(objUnderTest)  + ", " + SOURCELEVEL.get(objUnderTest)  + ", " + SOURCECOLUMN.get(objUnderTest)  + ", " + TARGETTABLE.get(objUnderTest)  + ", " + TARGETLEVEL.get(objUnderTest)  + ", " + TARGETCOLUMN.get(objUnderTest)  + ", " + EXPRESSION.get(objUnderTest)  + ", " + CARDINALITY.get(objUnderTest)  + ", " + CONTEXT.get(objUnderTest)  + ", " + EXCLUDEDCONTEXTS.get(objUnderTest)  + ", " + TMPCOUNTER.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testSOURCETABLE"  + ", testSOURCELEVEL"  + ", testSOURCECOLUMN"  + ", testTARGETTABLE"  + ", testTARGETLEVEL"  + ", testTARGETCOLUMN"  + ", testEXPRESSION"  + ", testCARDINALITY"  + ", testCONTEXT"  + ", testEXCLUDEDCONTEXTS"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniversejoinConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Universejoin whereObject = new Universejoin(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Universejoin(null, whereObject);
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
    assertEquals("Universejoin", objUnderTest.getTableName());
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
    Universejoin whereObject = new Universejoin(rockFactory);

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
    Universejoin whereObject = new Universejoin(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universejoin");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universejoin");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSIONID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSIONID FROM Universejoin");
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
    
    String expected = "<Universejoin VERSIONID=\"'testVERSIONID'\" SOURCETABLE=\"'testSOURCETABLE'\" SOURCELEVEL=\"'testSOURCELEVEL'\" SOURCECOLUMN=\"'testSOURCECOLUMN'\" TARGETTABLE=\"'testTARGETTABLE'\" TARGETLEVEL=\"'testTARGETLEVEL'\" TARGETCOLUMN=\"'testTARGETCOLUMN'\" EXPRESSION=\"'testEXPRESSION'\" CARDINALITY=\"'testCARDINALITY'\" CONTEXT=\"'testCONTEXT'\" EXCLUDEDCONTEXTS=\"'testEXCLUDEDCONTEXTS'\" TMPCOUNTER=\"1\" ORDERNRO=\"1\" UNIVERSEEXTENSION=\"'a'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Universejoin VERSIONID=\"'testVERSIONID'\" SOURCETABLE=\"'testSOURCETABLE'\" SOURCELEVEL=\"'testSOURCELEVEL'\" SOURCECOLUMN=\"'testSOURCECOLUMN'\" TARGETTABLE=\"'testTARGETTABLE'\" TARGETLEVEL=\"'testTARGETLEVEL'\" TARGETCOLUMN=\"'testTARGETCOLUMN'\" EXPRESSION=\"'testEXPRESSION'\" CARDINALITY=\"'testCARDINALITY'\" CONTEXT=\"'testCONTEXT'\" EXCLUDEDCONTEXTS=\"'testEXCLUDEDCONTEXTS'\" TMPCOUNTER=\"1\" ORDERNRO=\"1\" UNIVERSEEXTENSION=\"'a'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Universejoin>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Universejoin ( VERSIONID, SOURCETABLE, SOURCELEVEL, SOURCECOLUMN, TARGETTABLE, TARGETLEVEL, TARGETCOLUMN, EXPRESSION, CARDINALITY, CONTEXT, EXCLUDEDCONTEXTS, TMPCOUNTER, ORDERNRO, UNIVERSEEXTENSION ) values "
      + "( 'testVERSIONID', 'testSOURCETABLE', 'testSOURCELEVEL', 'testSOURCECOLUMN', 'testTARGETTABLE', 'testTARGETLEVEL', 'testTARGETCOLUMN', 'testEXPRESSION', 'testCARDINALITY', 'testCONTEXT', 'testEXCLUDEDCONTEXTS', 1, 1, 'a' );\n";
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
    objUnderTest.setVersionid(UniversejoinTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(UniversejoinTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSourcetable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSourcetable(UniversejoinTest.testStringGenerator("anotherSOURCETABLE", 32000));
    assertEquals(UniversejoinTest.testStringGenerator("anotherSOURCETABLE", 32000), SOURCETABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSourcelevel() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSourcelevel(UniversejoinTest.testStringGenerator("anotherSOURCELEVEL", 255));
    assertEquals(UniversejoinTest.testStringGenerator("anotherSOURCELEVEL", 255), SOURCELEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSourcecolumn() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSourcecolumn(UniversejoinTest.testStringGenerator("anotherSOURCECOLUMN", 255));
    assertEquals(UniversejoinTest.testStringGenerator("anotherSOURCECOLUMN", 255), SOURCECOLUMN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTargettable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTargettable(UniversejoinTest.testStringGenerator("anotherTARGETTABLE", 32000));
    assertEquals(UniversejoinTest.testStringGenerator("anotherTARGETTABLE", 32000), TARGETTABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTargetlevel() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTargetlevel(UniversejoinTest.testStringGenerator("anotherTARGETLEVEL", 255));
    assertEquals(UniversejoinTest.testStringGenerator("anotherTARGETLEVEL", 255), TARGETLEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTargetcolumn() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTargetcolumn(UniversejoinTest.testStringGenerator("anotherTARGETCOLUMN", 255));
    assertEquals(UniversejoinTest.testStringGenerator("anotherTARGETCOLUMN", 255), TARGETCOLUMN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetExpression() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setExpression(UniversejoinTest.testStringGenerator("anotherEXPRESSION", 255));
    assertEquals(UniversejoinTest.testStringGenerator("anotherEXPRESSION", 255), EXPRESSION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCardinality() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCardinality(UniversejoinTest.testStringGenerator("anotherCARDINALITY", 255));
    assertEquals(UniversejoinTest.testStringGenerator("anotherCARDINALITY", 255), CARDINALITY.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetContext() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setContext(UniversejoinTest.testStringGenerator("anotherCONTEXT", 32000));
    assertEquals(UniversejoinTest.testStringGenerator("anotherCONTEXT", 32000), CONTEXT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetExcludedcontexts() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setExcludedcontexts(UniversejoinTest.testStringGenerator("anotherEXCLUDEDCONTEXTS", 32000));
    assertEquals(UniversejoinTest.testStringGenerator("anotherEXCLUDEDCONTEXTS", 32000), EXCLUDEDCONTEXTS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTmpcounter() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTmpcounter(555);
    assertEquals(555, TMPCOUNTER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetOrdernro() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setOrdernro(555L);
    assertEquals(555L, ORDERNRO.get(objUnderTest));
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
    String[] expectedKeys = { "VERSIONID","SOURCETABLE","SOURCECOLUMN","TARGETTABLE","TARGETCOLUMN","TMPCOUNTER"};

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
    objUnderTest = new Universejoin(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSIONID.get(objUnderTest)  + ", " + SOURCETABLE.get(objUnderTest)  + ", " + SOURCELEVEL.get(objUnderTest)  + ", " + SOURCECOLUMN.get(objUnderTest)  + ", " + TARGETTABLE.get(objUnderTest)  + ", " + TARGETLEVEL.get(objUnderTest)  + ", " + TARGETCOLUMN.get(objUnderTest)  + ", " + EXPRESSION.get(objUnderTest)  + ", " + CARDINALITY.get(objUnderTest)  + ", " + CONTEXT.get(objUnderTest)  + ", " + EXCLUDEDCONTEXTS.get(objUnderTest)  + ", " + TMPCOUNTER.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0"  + ", 0" ;
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
    Universejoin compareObj = new Universejoin(rockFactory ,  "testVERSIONID",  "testSOURCETABLE",  "testSOURCECOLUMN",  "testTARGETTABLE",  "testTARGETCOLUMN",  1 );

    /* Testing first with null primary key value */
    VERSIONID.set(objUnderTest, null);
  	SOURCETABLE.set(objUnderTest, null);
  	SOURCECOLUMN.set(objUnderTest, null);
  	TARGETTABLE.set(objUnderTest, null);
  	TARGETCOLUMN.set(objUnderTest, null);
  	TMPCOUNTER.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSIONID.set(objUnderTest,  "different");
  	SOURCETABLE.set(objUnderTest,  "different");
  	SOURCECOLUMN.set(objUnderTest,  "different");
  	TARGETTABLE.set(objUnderTest,  "different");
  	TARGETCOLUMN.set(objUnderTest,  "different");
  	TMPCOUNTER.set(objUnderTest,  7 );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSIONID.set(objUnderTest,  "testVERSIONID");
  	SOURCETABLE.set(objUnderTest,  "testSOURCETABLE");
  	SOURCECOLUMN.set(objUnderTest,  "testSOURCECOLUMN");
  	TARGETTABLE.set(objUnderTest,  "testTARGETTABLE");
  	TARGETCOLUMN.set(objUnderTest,  "testTARGETCOLUMN");
  	TMPCOUNTER.set(objUnderTest,  1 );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Universejoin with our current one. If the two
   * Universejoins are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnUniversejoin() throws Exception {

    /* Creating another Universejoin which will be compared to the tested one */
    Universejoin comparedObj = new Universejoin(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universejoin with our current one. If the two
   * Universejoins are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentUniversejoin() throws Exception {

    /* Creating another Universejoin which will be compared to the tested one */
    Universejoin comparedObj = new Universejoin(rockFactory ,  "testVERSIONID",  "testSOURCETABLE",  "testSOURCECOLUMN",  "testTARGETTABLE",  "testTARGETCOLUMN",  1 );
    comparedObj.setVersionid( "DifferentVERSIONID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universejoin with our current one. If the two
   * Universejoins are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameUniversejoin() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Universejoin comparedObj = new Universejoin(rockFactory ,  "testVERSIONID",  "testSOURCETABLE",  "testSOURCECOLUMN",  "testTARGETTABLE",  "testTARGETCOLUMN",  1 );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universejoin with our current one using null value.
   */
  @Test
  public void testEqualsWithNullUniversejoin() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Universejoin comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Universejoin was null \n");
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
    assertEquals(Universejoin.class, actualObject.getClass());
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
    Universejoin testAgg = new Universejoin(rockFactory ,  "testVERSIONID",  "testSOURCETABLE",  "testSOURCECOLUMN",  "testTARGETTABLE",  "testTARGETCOLUMN",  1 );
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
   * Testing columnsize retrieving for Sourcetable.
   */
  @Test
  public void testGetSourcetableColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getSourcetableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Sourcetable.
  */
  @Test
  public void testGetSourcetableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSourcetableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Sourcetable.
  */
  @Test
  public void testGetSourcetableSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSourcetableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Sourcelevel.
   */
  @Test
  public void testGetSourcelevelColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getSourcelevelColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Sourcelevel.
  */
  @Test
  public void testGetSourcelevelDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSourcelevelDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Sourcelevel.
  */
  @Test
  public void testGetSourcelevelSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSourcelevelSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Sourcecolumn.
   */
  @Test
  public void testGetSourcecolumnColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getSourcecolumnColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Sourcecolumn.
  */
  @Test
  public void testGetSourcecolumnDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSourcecolumnDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Sourcecolumn.
  */
  @Test
  public void testGetSourcecolumnSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSourcecolumnSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Targettable.
   */
  @Test
  public void testGetTargettableColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getTargettableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Targettable.
  */
  @Test
  public void testGetTargettableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTargettableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Targettable.
  */
  @Test
  public void testGetTargettableSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTargettableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Targetlevel.
   */
  @Test
  public void testGetTargetlevelColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getTargetlevelColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Targetlevel.
  */
  @Test
  public void testGetTargetlevelDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTargetlevelDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Targetlevel.
  */
  @Test
  public void testGetTargetlevelSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTargetlevelSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Targetcolumn.
   */
  @Test
  public void testGetTargetcolumnColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getTargetcolumnColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Targetcolumn.
  */
  @Test
  public void testGetTargetcolumnDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTargetcolumnDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Targetcolumn.
  */
  @Test
  public void testGetTargetcolumnSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTargetcolumnSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Expression.
   */
  @Test
  public void testGetExpressionColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getExpressionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Expression.
  */
  @Test
  public void testGetExpressionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getExpressionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Expression.
  */
  @Test
  public void testGetExpressionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getExpressionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Cardinality.
   */
  @Test
  public void testGetCardinalityColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getCardinalityColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Cardinality.
  */
  @Test
  public void testGetCardinalityDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCardinalityDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Cardinality.
  */
  @Test
  public void testGetCardinalitySQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getCardinalitySQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Context.
   */
  @Test
  public void testGetContextColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getContextColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Context.
  */
  @Test
  public void testGetContextDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getContextDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Context.
  */
  @Test
  public void testGetContextSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getContextSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Excludedcontexts.
   */
  @Test
  public void testGetExcludedcontextsColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getExcludedcontextsColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Excludedcontexts.
  */
  @Test
  public void testGetExcludedcontextsDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getExcludedcontextsDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Excludedcontexts.
  */
  @Test
  public void testGetExcludedcontextsSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getExcludedcontextsSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Tmpcounter.
   */
  @Test
  public void testGetTmpcounterColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getTmpcounterColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Tmpcounter.
  */
  @Test
  public void testGetTmpcounterDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTmpcounterDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Tmpcounter.
  */
  @Test
  public void testGetTmpcounterSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getTmpcounterSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Ordernro.
   */
  @Test
  public void testGetOrdernroColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getOrdernroColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Ordernro.
  */
  @Test
  public void testGetOrdernroDecimalDigits() throws Exception {
    
     assertEquals(6, objUnderTest.getOrdernroDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Ordernro.
  */
  @Test
  public void testGetOrdernroSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getOrdernroSQLType());    
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
   * Testing original Universejoin object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Universejoin(rockFactory, false);
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
    Universejoin changedOriginal = new Universejoin(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Universejoin(rockFactory, false);
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
    Universejoin changedOriginal = new Universejoin(rockFactory, false);
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