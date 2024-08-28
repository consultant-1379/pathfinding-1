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
 * Test class for Versioning. Changes to Versioning table are made via
 * this class.
 */
public class VersioningTest {

  private static Versioning objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSIONID;
  
  private static Field DESCRIPTION;
  
  private static Field STATUS;
  
  private static Field TECHPACK_NAME;
  
  private static Field TECHPACK_VERSION;
  
  private static Field TECHPACK_TYPE;
  
  private static Field PRODUCT_NUMBER;
  
  private static Field LOCKEDBY;
  
  private static Field LOCKDATE;
  
  private static Field BASEDEFINITION;
  
  private static Field BASEVERSION;
  
  private static Field INSTALLDESCRIPTION;
  
  private static Field UNIVERSENAME;
  
  private static Field UNIVERSEEXTENSION;
  
  private static Field ENIQ_LEVEL;
  
  private static Field LICENSENAME;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Versioning.class.getDeclaredField("newItem");
		VERSIONID = Versioning.class.getDeclaredField("VERSIONID");
		DESCRIPTION = Versioning.class.getDeclaredField("DESCRIPTION");
		STATUS = Versioning.class.getDeclaredField("STATUS");
		TECHPACK_NAME = Versioning.class.getDeclaredField("TECHPACK_NAME");
		TECHPACK_VERSION = Versioning.class.getDeclaredField("TECHPACK_VERSION");
		TECHPACK_TYPE = Versioning.class.getDeclaredField("TECHPACK_TYPE");
		PRODUCT_NUMBER = Versioning.class.getDeclaredField("PRODUCT_NUMBER");
		LOCKEDBY = Versioning.class.getDeclaredField("LOCKEDBY");
		LOCKDATE = Versioning.class.getDeclaredField("LOCKDATE");
		BASEDEFINITION = Versioning.class.getDeclaredField("BASEDEFINITION");
		BASEVERSION = Versioning.class.getDeclaredField("BASEVERSION");
		INSTALLDESCRIPTION = Versioning.class.getDeclaredField("INSTALLDESCRIPTION");
		UNIVERSENAME = Versioning.class.getDeclaredField("UNIVERSENAME");
		UNIVERSEEXTENSION = Versioning.class.getDeclaredField("UNIVERSEEXTENSION");
		ENIQ_LEVEL = Versioning.class.getDeclaredField("ENIQ_LEVEL");
		LICENSENAME = Versioning.class.getDeclaredField("LICENSENAME");
		newItem.setAccessible(true);
		VERSIONID.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		STATUS.setAccessible(true);
		TECHPACK_NAME.setAccessible(true);
		TECHPACK_VERSION.setAccessible(true);
		TECHPACK_TYPE.setAccessible(true);
		PRODUCT_NUMBER.setAccessible(true);
		LOCKEDBY.setAccessible(true);
		LOCKDATE.setAccessible(true);
		BASEDEFINITION.setAccessible(true);
		BASEVERSION.setAccessible(true);
		INSTALLDESCRIPTION.setAccessible(true);
		UNIVERSENAME.setAccessible(true);
		UNIVERSEEXTENSION.setAccessible(true);
		ENIQ_LEVEL.setAccessible(true);
		LICENSENAME.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Versioning ( VERSIONID VARCHAR(31)  ,DESCRIPTION VARCHAR(31) ,STATUS BIGINT  ,TECHPACK_NAME VARCHAR(31) ,TECHPACK_VERSION VARCHAR(31) ,TECHPACK_TYPE VARCHAR(31) ,PRODUCT_NUMBER VARCHAR(31) ,LOCKEDBY VARCHAR(31) ,LOCKDATE TIMESTAMP  ,BASEDEFINITION VARCHAR(31) ,BASEVERSION VARCHAR(31) ,INSTALLDESCRIPTION VARCHAR(31) ,UNIVERSENAME VARCHAR(31) ,UNIVERSEEXTENSION VARCHAR(31) ,ENIQ_LEVEL VARCHAR(31) ,LICENSENAME VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Versioning");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Versioning VALUES( 'testVERSIONID'  ,'testDESCRIPTION'  ,1  ,'testTECHPACK_NAME'  ,'testTECHPACK_VERSION'  ,'testTECHPACK_TYPE'  ,'testPRODUCT_NUMBER'  ,'testLOCKEDBY'  ,'2000-01-01 00:00:00.0'  ,'testBASEDEFINITION'  ,'testBASEVERSION'  ,'testINSTALLDESCRIPTION'  ,'testUNIVERSENAME'  ,'testUNIVERSEEXTENSION'  ,'testENIQ_LEVEL'  ,'testLICENSENAME' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Versioning(rockFactory ,  "testVERSIONID");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Versioning");
    objUnderTest = null;
  }
  
  /**
   * Testing Versioning constructor variable initialization with null values.
   */
  @Test
  public void testVersioningConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Versioning(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + TECHPACK_NAME.get(objUnderTest)  + ", " + TECHPACK_VERSION.get(objUnderTest)  + ", " + TECHPACK_TYPE.get(objUnderTest)  + ", " + PRODUCT_NUMBER.get(objUnderTest)  + ", " + LOCKEDBY.get(objUnderTest)  + ", " + LOCKDATE.get(objUnderTest)  + ", " + BASEDEFINITION.get(objUnderTest)  + ", " + BASEVERSION.get(objUnderTest)  + ", " + INSTALLDESCRIPTION.get(objUnderTest)  + ", " + UNIVERSENAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + ENIQ_LEVEL.get(objUnderTest)  + ", " + LICENSENAME.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Versioning constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testVersioningConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Versioning(rockFactory ,  "testVERSIONID");

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + TECHPACK_NAME.get(objUnderTest)  + ", " + TECHPACK_VERSION.get(objUnderTest)  + ", " + TECHPACK_TYPE.get(objUnderTest)  + ", " + PRODUCT_NUMBER.get(objUnderTest)  + ", " + LOCKEDBY.get(objUnderTest)  + ", " + LOCKDATE.get(objUnderTest)  + ", " + BASEDEFINITION.get(objUnderTest)  + ", " + BASEVERSION.get(objUnderTest)  + ", " + INSTALLDESCRIPTION.get(objUnderTest)  + ", " + UNIVERSENAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + ENIQ_LEVEL.get(objUnderTest)  + ", " + LICENSENAME.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testDESCRIPTION"  + ", 1"  + ", testTECHPACK_NAME"  + ", testTECHPACK_VERSION"  + ", testTECHPACK_TYPE"  + ", testPRODUCT_NUMBER"  + ", testLOCKEDBY"  + ", 2000-01-01 00:00:00.0"  + ", testBASEDEFINITION"  + ", testBASEVERSION"  + ", testINSTALLDESCRIPTION"  + ", testUNIVERSENAME"  + ", testUNIVERSEEXTENSION"  + ", testENIQ_LEVEL"  + ", testLICENSENAME" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testVersioningConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Versioning(null ,  "testVERSIONID");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Versioning constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testVersioningConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Versioning whereObject = new Versioning(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Versioning(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + TECHPACK_NAME.get(objUnderTest)  + ", " + TECHPACK_VERSION.get(objUnderTest)  + ", " + TECHPACK_TYPE.get(objUnderTest)  + ", " + PRODUCT_NUMBER.get(objUnderTest)  + ", " + LOCKEDBY.get(objUnderTest)  + ", " + LOCKDATE.get(objUnderTest)  + ", " + BASEDEFINITION.get(objUnderTest)  + ", " + BASEVERSION.get(objUnderTest)  + ", " + INSTALLDESCRIPTION.get(objUnderTest)  + ", " + UNIVERSENAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + ENIQ_LEVEL.get(objUnderTest)  + ", " + LICENSENAME.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testDESCRIPTION"  + ", 1"  + ", testTECHPACK_NAME"  + ", testTECHPACK_VERSION"  + ", testTECHPACK_TYPE"  + ", testPRODUCT_NUMBER"  + ", testLOCKEDBY"  + ", 2000-01-01 00:00:00.0"  + ", testBASEDEFINITION"  + ", testBASEVERSION"  + ", testINSTALLDESCRIPTION"  + ", testUNIVERSENAME"  + ", testUNIVERSEEXTENSION"  + ", testENIQ_LEVEL"  + ", testLICENSENAME" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testVersioningConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Versioning whereObject = new Versioning(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Versioning(null, whereObject);
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
    assertEquals("Versioning", objUnderTest.getTableName());
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
    Versioning whereObject = new Versioning(rockFactory);

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
    Versioning whereObject = new Versioning(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Versioning");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Versioning");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSIONID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSIONID FROM Versioning");
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
    
    String expected = "<Versioning VERSIONID=\"'testVERSIONID'\" DESCRIPTION=\"'testDESCRIPTION'\" STATUS=\"1\" TECHPACK_NAME=\"'testTECHPACK_NAME'\" TECHPACK_VERSION=\"'testTECHPACK_VERSION'\" TECHPACK_TYPE=\"'testTECHPACK_TYPE'\" PRODUCT_NUMBER=\"'testPRODUCT_NUMBER'\" LOCKEDBY=\"'testLOCKEDBY'\" LOCKDATE=\"'2000-01-01 00:00:00.0'\" BASEDEFINITION=\"'testBASEDEFINITION'\" BASEVERSION=\"'testBASEVERSION'\" INSTALLDESCRIPTION=\"'testINSTALLDESCRIPTION'\" UNIVERSENAME=\"'testUNIVERSENAME'\" UNIVERSEEXTENSION=\"'testUNIVERSEEXTENSION'\" ENIQ_LEVEL=\"'testENIQ_LEVEL'\" LICENSENAME=\"'testLICENSENAME'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Versioning VERSIONID=\"'testVERSIONID'\" DESCRIPTION=\"'testDESCRIPTION'\" STATUS=\"1\" TECHPACK_NAME=\"'testTECHPACK_NAME'\" TECHPACK_VERSION=\"'testTECHPACK_VERSION'\" TECHPACK_TYPE=\"'testTECHPACK_TYPE'\" PRODUCT_NUMBER=\"'testPRODUCT_NUMBER'\" LOCKEDBY=\"'testLOCKEDBY'\" LOCKDATE=\"'2000-01-01 00:00:00.0'\" BASEDEFINITION=\"'testBASEDEFINITION'\" BASEVERSION=\"'testBASEVERSION'\" INSTALLDESCRIPTION=\"'testINSTALLDESCRIPTION'\" UNIVERSENAME=\"'testUNIVERSENAME'\" UNIVERSEEXTENSION=\"'testUNIVERSEEXTENSION'\" ENIQ_LEVEL=\"'testENIQ_LEVEL'\" LICENSENAME=\"'testLICENSENAME'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Versioning>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Versioning ( VERSIONID, DESCRIPTION, STATUS, TECHPACK_NAME, TECHPACK_VERSION, TECHPACK_TYPE, PRODUCT_NUMBER, LOCKEDBY, LOCKDATE, BASEDEFINITION, BASEVERSION, INSTALLDESCRIPTION, UNIVERSENAME, UNIVERSEEXTENSION, ENIQ_LEVEL, LICENSENAME ) values "
      + "( 'testVERSIONID', 'testDESCRIPTION', 1, 'testTECHPACK_NAME', 'testTECHPACK_VERSION', 'testTECHPACK_TYPE', 'testPRODUCT_NUMBER', 'testLOCKEDBY', '2000-01-01 00:00:00.0', 'testBASEDEFINITION', 'testBASEVERSION', 'testINSTALLDESCRIPTION', 'testUNIVERSENAME', 'testUNIVERSEEXTENSION', 'testENIQ_LEVEL', 'testLICENSENAME' );\n";
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
    objUnderTest.setVersionid(VersioningTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(VersioningTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(VersioningTest.testStringGenerator("anotherDESCRIPTION", 50));
    assertEquals(VersioningTest.testStringGenerator("anotherDESCRIPTION", 50), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatus() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatus(555L);
    assertEquals(555L, STATUS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTechpack_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTechpack_name(VersioningTest.testStringGenerator("anotherTECHPACK_NAME", 30));
    assertEquals(VersioningTest.testStringGenerator("anotherTECHPACK_NAME", 30), TECHPACK_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTechpack_version() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTechpack_version(VersioningTest.testStringGenerator("anotherTECHPACK_VERSION", 32));
    assertEquals(VersioningTest.testStringGenerator("anotherTECHPACK_VERSION", 32), TECHPACK_VERSION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTechpack_type() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTechpack_type(VersioningTest.testStringGenerator("anotherTECHPACK_TYPE", 10));
    assertEquals(VersioningTest.testStringGenerator("anotherTECHPACK_TYPE", 10), TECHPACK_TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetProduct_number() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setProduct_number(VersioningTest.testStringGenerator("anotherPRODUCT_NUMBER", 255));
    assertEquals(VersioningTest.testStringGenerator("anotherPRODUCT_NUMBER", 255), PRODUCT_NUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLockedby() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLockedby(VersioningTest.testStringGenerator("anotherLOCKEDBY", 255));
    assertEquals(VersioningTest.testStringGenerator("anotherLOCKEDBY", 255), LOCKEDBY.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLockdate() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLockdate(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), LOCKDATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBasedefinition() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBasedefinition(VersioningTest.testStringGenerator("anotherBASEDEFINITION", 128));
    assertEquals(VersioningTest.testStringGenerator("anotherBASEDEFINITION", 128), BASEDEFINITION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBaseversion() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBaseversion(VersioningTest.testStringGenerator("anotherBASEVERSION", 16));
    assertEquals(VersioningTest.testStringGenerator("anotherBASEVERSION", 16), BASEVERSION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetInstalldescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInstalldescription(VersioningTest.testStringGenerator("anotherINSTALLDESCRIPTION", 32000));
    assertEquals(VersioningTest.testStringGenerator("anotherINSTALLDESCRIPTION", 32000), INSTALLDESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniversename() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniversename(VersioningTest.testStringGenerator("anotherUNIVERSENAME", 30));
    assertEquals(VersioningTest.testStringGenerator("anotherUNIVERSENAME", 30), UNIVERSENAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniverseextension() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniverseextension(VersioningTest.testStringGenerator("anotherUNIVERSEEXTENSION", 16));
    assertEquals(VersioningTest.testStringGenerator("anotherUNIVERSEEXTENSION", 16), UNIVERSEEXTENSION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetEniq_level() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setEniq_level(VersioningTest.testStringGenerator("anotherENIQ_LEVEL", 12));
    assertEquals(VersioningTest.testStringGenerator("anotherENIQ_LEVEL", 12), ENIQ_LEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLicensename() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLicensename(VersioningTest.testStringGenerator("anotherLICENSENAME", 1023));
    assertEquals(VersioningTest.testStringGenerator("anotherLICENSENAME", 1023), LICENSENAME.get(objUnderTest));
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
    String[] expectedKeys = { "VERSIONID"};

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
    objUnderTest = new Versioning(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSIONID.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + TECHPACK_NAME.get(objUnderTest)  + ", " + TECHPACK_VERSION.get(objUnderTest)  + ", " + TECHPACK_TYPE.get(objUnderTest)  + ", " + PRODUCT_NUMBER.get(objUnderTest)  + ", " + LOCKEDBY.get(objUnderTest)  + ", " + LOCKDATE.get(objUnderTest)  + ", " + BASEDEFINITION.get(objUnderTest)  + ", " + BASEVERSION.get(objUnderTest)  + ", " + INSTALLDESCRIPTION.get(objUnderTest)  + ", " + UNIVERSENAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + ENIQ_LEVEL.get(objUnderTest)  + ", " + LICENSENAME.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", 0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", " + new Timestamp(0)  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", " ;
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
    Versioning compareObj = new Versioning(rockFactory ,  "testVERSIONID");

    /* Testing first with null primary key value */
    VERSIONID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSIONID.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSIONID.set(objUnderTest,  "testVERSIONID");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Versioning with our current one. If the two
   * Versionings are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnVersioning() throws Exception {

    /* Creating another Versioning which will be compared to the tested one */
    Versioning comparedObj = new Versioning(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Versioning with our current one. If the two
   * Versionings are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentVersioning() throws Exception {

    /* Creating another Versioning which will be compared to the tested one */
    Versioning comparedObj = new Versioning(rockFactory ,  "testVERSIONID");
    comparedObj.setVersionid( "DifferentVERSIONID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Versioning with our current one. If the two
   * Versionings are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameVersioning() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Versioning comparedObj = new Versioning(rockFactory ,  "testVERSIONID");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Versioning with our current one using null value.
   */
  @Test
  public void testEqualsWithNullVersioning() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Versioning comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Versioning was null \n");
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
    assertEquals(Versioning.class, actualObject.getClass());
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
    Versioning testAgg = new Versioning(rockFactory ,  "testVERSIONID");
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
   * Testing columnsize retrieving for Description.
   */
  @Test
  public void testGetDescriptionColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getDescriptionColumnSize());   
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
   * Testing columnsize retrieving for Status.
   */
  @Test
  public void testGetStatusColumnSize() throws Exception {
    
     assertEquals(9, objUnderTest.getStatusColumnSize());   
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
    
    assertEquals(2, objUnderTest.getStatusSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Techpack_name.
   */
  @Test
  public void testGetTechpack_nameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getTechpack_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Techpack_name.
  */
  @Test
  public void testGetTechpack_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTechpack_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Techpack_name.
  */
  @Test
  public void testGetTechpack_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTechpack_nameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Techpack_version.
   */
  @Test
  public void testGetTechpack_versionColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getTechpack_versionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Techpack_version.
  */
  @Test
  public void testGetTechpack_versionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTechpack_versionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Techpack_version.
  */
  @Test
  public void testGetTechpack_versionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTechpack_versionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Techpack_type.
   */
  @Test
  public void testGetTechpack_typeColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getTechpack_typeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Techpack_type.
  */
  @Test
  public void testGetTechpack_typeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTechpack_typeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Techpack_type.
  */
  @Test
  public void testGetTechpack_typeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTechpack_typeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Product_number.
   */
  @Test
  public void testGetProduct_numberColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getProduct_numberColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Product_number.
  */
  @Test
  public void testGetProduct_numberDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getProduct_numberDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Product_number.
  */
  @Test
  public void testGetProduct_numberSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getProduct_numberSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Lockedby.
   */
  @Test
  public void testGetLockedbyColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getLockedbyColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Lockedby.
  */
  @Test
  public void testGetLockedbyDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLockedbyDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Lockedby.
  */
  @Test
  public void testGetLockedbySQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getLockedbySQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Lockdate.
   */
  @Test
  public void testGetLockdateColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getLockdateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Lockdate.
  */
  @Test
  public void testGetLockdateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLockdateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Lockdate.
  */
  @Test
  public void testGetLockdateSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getLockdateSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Basedefinition.
   */
  @Test
  public void testGetBasedefinitionColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getBasedefinitionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Basedefinition.
  */
  @Test
  public void testGetBasedefinitionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBasedefinitionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Basedefinition.
  */
  @Test
  public void testGetBasedefinitionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBasedefinitionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Baseversion.
   */
  @Test
  public void testGetBaseversionColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getBaseversionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Baseversion.
  */
  @Test
  public void testGetBaseversionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBaseversionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Baseversion.
  */
  @Test
  public void testGetBaseversionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBaseversionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Installdescription.
   */
  @Test
  public void testGetInstalldescriptionColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getInstalldescriptionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Installdescription.
  */
  @Test
  public void testGetInstalldescriptionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getInstalldescriptionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Installdescription.
  */
  @Test
  public void testGetInstalldescriptionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getInstalldescriptionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Universename.
   */
  @Test
  public void testGetUniversenameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getUniversenameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Universename.
  */
  @Test
  public void testGetUniversenameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUniversenameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Universename.
  */
  @Test
  public void testGetUniversenameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUniversenameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Universeextension.
   */
  @Test
  public void testGetUniverseextensionColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getUniverseextensionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Universeextension.
  */
  @Test
  public void testGetUniverseextensionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUniverseextensionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Universeextension.
  */
  @Test
  public void testGetUniverseextensionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUniverseextensionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Eniq_level.
   */
  @Test
  public void testGetEniq_levelColumnSize() throws Exception {
    
     assertEquals(12, objUnderTest.getEniq_levelColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Eniq_level.
  */
  @Test
  public void testGetEniq_levelDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getEniq_levelDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Eniq_level.
  */
  @Test
  public void testGetEniq_levelSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getEniq_levelSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Licensename.
   */
  @Test
  public void testGetLicensenameColumnSize() throws Exception {
    
     assertEquals(1023, objUnderTest.getLicensenameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Licensename.
  */
  @Test
  public void testGetLicensenameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLicensenameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Licensename.
  */
  @Test
  public void testGetLicensenameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getLicensenameSQLType());    
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
   * Testing original Versioning object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Versioning(rockFactory, false);
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
    Versioning changedOriginal = new Versioning(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Versioning(rockFactory, false);
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
    Versioning changedOriginal = new Versioning(rockFactory, false);
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