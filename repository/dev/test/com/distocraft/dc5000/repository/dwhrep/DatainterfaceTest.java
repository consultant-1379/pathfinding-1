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
 * Test class for Datainterface. Changes to Datainterface table are made via
 * this class.
 */
public class DatainterfaceTest {

  private static Datainterface objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field INTERFACENAME;
  
  private static Field STATUS;
  
  private static Field INTERFACETYPE;
  
  private static Field DESCRIPTION;
  
  private static Field DATAFORMATTYPE;
  
  private static Field INTERFACEVERSION;
  
  private static Field LOCKEDBY;
  
  private static Field LOCKDATE;
  
  private static Field PRODUCTNUMBER;
  
  private static Field ENIQ_LEVEL;
  
  private static Field RSTATE;
  
  private static Field INSTALLDESCRIPTION;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Datainterface.class.getDeclaredField("newItem");
		INTERFACENAME = Datainterface.class.getDeclaredField("INTERFACENAME");
		STATUS = Datainterface.class.getDeclaredField("STATUS");
		INTERFACETYPE = Datainterface.class.getDeclaredField("INTERFACETYPE");
		DESCRIPTION = Datainterface.class.getDeclaredField("DESCRIPTION");
		DATAFORMATTYPE = Datainterface.class.getDeclaredField("DATAFORMATTYPE");
		INTERFACEVERSION = Datainterface.class.getDeclaredField("INTERFACEVERSION");
		LOCKEDBY = Datainterface.class.getDeclaredField("LOCKEDBY");
		LOCKDATE = Datainterface.class.getDeclaredField("LOCKDATE");
		PRODUCTNUMBER = Datainterface.class.getDeclaredField("PRODUCTNUMBER");
		ENIQ_LEVEL = Datainterface.class.getDeclaredField("ENIQ_LEVEL");
		RSTATE = Datainterface.class.getDeclaredField("RSTATE");
		INSTALLDESCRIPTION = Datainterface.class.getDeclaredField("INSTALLDESCRIPTION");
		newItem.setAccessible(true);
		INTERFACENAME.setAccessible(true);
		STATUS.setAccessible(true);
		INTERFACETYPE.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		DATAFORMATTYPE.setAccessible(true);
		INTERFACEVERSION.setAccessible(true);
		LOCKEDBY.setAccessible(true);
		LOCKDATE.setAccessible(true);
		PRODUCTNUMBER.setAccessible(true);
		ENIQ_LEVEL.setAccessible(true);
		RSTATE.setAccessible(true);
		INSTALLDESCRIPTION.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Datainterface ( INTERFACENAME VARCHAR(31)  ,STATUS BIGINT  ,INTERFACETYPE VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,DATAFORMATTYPE VARCHAR(31) ,INTERFACEVERSION VARCHAR(31) ,LOCKEDBY VARCHAR(31) ,LOCKDATE TIMESTAMP  ,PRODUCTNUMBER VARCHAR(31) ,ENIQ_LEVEL VARCHAR(31) ,RSTATE VARCHAR(31) ,INSTALLDESCRIPTION VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Datainterface");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Datainterface VALUES( 'testINTERFACENAME'  ,1  ,'testINTERFACETYPE'  ,'testDESCRIPTION'  ,'testDATAFORMATTYPE'  ,'testINTERFACEVERSION'  ,'testLOCKEDBY'  ,'2000-01-01 00:00:00.0'  ,'testPRODUCTNUMBER'  ,'testENIQ_LEVEL'  ,'testRSTATE'  ,'testINSTALLDESCRIPTION' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Datainterface(rockFactory ,  "testINTERFACENAME",  "testINTERFACEVERSION");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Datainterface");
    objUnderTest = null;
  }
  
  /**
   * Testing Datainterface constructor variable initialization with null values.
   */
  @Test
  public void testDatainterfaceConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Datainterface(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  INTERFACENAME.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + INTERFACETYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + DATAFORMATTYPE.get(objUnderTest)  + ", " + INTERFACEVERSION.get(objUnderTest)  + ", " + LOCKEDBY.get(objUnderTest)  + ", " + LOCKDATE.get(objUnderTest)  + ", " + PRODUCTNUMBER.get(objUnderTest)  + ", " + ENIQ_LEVEL.get(objUnderTest)  + ", " + RSTATE.get(objUnderTest)  + ", " + INSTALLDESCRIPTION.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Datainterface constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDatainterfaceConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Datainterface(rockFactory ,  "testINTERFACENAME",  "testINTERFACEVERSION");

    /* Asserting that variables are initialized */
    String actual =  INTERFACENAME.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + INTERFACETYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + DATAFORMATTYPE.get(objUnderTest)  + ", " + INTERFACEVERSION.get(objUnderTest)  + ", " + LOCKEDBY.get(objUnderTest)  + ", " + LOCKDATE.get(objUnderTest)  + ", " + PRODUCTNUMBER.get(objUnderTest)  + ", " + ENIQ_LEVEL.get(objUnderTest)  + ", " + RSTATE.get(objUnderTest)  + ", " + INSTALLDESCRIPTION.get(objUnderTest) ;
    String expected =  "testINTERFACENAME"  + ", 1"  + ", testINTERFACETYPE"  + ", testDESCRIPTION"  + ", testDATAFORMATTYPE"  + ", testINTERFACEVERSION"  + ", testLOCKEDBY"  + ", 2000-01-01 00:00:00.0"  + ", testPRODUCTNUMBER"  + ", testENIQ_LEVEL"  + ", testRSTATE"  + ", testINSTALLDESCRIPTION" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDatainterfaceConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Datainterface(null ,  "testINTERFACENAME",  "testINTERFACEVERSION");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Datainterface constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDatainterfaceConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Datainterface whereObject = new Datainterface(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Datainterface(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  INTERFACENAME.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + INTERFACETYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + DATAFORMATTYPE.get(objUnderTest)  + ", " + INTERFACEVERSION.get(objUnderTest)  + ", " + LOCKEDBY.get(objUnderTest)  + ", " + LOCKDATE.get(objUnderTest)  + ", " + PRODUCTNUMBER.get(objUnderTest)  + ", " + ENIQ_LEVEL.get(objUnderTest)  + ", " + RSTATE.get(objUnderTest)  + ", " + INSTALLDESCRIPTION.get(objUnderTest) ;
    String expected =  "testINTERFACENAME"  + ", 1"  + ", testINTERFACETYPE"  + ", testDESCRIPTION"  + ", testDATAFORMATTYPE"  + ", testINTERFACEVERSION"  + ", testLOCKEDBY"  + ", 2000-01-01 00:00:00.0"  + ", testPRODUCTNUMBER"  + ", testENIQ_LEVEL"  + ", testRSTATE"  + ", testINSTALLDESCRIPTION" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDatainterfaceConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Datainterface whereObject = new Datainterface(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Datainterface(null, whereObject);
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
    assertEquals("Datainterface", objUnderTest.getTableName());
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
    Datainterface whereObject = new Datainterface(rockFactory);

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
    Datainterface whereObject = new Datainterface(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Datainterface");
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
    INTERFACENAME.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("INTERFACENAME");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Datainterface");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the INTERFACENAME column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT INTERFACENAME FROM Datainterface");
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
    
    String expected = "<Datainterface INTERFACENAME=\"'testINTERFACENAME'\" STATUS=\"1\" INTERFACETYPE=\"'testINTERFACETYPE'\" DESCRIPTION=\"'testDESCRIPTION'\" DATAFORMATTYPE=\"'testDATAFORMATTYPE'\" INTERFACEVERSION=\"'testINTERFACEVERSION'\" LOCKEDBY=\"'testLOCKEDBY'\" LOCKDATE=\"'2000-01-01 00:00:00.0'\" PRODUCTNUMBER=\"'testPRODUCTNUMBER'\" ENIQ_LEVEL=\"'testENIQ_LEVEL'\" RSTATE=\"'testRSTATE'\" INSTALLDESCRIPTION=\"'testINSTALLDESCRIPTION'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Datainterface INTERFACENAME=\"'testINTERFACENAME'\" STATUS=\"1\" INTERFACETYPE=\"'testINTERFACETYPE'\" DESCRIPTION=\"'testDESCRIPTION'\" DATAFORMATTYPE=\"'testDATAFORMATTYPE'\" INTERFACEVERSION=\"'testINTERFACEVERSION'\" LOCKEDBY=\"'testLOCKEDBY'\" LOCKDATE=\"'2000-01-01 00:00:00.0'\" PRODUCTNUMBER=\"'testPRODUCTNUMBER'\" ENIQ_LEVEL=\"'testENIQ_LEVEL'\" RSTATE=\"'testRSTATE'\" INSTALLDESCRIPTION=\"'testINSTALLDESCRIPTION'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Datainterface>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Datainterface ( INTERFACENAME, STATUS, INTERFACETYPE, DESCRIPTION, DATAFORMATTYPE, INTERFACEVERSION, LOCKEDBY, LOCKDATE, PRODUCTNUMBER, ENIQ_LEVEL, RSTATE, INSTALLDESCRIPTION ) values "
      + "( 'testINTERFACENAME', 1, 'testINTERFACETYPE', 'testDESCRIPTION', 'testDATAFORMATTYPE', 'testINTERFACEVERSION', 'testLOCKEDBY', '2000-01-01 00:00:00.0', 'testPRODUCTNUMBER', 'testENIQ_LEVEL', 'testRSTATE', 'testINSTALLDESCRIPTION' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetInterfacename() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInterfacename(DatainterfaceTest.testStringGenerator("anotherINTERFACENAME", 255));
    assertEquals(DatainterfaceTest.testStringGenerator("anotherINTERFACENAME", 255), INTERFACENAME.get(objUnderTest));
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
  public void testSetAndGetInterfacetype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInterfacetype(DatainterfaceTest.testStringGenerator("anotherINTERFACETYPE", 50));
    assertEquals(DatainterfaceTest.testStringGenerator("anotherINTERFACETYPE", 50), INTERFACETYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(DatainterfaceTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(DatainterfaceTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataformattype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataformattype(DatainterfaceTest.testStringGenerator("anotherDATAFORMATTYPE", 50));
    assertEquals(DatainterfaceTest.testStringGenerator("anotherDATAFORMATTYPE", 50), DATAFORMATTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetInterfaceversion() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInterfaceversion(DatainterfaceTest.testStringGenerator("anotherINTERFACEVERSION", 32));
    assertEquals(DatainterfaceTest.testStringGenerator("anotherINTERFACEVERSION", 32), INTERFACEVERSION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLockedby() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLockedby(DatainterfaceTest.testStringGenerator("anotherLOCKEDBY", 255));
    assertEquals(DatainterfaceTest.testStringGenerator("anotherLOCKEDBY", 255), LOCKEDBY.get(objUnderTest));
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
  public void testSetAndGetProductnumber() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setProductnumber(DatainterfaceTest.testStringGenerator("anotherPRODUCTNUMBER", 255));
    assertEquals(DatainterfaceTest.testStringGenerator("anotherPRODUCTNUMBER", 255), PRODUCTNUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetEniq_level() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setEniq_level(DatainterfaceTest.testStringGenerator("anotherENIQ_LEVEL", 12));
    assertEquals(DatainterfaceTest.testStringGenerator("anotherENIQ_LEVEL", 12), ENIQ_LEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetRstate() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setRstate(DatainterfaceTest.testStringGenerator("anotherRSTATE", 255));
    assertEquals(DatainterfaceTest.testStringGenerator("anotherRSTATE", 255), RSTATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetInstalldescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInstalldescription(DatainterfaceTest.testStringGenerator("anotherINSTALLDESCRIPTION", 32000));
    assertEquals(DatainterfaceTest.testStringGenerator("anotherINSTALLDESCRIPTION", 32000), INSTALLDESCRIPTION.get(objUnderTest));
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
    String[] expectedKeys = { "INTERFACENAME","INTERFACEVERSION"};

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
    objUnderTest = new Datainterface(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  INTERFACENAME.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + INTERFACETYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + DATAFORMATTYPE.get(objUnderTest)  + ", " + INTERFACEVERSION.get(objUnderTest)  + ", " + LOCKEDBY.get(objUnderTest)  + ", " + LOCKDATE.get(objUnderTest)  + ", " + PRODUCTNUMBER.get(objUnderTest)  + ", " + ENIQ_LEVEL.get(objUnderTest)  + ", " + RSTATE.get(objUnderTest)  + ", " + INSTALLDESCRIPTION.get(objUnderTest) ;
    String expected =  ""  + ", 0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", " + new Timestamp(0)  + ", "  + ", "  + ", "  + ", " ;
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
    Datainterface compareObj = new Datainterface(rockFactory ,  "testINTERFACENAME",  "testINTERFACEVERSION");

    /* Testing first with null primary key value */
    INTERFACENAME.set(objUnderTest, null);
  	INTERFACEVERSION.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    INTERFACENAME.set(objUnderTest,  "different");
  	INTERFACEVERSION.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    INTERFACENAME.set(objUnderTest,  "testINTERFACENAME");
  	INTERFACEVERSION.set(objUnderTest,  "testINTERFACEVERSION");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Datainterface with our current one. If the two
   * Datainterfaces are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnDatainterface() throws Exception {

    /* Creating another Datainterface which will be compared to the tested one */
    Datainterface comparedObj = new Datainterface(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Datainterface with our current one. If the two
   * Datainterfaces are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentDatainterface() throws Exception {

    /* Creating another Datainterface which will be compared to the tested one */
    Datainterface comparedObj = new Datainterface(rockFactory ,  "testINTERFACENAME",  "testINTERFACEVERSION");
    comparedObj.setInterfacename( "DifferentINTERFACENAME");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Datainterface with our current one. If the two
   * Datainterfaces are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameDatainterface() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Datainterface comparedObj = new Datainterface(rockFactory ,  "testINTERFACENAME",  "testINTERFACEVERSION");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Datainterface with our current one using null value.
   */
  @Test
  public void testEqualsWithNullDatainterface() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Datainterface comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Datainterface was null \n");
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
    assertEquals(Datainterface.class, actualObject.getClass());
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
    Datainterface testAgg = new Datainterface(rockFactory ,  "testINTERFACENAME",  "testINTERFACEVERSION");
    INTERFACENAME.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Interfacename.
   */
  @Test
  public void testGetInterfacenameColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getInterfacenameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Interfacename.
  */
  @Test
  public void testGetInterfacenameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getInterfacenameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Interfacename.
  */
  @Test
  public void testGetInterfacenameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getInterfacenameSQLType());    
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
   * Testing columnsize retrieving for Interfacetype.
   */
  @Test
  public void testGetInterfacetypeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getInterfacetypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Interfacetype.
  */
  @Test
  public void testGetInterfacetypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getInterfacetypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Interfacetype.
  */
  @Test
  public void testGetInterfacetypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getInterfacetypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Description.
   */
  @Test
  public void testGetDescriptionColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getDescriptionColumnSize());   
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
   * Testing columnsize retrieving for Dataformattype.
   */
  @Test
  public void testGetDataformattypeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getDataformattypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dataformattype.
  */
  @Test
  public void testGetDataformattypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDataformattypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dataformattype.
  */
  @Test
  public void testGetDataformattypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDataformattypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Interfaceversion.
   */
  @Test
  public void testGetInterfaceversionColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getInterfaceversionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Interfaceversion.
  */
  @Test
  public void testGetInterfaceversionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getInterfaceversionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Interfaceversion.
  */
  @Test
  public void testGetInterfaceversionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getInterfaceversionSQLType());    
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
   * Testing columnsize retrieving for Productnumber.
   */
  @Test
  public void testGetProductnumberColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getProductnumberColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Productnumber.
  */
  @Test
  public void testGetProductnumberDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getProductnumberDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Productnumber.
  */
  @Test
  public void testGetProductnumberSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getProductnumberSQLType());    
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
   * Testing columnsize retrieving for Rstate.
   */
  @Test
  public void testGetRstateColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getRstateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Rstate.
  */
  @Test
  public void testGetRstateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getRstateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Rstate.
  */
  @Test
  public void testGetRstateSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getRstateSQLType());    
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
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Datainterface object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Datainterface(rockFactory, false);
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
    Datainterface changedOriginal = new Datainterface(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Datainterface(rockFactory, false);
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
    Datainterface changedOriginal = new Datainterface(rockFactory, false);
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