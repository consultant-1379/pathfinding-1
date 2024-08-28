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
 * Test class for Infomessages. Changes to Infomessages table are made via
 * this class.
 */
public class InfomessagesTest {

  private static Infomessages objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field MSGID;
  
  private static Field TITLE;
  
  private static Field MSGDATE;
  
  private static Field NAME;
  
  private static Field EMAIL;
  
  private static Field STATUS;
  
  private static Field MSG;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Infomessages.class.getDeclaredField("newItem");
		MSGID = Infomessages.class.getDeclaredField("MSGID");
		TITLE = Infomessages.class.getDeclaredField("TITLE");
		MSGDATE = Infomessages.class.getDeclaredField("MSGDATE");
		NAME = Infomessages.class.getDeclaredField("NAME");
		EMAIL = Infomessages.class.getDeclaredField("EMAIL");
		STATUS = Infomessages.class.getDeclaredField("STATUS");
		MSG = Infomessages.class.getDeclaredField("MSG");
		newItem.setAccessible(true);
		MSGID.setAccessible(true);
		TITLE.setAccessible(true);
		MSGDATE.setAccessible(true);
		NAME.setAccessible(true);
		EMAIL.setAccessible(true);
		STATUS.setAccessible(true);
		MSG.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Infomessages ( MSGID INTEGER  ,TITLE VARCHAR(31) ,MSGDATE TIMESTAMP  ,NAME VARCHAR(31) ,EMAIL VARCHAR(31) ,STATUS VARCHAR(31) ,MSG VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Infomessages");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Infomessages VALUES( 1  ,'testTITLE'  ,'2000-01-01 00:00:00.0'  ,'testNAME'  ,'testEMAIL'  ,'testSTATUS'  ,'testMSG' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Infomessages(rockFactory ,  1 );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Infomessages");
    objUnderTest = null;
  }
  
  /**
   * Testing Infomessages constructor variable initialization with null values.
   */
  @Test
  public void testInfomessagesConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Infomessages(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  MSGID.get(objUnderTest)  + ", " + TITLE.get(objUnderTest)  + ", " + MSGDATE.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + EMAIL.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + MSG.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Infomessages constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testInfomessagesConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Infomessages(rockFactory ,  1 );

    /* Asserting that variables are initialized */
    String actual =  MSGID.get(objUnderTest)  + ", " + TITLE.get(objUnderTest)  + ", " + MSGDATE.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + EMAIL.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + MSG.get(objUnderTest) ;
    String expected =  "1"  + ", testTITLE"  + ", 2000-01-01 00:00:00.0"  + ", testNAME"  + ", testEMAIL"  + ", testSTATUS"  + ", testMSG" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testInfomessagesConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Infomessages(null ,  1 );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Infomessages constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testInfomessagesConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Infomessages whereObject = new Infomessages(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Infomessages(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  MSGID.get(objUnderTest)  + ", " + TITLE.get(objUnderTest)  + ", " + MSGDATE.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + EMAIL.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + MSG.get(objUnderTest) ;
    String expected =  "1"  + ", testTITLE"  + ", 2000-01-01 00:00:00.0"  + ", testNAME"  + ", testEMAIL"  + ", testSTATUS"  + ", testMSG" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testInfomessagesConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Infomessages whereObject = new Infomessages(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Infomessages(null, whereObject);
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
    assertEquals("Infomessages", objUnderTest.getTableName());
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
    Infomessages whereObject = new Infomessages(rockFactory);

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
    Infomessages whereObject = new Infomessages(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Infomessages");
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
    MSGID.set(objUnderTest, 1);
    HashSet testSet = new HashSet();
    testSet.add("MSGID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Infomessages");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the MSGID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT MSGID FROM Infomessages");
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
    
    String expected = "<Infomessages MSGID=\"1\" TITLE=\"'testTITLE'\" MSGDATE=\"'2000-01-01 00:00:00.0'\" NAME=\"'testNAME'\" EMAIL=\"'testEMAIL'\" STATUS=\"'testSTATUS'\" MSG=\"'testMSG'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Infomessages MSGID=\"1\" TITLE=\"'testTITLE'\" MSGDATE=\"'2000-01-01 00:00:00.0'\" NAME=\"'testNAME'\" EMAIL=\"'testEMAIL'\" STATUS=\"'testSTATUS'\" MSG=\"'testMSG'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Infomessages>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Infomessages ( MSGID, TITLE, MSGDATE, NAME, EMAIL, STATUS, MSG ) values "
      + "( 1, 'testTITLE', '2000-01-01 00:00:00.0', 'testNAME', 'testEMAIL', 'testSTATUS', 'testMSG' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMsgid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMsgid(555);
    assertEquals(555, MSGID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTitle() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTitle(InfomessagesTest.testStringGenerator("anotherTITLE", 50));
    assertEquals(InfomessagesTest.testStringGenerator("anotherTITLE", 50), TITLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMsgdate() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMsgdate(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), MSGDATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetName() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setName(InfomessagesTest.testStringGenerator("anotherNAME", 50));
    assertEquals(InfomessagesTest.testStringGenerator("anotherNAME", 50), NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetEmail() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setEmail(InfomessagesTest.testStringGenerator("anotherEMAIL", 50));
    assertEquals(InfomessagesTest.testStringGenerator("anotherEMAIL", 50), EMAIL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatus() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatus(InfomessagesTest.testStringGenerator("anotherSTATUS", 20));
    assertEquals(InfomessagesTest.testStringGenerator("anotherSTATUS", 20), STATUS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMsg() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMsg(InfomessagesTest.testStringGenerator("anotherMSG", 500));
    assertEquals(InfomessagesTest.testStringGenerator("anotherMSG", 500), MSG.get(objUnderTest));
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
    String[] expectedKeys = { "MSGID"};

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
    objUnderTest = new Infomessages(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  MSGID.get(objUnderTest)  + ", " + TITLE.get(objUnderTest)  + ", " + MSGDATE.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + EMAIL.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + MSG.get(objUnderTest) ;
    String expected =  "0"  + ", "  + ", " + new Timestamp(0)  + ", "  + ", "  + ", "  + ", " ;
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
    Infomessages compareObj = new Infomessages(rockFactory ,  1 );

    /* Testing first with null primary key value */
    MSGID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    MSGID.set(objUnderTest,  7 );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    MSGID.set(objUnderTest,  1 );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Infomessages with our current one. If the two
   * Infomessagess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnInfomessages() throws Exception {

    /* Creating another Infomessages which will be compared to the tested one */
    Infomessages comparedObj = new Infomessages(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Infomessages with our current one. If the two
   * Infomessagess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentInfomessages() throws Exception {

    /* Creating another Infomessages which will be compared to the tested one */
    Infomessages comparedObj = new Infomessages(rockFactory ,  1 );
    comparedObj.setMsgid( 7 );

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Infomessages with our current one. If the two
   * Infomessagess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameInfomessages() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Infomessages comparedObj = new Infomessages(rockFactory ,  1 );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Infomessages with our current one using null value.
   */
  @Test
  public void testEqualsWithNullInfomessages() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Infomessages comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Infomessages was null \n");
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
    assertEquals(Infomessages.class, actualObject.getClass());
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
    Infomessages testAgg = new Infomessages(rockFactory ,  1 );
    MSGID.set(objUnderTest, 7);

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Msgid.
   */
  @Test
  public void testGetMsgidColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getMsgidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Msgid.
  */
  @Test
  public void testGetMsgidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMsgidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Msgid.
  */
  @Test
  public void testGetMsgidSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getMsgidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Title.
   */
  @Test
  public void testGetTitleColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getTitleColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Title.
  */
  @Test
  public void testGetTitleDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTitleDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Title.
  */
  @Test
  public void testGetTitleSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTitleSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Msgdate.
   */
  @Test
  public void testGetMsgdateColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getMsgdateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Msgdate.
  */
  @Test
  public void testGetMsgdateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMsgdateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Msgdate.
  */
  @Test
  public void testGetMsgdateSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getMsgdateSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Name.
   */
  @Test
  public void testGetNameColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getNameColumnSize());   
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
   * Testing columnsize retrieving for Email.
   */
  @Test
  public void testGetEmailColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getEmailColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Email.
  */
  @Test
  public void testGetEmailDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getEmailDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Email.
  */
  @Test
  public void testGetEmailSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getEmailSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Status.
   */
  @Test
  public void testGetStatusColumnSize() throws Exception {
    
     assertEquals(20, objUnderTest.getStatusColumnSize());   
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
    
    assertEquals(12, objUnderTest.getStatusSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Msg.
   */
  @Test
  public void testGetMsgColumnSize() throws Exception {
    
     assertEquals(500, objUnderTest.getMsgColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Msg.
  */
  @Test
  public void testGetMsgDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMsgDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Msg.
  */
  @Test
  public void testGetMsgSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMsgSQLType());    
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
   * Testing original Infomessages object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Infomessages(rockFactory, false);
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
    Infomessages changedOriginal = new Infomessages(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Infomessages(rockFactory, false);
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
    Infomessages changedOriginal = new Infomessages(rockFactory, false);
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