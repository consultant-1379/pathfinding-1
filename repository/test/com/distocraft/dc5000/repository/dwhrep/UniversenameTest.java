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
 * Test class for Universename. Changes to Universename table are made via
 * this class.
 */
public class UniversenameTest {

  private static Universename objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSIONID;
  
  private static Field UNIVERSENAME;
  
  private static Field UNIVERSEEXTENSION;
  
  private static Field ORDERNRO;
  
  private static Field UNIVERSEEXTENSIONNAME;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Universename.class.getDeclaredField("newItem");
		VERSIONID = Universename.class.getDeclaredField("VERSIONID");
		UNIVERSENAME = Universename.class.getDeclaredField("UNIVERSENAME");
		UNIVERSEEXTENSION = Universename.class.getDeclaredField("UNIVERSEEXTENSION");
		ORDERNRO = Universename.class.getDeclaredField("ORDERNRO");
		UNIVERSEEXTENSIONNAME = Universename.class.getDeclaredField("UNIVERSEEXTENSIONNAME");
		newItem.setAccessible(true);
		VERSIONID.setAccessible(true);
		UNIVERSENAME.setAccessible(true);
		UNIVERSEEXTENSION.setAccessible(true);
		ORDERNRO.setAccessible(true);
		UNIVERSEEXTENSIONNAME.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Universename ( VERSIONID VARCHAR(31)  ,UNIVERSENAME VARCHAR(31) ,UNIVERSEEXTENSION VARCHAR(31) ,ORDERNRO BIGINT  ,UNIVERSEEXTENSIONNAME VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Universename");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Universename VALUES( 'testVERSIONID'  ,'testUNIVERSENAME'  ,'testUNIVERSEEXTENSION'  ,1  ,'testUNIVERSEEXTENSIONNAME' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Universename(rockFactory ,  "testVERSIONID",  "testUNIVERSEEXTENSION");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Universename");
    objUnderTest = null;
  }
  
  /**
   * Testing Universename constructor variable initialization with null values.
   */
  @Test
  public void testUniversenameConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Universename(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + UNIVERSENAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest)  + ", " + UNIVERSEEXTENSIONNAME.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Universename constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testUniversenameConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Universename(rockFactory ,  "testVERSIONID",  "testUNIVERSEEXTENSION");

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + UNIVERSENAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest)  + ", " + UNIVERSEEXTENSIONNAME.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testUNIVERSENAME"  + ", testUNIVERSEEXTENSION"  + ", 1"  + ", testUNIVERSEEXTENSIONNAME" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniversenameConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Universename(null ,  "testVERSIONID",  "testUNIVERSEEXTENSION");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Universename constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testUniversenameConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Universename whereObject = new Universename(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Universename(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + UNIVERSENAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest)  + ", " + UNIVERSEEXTENSIONNAME.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testUNIVERSENAME"  + ", testUNIVERSEEXTENSION"  + ", 1"  + ", testUNIVERSEEXTENSIONNAME" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniversenameConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Universename whereObject = new Universename(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Universename(null, whereObject);
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
    assertEquals("Universename", objUnderTest.getTableName());
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
    Universename whereObject = new Universename(rockFactory);

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
    Universename whereObject = new Universename(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universename");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universename");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSIONID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSIONID FROM Universename");
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
    
    String expected = "<Universename VERSIONID=\"'testVERSIONID'\" UNIVERSENAME=\"'testUNIVERSENAME'\" UNIVERSEEXTENSION=\"'testUNIVERSEEXTENSION'\" ORDERNRO=\"1\" UNIVERSEEXTENSIONNAME=\"'testUNIVERSEEXTENSIONNAME'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Universename VERSIONID=\"'testVERSIONID'\" UNIVERSENAME=\"'testUNIVERSENAME'\" UNIVERSEEXTENSION=\"'testUNIVERSEEXTENSION'\" ORDERNRO=\"1\" UNIVERSEEXTENSIONNAME=\"'testUNIVERSEEXTENSIONNAME'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Universename>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Universename ( VERSIONID, UNIVERSENAME, UNIVERSEEXTENSION, ORDERNRO, UNIVERSEEXTENSIONNAME ) values "
      + "( 'testVERSIONID', 'testUNIVERSENAME', 'testUNIVERSEEXTENSION', 1, 'testUNIVERSEEXTENSIONNAME' );\n";
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
    objUnderTest.setVersionid(UniversenameTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(UniversenameTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniversename() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniversename(UniversenameTest.testStringGenerator("anotherUNIVERSENAME", 30));
    assertEquals(UniversenameTest.testStringGenerator("anotherUNIVERSENAME", 30), UNIVERSENAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniverseextension() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniverseextension(UniversenameTest.testStringGenerator("anotherUNIVERSEEXTENSION", 16));
    assertEquals(UniversenameTest.testStringGenerator("anotherUNIVERSEEXTENSION", 16), UNIVERSEEXTENSION.get(objUnderTest));
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
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniverseextensionname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniverseextensionname(UniversenameTest.testStringGenerator("anotherUNIVERSEEXTENSIONNAME", 35));
    assertEquals(UniversenameTest.testStringGenerator("anotherUNIVERSEEXTENSIONNAME", 35), UNIVERSEEXTENSIONNAME.get(objUnderTest));
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
    String[] expectedKeys = { "VERSIONID","UNIVERSEEXTENSION"};

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
    objUnderTest = new Universename(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSIONID.get(objUnderTest)  + ", " + UNIVERSENAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest)  + ", " + UNIVERSEEXTENSIONNAME.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", 0"  + ", " ;
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
    Universename compareObj = new Universename(rockFactory ,  "testVERSIONID",  "testUNIVERSEEXTENSION");

    /* Testing first with null primary key value */
    VERSIONID.set(objUnderTest, null);
  	UNIVERSEEXTENSION.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSIONID.set(objUnderTest,  "different");
  	UNIVERSEEXTENSION.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSIONID.set(objUnderTest,  "testVERSIONID");
  	UNIVERSEEXTENSION.set(objUnderTest,  "testUNIVERSEEXTENSION");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Universename with our current one. If the two
   * Universenames are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnUniversename() throws Exception {

    /* Creating another Universename which will be compared to the tested one */
    Universename comparedObj = new Universename(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universename with our current one. If the two
   * Universenames are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentUniversename() throws Exception {

    /* Creating another Universename which will be compared to the tested one */
    Universename comparedObj = new Universename(rockFactory ,  "testVERSIONID",  "testUNIVERSEEXTENSION");
    comparedObj.setVersionid( "DifferentVERSIONID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universename with our current one. If the two
   * Universenames are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameUniversename() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Universename comparedObj = new Universename(rockFactory ,  "testVERSIONID",  "testUNIVERSEEXTENSION");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universename with our current one using null value.
   */
  @Test
  public void testEqualsWithNullUniversename() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Universename comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Universename was null \n");
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
    assertEquals(Universename.class, actualObject.getClass());
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
    Universename testAgg = new Universename(rockFactory ,  "testVERSIONID",  "testUNIVERSEEXTENSION");
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
   * Testing columnsize retrieving for Universeextensionname.
   */
  @Test
  public void testGetUniverseextensionnameColumnSize() throws Exception {
    
     assertEquals(35, objUnderTest.getUniverseextensionnameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Universeextensionname.
  */
  @Test
  public void testGetUniverseextensionnameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUniverseextensionnameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Universeextensionname.
  */
  @Test
  public void testGetUniverseextensionnameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUniverseextensionnameSQLType());    
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
   * Testing original Universename object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Universename(rockFactory, false);
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
    Universename changedOriginal = new Universename(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Universename(rockFactory, false);
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
    Universename changedOriginal = new Universename(rockFactory, false);
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