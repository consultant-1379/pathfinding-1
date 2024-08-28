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
 * Test class for Dwhcolumn. Changes to Dwhcolumn table are made via
 * this class.
 */
public class DwhcolumnTest {

  private static Dwhcolumn objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field STORAGEID;
  
  private static Field DATANAME;
  
  private static Field COLNUMBER;
  
  private static Field DATATYPE;
  
  private static Field DATASIZE;
  
  private static Field DATASCALE;
  
  private static Field UNIQUEVALUE;
  
  private static Field NULLABLE;
  
  private static Field INDEXES;
  
  private static Field UNIQUEKEY;
  
  private static Field STATUS;
  
  private static Field INCLUDESQL;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Dwhcolumn.class.getDeclaredField("newItem");
		STORAGEID = Dwhcolumn.class.getDeclaredField("STORAGEID");
		DATANAME = Dwhcolumn.class.getDeclaredField("DATANAME");
		COLNUMBER = Dwhcolumn.class.getDeclaredField("COLNUMBER");
		DATATYPE = Dwhcolumn.class.getDeclaredField("DATATYPE");
		DATASIZE = Dwhcolumn.class.getDeclaredField("DATASIZE");
		DATASCALE = Dwhcolumn.class.getDeclaredField("DATASCALE");
		UNIQUEVALUE = Dwhcolumn.class.getDeclaredField("UNIQUEVALUE");
		NULLABLE = Dwhcolumn.class.getDeclaredField("NULLABLE");
		INDEXES = Dwhcolumn.class.getDeclaredField("INDEXES");
		UNIQUEKEY = Dwhcolumn.class.getDeclaredField("UNIQUEKEY");
		STATUS = Dwhcolumn.class.getDeclaredField("STATUS");
		INCLUDESQL = Dwhcolumn.class.getDeclaredField("INCLUDESQL");
		newItem.setAccessible(true);
		STORAGEID.setAccessible(true);
		DATANAME.setAccessible(true);
		COLNUMBER.setAccessible(true);
		DATATYPE.setAccessible(true);
		DATASIZE.setAccessible(true);
		DATASCALE.setAccessible(true);
		UNIQUEVALUE.setAccessible(true);
		NULLABLE.setAccessible(true);
		INDEXES.setAccessible(true);
		UNIQUEKEY.setAccessible(true);
		STATUS.setAccessible(true);
		INCLUDESQL.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Dwhcolumn ( STORAGEID VARCHAR(31)  ,DATANAME VARCHAR(31) ,COLNUMBER BIGINT  ,DATATYPE VARCHAR(31) ,DATASIZE INTEGER  ,DATASCALE INTEGER  ,UNIQUEVALUE BIGINT  ,NULLABLE INTEGER  ,INDEXES VARCHAR(31) ,UNIQUEKEY INTEGER  ,STATUS VARCHAR(31) ,INCLUDESQL INTEGER )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Dwhcolumn");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Dwhcolumn VALUES( 'testSTORAGEID'  ,'testDATANAME'  ,1  ,'testDATATYPE'  ,1  ,1  ,1  ,1  ,'testINDEXES'  ,1  ,'testSTATUS'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Dwhcolumn(rockFactory ,  "testSTORAGEID",  "testDATANAME");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Dwhcolumn");
    objUnderTest = null;
  }
  
  /**
   * Testing Dwhcolumn constructor variable initialization with null values.
   */
  @Test
  public void testDwhcolumnConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Dwhcolumn(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  STORAGEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Dwhcolumn constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDwhcolumnConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Dwhcolumn(rockFactory ,  "testSTORAGEID",  "testDATANAME");

    /* Asserting that variables are initialized */
    String actual =  STORAGEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest) ;
    String expected =  "testSTORAGEID"  + ", testDATANAME"  + ", 1"  + ", testDATATYPE"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testINDEXES"  + ", 1"  + ", testSTATUS"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDwhcolumnConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Dwhcolumn(null ,  "testSTORAGEID",  "testDATANAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Dwhcolumn constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDwhcolumnConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Dwhcolumn whereObject = new Dwhcolumn(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Dwhcolumn(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  STORAGEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest) ;
    String expected =  "testSTORAGEID"  + ", testDATANAME"  + ", 1"  + ", testDATATYPE"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testINDEXES"  + ", 1"  + ", testSTATUS"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDwhcolumnConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Dwhcolumn whereObject = new Dwhcolumn(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Dwhcolumn(null, whereObject);
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
    assertEquals("Dwhcolumn", objUnderTest.getTableName());
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
    Dwhcolumn whereObject = new Dwhcolumn(rockFactory);

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
    Dwhcolumn whereObject = new Dwhcolumn(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dwhcolumn");
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
    STORAGEID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("STORAGEID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dwhcolumn");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the STORAGEID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT STORAGEID FROM Dwhcolumn");
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
    
    String expected = "<Dwhcolumn STORAGEID=\"'testSTORAGEID'\" DATANAME=\"'testDATANAME'\" COLNUMBER=\"1\" DATATYPE=\"'testDATATYPE'\" DATASIZE=\"1\" DATASCALE=\"1\" UNIQUEVALUE=\"1\" NULLABLE=\"1\" INDEXES=\"'testINDEXES'\" UNIQUEKEY=\"1\" STATUS=\"'testSTATUS'\" INCLUDESQL=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Dwhcolumn STORAGEID=\"'testSTORAGEID'\" DATANAME=\"'testDATANAME'\" COLNUMBER=\"1\" DATATYPE=\"'testDATATYPE'\" DATASIZE=\"1\" DATASCALE=\"1\" UNIQUEVALUE=\"1\" NULLABLE=\"1\" INDEXES=\"'testINDEXES'\" UNIQUEKEY=\"1\" STATUS=\"'testSTATUS'\" INCLUDESQL=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Dwhcolumn>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Dwhcolumn ( STORAGEID, DATANAME, COLNUMBER, DATATYPE, DATASIZE, DATASCALE, UNIQUEVALUE, NULLABLE, INDEXES, UNIQUEKEY, STATUS, INCLUDESQL ) values "
      + "( 'testSTORAGEID', 'testDATANAME', 1, 'testDATATYPE', 1, 1, 1, 1, 'testINDEXES', 1, 'testSTATUS', 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStorageid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStorageid(DwhcolumnTest.testStringGenerator("anotherSTORAGEID", 255));
    assertEquals(DwhcolumnTest.testStringGenerator("anotherSTORAGEID", 255), STORAGEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataname(DwhcolumnTest.testStringGenerator("anotherDATANAME", 128));
    assertEquals(DwhcolumnTest.testStringGenerator("anotherDATANAME", 128), DATANAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColnumber() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColnumber(555L);
    assertEquals(555L, COLNUMBER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDatatype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDatatype(DwhcolumnTest.testStringGenerator("anotherDATATYPE", 50));
    assertEquals(DwhcolumnTest.testStringGenerator("anotherDATATYPE", 50), DATATYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDatasize() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDatasize(555);
    assertEquals(555, DATASIZE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDatascale() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDatascale(555);
    assertEquals(555, DATASCALE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniquevalue() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniquevalue(555L);
    assertEquals(555L, UNIQUEVALUE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetNullable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setNullable(555);
    assertEquals(555, NULLABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIndexes() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIndexes(DwhcolumnTest.testStringGenerator("anotherINDEXES", 20));
    assertEquals(DwhcolumnTest.testStringGenerator("anotherINDEXES", 20), INDEXES.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniquekey() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniquekey(555);
    assertEquals(555, UNIQUEKEY.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatus() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatus(DwhcolumnTest.testStringGenerator("anotherSTATUS", 10));
    assertEquals(DwhcolumnTest.testStringGenerator("anotherSTATUS", 10), STATUS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIncludesql() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIncludesql(555);
    assertEquals(555, INCLUDESQL.get(objUnderTest));
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
    String[] expectedKeys = { "STORAGEID","DATANAME"};

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
    objUnderTest = new Dwhcolumn(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  STORAGEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", 0"  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", 0"  + ", "  + ", 0" ;
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
    Dwhcolumn compareObj = new Dwhcolumn(rockFactory ,  "testSTORAGEID",  "testDATANAME");

    /* Testing first with null primary key value */
    STORAGEID.set(objUnderTest, null);
  	DATANAME.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    STORAGEID.set(objUnderTest,  "different");
  	DATANAME.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    STORAGEID.set(objUnderTest,  "testSTORAGEID");
  	DATANAME.set(objUnderTest,  "testDATANAME");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Dwhcolumn with our current one. If the two
   * Dwhcolumns are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnDwhcolumn() throws Exception {

    /* Creating another Dwhcolumn which will be compared to the tested one */
    Dwhcolumn comparedObj = new Dwhcolumn(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dwhcolumn with our current one. If the two
   * Dwhcolumns are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentDwhcolumn() throws Exception {

    /* Creating another Dwhcolumn which will be compared to the tested one */
    Dwhcolumn comparedObj = new Dwhcolumn(rockFactory ,  "testSTORAGEID",  "testDATANAME");
    comparedObj.setStorageid( "DifferentSTORAGEID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dwhcolumn with our current one. If the two
   * Dwhcolumns are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameDwhcolumn() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Dwhcolumn comparedObj = new Dwhcolumn(rockFactory ,  "testSTORAGEID",  "testDATANAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dwhcolumn with our current one using null value.
   */
  @Test
  public void testEqualsWithNullDwhcolumn() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Dwhcolumn comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Dwhcolumn was null \n");
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
    assertEquals(Dwhcolumn.class, actualObject.getClass());
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
    Dwhcolumn testAgg = new Dwhcolumn(rockFactory ,  "testSTORAGEID",  "testDATANAME");
    STORAGEID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Storageid.
   */
  @Test
  public void testGetStorageidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getStorageidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Storageid.
  */
  @Test
  public void testGetStorageidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getStorageidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Storageid.
  */
  @Test
  public void testGetStorageidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getStorageidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Dataname.
   */
  @Test
  public void testGetDatanameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getDatanameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dataname.
  */
  @Test
  public void testGetDatanameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDatanameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dataname.
  */
  @Test
  public void testGetDatanameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDatanameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Colnumber.
   */
  @Test
  public void testGetColnumberColumnSize() throws Exception {
    
     assertEquals(9, objUnderTest.getColnumberColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Colnumber.
  */
  @Test
  public void testGetColnumberDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColnumberDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Colnumber.
  */
  @Test
  public void testGetColnumberSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getColnumberSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Datatype.
   */
  @Test
  public void testGetDatatypeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getDatatypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Datatype.
  */
  @Test
  public void testGetDatatypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDatatypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Datatype.
  */
  @Test
  public void testGetDatatypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDatatypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Datasize.
   */
  @Test
  public void testGetDatasizeColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getDatasizeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Datasize.
  */
  @Test
  public void testGetDatasizeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDatasizeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Datasize.
  */
  @Test
  public void testGetDatasizeSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getDatasizeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Datascale.
   */
  @Test
  public void testGetDatascaleColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getDatascaleColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Datascale.
  */
  @Test
  public void testGetDatascaleDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDatascaleDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Datascale.
  */
  @Test
  public void testGetDatascaleSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getDatascaleSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Uniquevalue.
   */
  @Test
  public void testGetUniquevalueColumnSize() throws Exception {
    
     assertEquals(9, objUnderTest.getUniquevalueColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Uniquevalue.
  */
  @Test
  public void testGetUniquevalueDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUniquevalueDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Uniquevalue.
  */
  @Test
  public void testGetUniquevalueSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getUniquevalueSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Nullable.
   */
  @Test
  public void testGetNullableColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getNullableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Nullable.
  */
  @Test
  public void testGetNullableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getNullableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Nullable.
  */
  @Test
  public void testGetNullableSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getNullableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Indexes.
   */
  @Test
  public void testGetIndexesColumnSize() throws Exception {
    
     assertEquals(20, objUnderTest.getIndexesColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Indexes.
  */
  @Test
  public void testGetIndexesDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIndexesDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Indexes.
  */
  @Test
  public void testGetIndexesSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getIndexesSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Uniquekey.
   */
  @Test
  public void testGetUniquekeyColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getUniquekeyColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Uniquekey.
  */
  @Test
  public void testGetUniquekeyDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUniquekeyDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Uniquekey.
  */
  @Test
  public void testGetUniquekeySQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getUniquekeySQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Status.
   */
  @Test
  public void testGetStatusColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getStatusColumnSize());   
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
   * Testing columnsize retrieving for Includesql.
   */
  @Test
  public void testGetIncludesqlColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getIncludesqlColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Includesql.
  */
  @Test
  public void testGetIncludesqlDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIncludesqlDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Includesql.
  */
  @Test
  public void testGetIncludesqlSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getIncludesqlSQLType());    
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
   * Testing original Dwhcolumn object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Dwhcolumn(rockFactory, false);
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
    Dwhcolumn changedOriginal = new Dwhcolumn(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Dwhcolumn(rockFactory, false);
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
    Dwhcolumn changedOriginal = new Dwhcolumn(rockFactory, false);
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