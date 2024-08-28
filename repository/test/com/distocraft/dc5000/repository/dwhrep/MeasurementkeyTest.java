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
 * Test class for Measurementkey. Changes to Measurementkey table are made via
 * this class.
 */
public class MeasurementkeyTest {

  private static Measurementkey objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field TYPEID;
  
  private static Field DATANAME;
  
  private static Field DESCRIPTION;
  
  private static Field ISELEMENT;
  
  private static Field UNIQUEKEY;
  
  private static Field COLNUMBER;
  
  private static Field DATATYPE;
  
  private static Field DATASIZE;
  
  private static Field DATASCALE;
  
  private static Field UNIQUEVALUE;
  
  private static Field NULLABLE;
  
  private static Field INDEXES;
  
  private static Field INCLUDESQL;
  
  private static Field UNIVOBJECT;
  
  private static Field JOINABLE;
  
  private static Field DATAID;
  private static Field ROPGRPCELL;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Measurementkey.class.getDeclaredField("newItem");
		TYPEID = Measurementkey.class.getDeclaredField("TYPEID");
		DATANAME = Measurementkey.class.getDeclaredField("DATANAME");
		DESCRIPTION = Measurementkey.class.getDeclaredField("DESCRIPTION");
		ISELEMENT = Measurementkey.class.getDeclaredField("ISELEMENT");
		UNIQUEKEY = Measurementkey.class.getDeclaredField("UNIQUEKEY");
		COLNUMBER = Measurementkey.class.getDeclaredField("COLNUMBER");
		DATATYPE = Measurementkey.class.getDeclaredField("DATATYPE");
		DATASIZE = Measurementkey.class.getDeclaredField("DATASIZE");
		DATASCALE = Measurementkey.class.getDeclaredField("DATASCALE");
		UNIQUEVALUE = Measurementkey.class.getDeclaredField("UNIQUEVALUE");
		NULLABLE = Measurementkey.class.getDeclaredField("NULLABLE");
		INDEXES = Measurementkey.class.getDeclaredField("INDEXES");
		INCLUDESQL = Measurementkey.class.getDeclaredField("INCLUDESQL");
		UNIVOBJECT = Measurementkey.class.getDeclaredField("UNIVOBJECT");
		JOINABLE = Measurementkey.class.getDeclaredField("JOINABLE");
		DATAID = Measurementkey.class.getDeclaredField("DATAID");
		ROPGRPCELL = Measurementkey.class.getDeclaredField("ROPGRPCELL");
		newItem.setAccessible(true);
		TYPEID.setAccessible(true);
		DATANAME.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		ISELEMENT.setAccessible(true);
		UNIQUEKEY.setAccessible(true);
		COLNUMBER.setAccessible(true);
		DATATYPE.setAccessible(true);
		DATASIZE.setAccessible(true);
		DATASCALE.setAccessible(true);
		UNIQUEVALUE.setAccessible(true);
		NULLABLE.setAccessible(true);
		INDEXES.setAccessible(true);
		INCLUDESQL.setAccessible(true);
		UNIVOBJECT.setAccessible(true);
		JOINABLE.setAccessible(true);
		DATAID.setAccessible(true);
		ROPGRPCELL.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Measurementkey ( TYPEID VARCHAR(31)  ,DATANAME VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,ISELEMENT INTEGER  ,UNIQUEKEY INTEGER  ,COLNUMBER BIGINT  ,DATATYPE VARCHAR(31) ,DATASIZE INTEGER  ,DATASCALE INTEGER  ,UNIQUEVALUE BIGINT  ,NULLABLE INTEGER  ,INDEXES VARCHAR(31) ,INCLUDESQL INTEGER  ,UNIVOBJECT VARCHAR(31) ,JOINABLE INTEGER  ,DATAID VARCHAR(31) ,ROPGRPCELL INTEGER)");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Measurementkey");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Measurementkey VALUES( 'testTYPEID'  ,'testDATANAME'  ,'testDESCRIPTION'  ,1  ,1  ,1  ,'testDATATYPE'  ,1  ,1  ,1  ,1  ,'testINDEXES'  ,1  ,'testUNIVOBJECT'  ,1  ,'testDATAID' ,0 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Measurementkey(rockFactory ,  "testTYPEID",  "testDATANAME");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Measurementkey");
    objUnderTest = null;
  }
  
  /**
   * Testing Measurementkey constructor variable initialization with null values.
   */
  @Test
  public void testMeasurementkeyConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Measurementkey(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + ISELEMENT.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest)  + ", " + UNIVOBJECT.get(objUnderTest)  + ", " + JOINABLE.get(objUnderTest)  + ", " + DATAID.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Measurementkey constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeasurementkeyConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Measurementkey(rockFactory ,  "testTYPEID",  "testDATANAME");

    /* Asserting that variables are initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + ISELEMENT.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest)  + ", " + UNIVOBJECT.get(objUnderTest)  + ", " + JOINABLE.get(objUnderTest)  + ", " + DATAID.get(objUnderTest) ;
    String expected =  "testTYPEID"  + ", testDATANAME"  + ", testDESCRIPTION"  + ", 1"  + ", 1"  + ", 1"  + ", testDATATYPE"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testINDEXES"  + ", 1"  + ", testUNIVOBJECT"  + ", 1"  + ", testDATAID" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementkeyConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Measurementkey(null ,  "testTYPEID",  "testDATANAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Measurementkey constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeasurementkeyConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Measurementkey whereObject = new Measurementkey(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Measurementkey(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + ISELEMENT.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest)  + ", " + UNIVOBJECT.get(objUnderTest)  + ", " + JOINABLE.get(objUnderTest)  + ", " + DATAID.get(objUnderTest) ;
    String expected =  "testTYPEID"  + ", testDATANAME"  + ", testDESCRIPTION"  + ", 1"  + ", 1"  + ", 1"  + ", testDATATYPE"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testINDEXES"  + ", 1"  + ", testUNIVOBJECT"  + ", 1"  + ", testDATAID" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementkeyConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Measurementkey whereObject = new Measurementkey(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Measurementkey(null, whereObject);
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
    assertEquals("Measurementkey", objUnderTest.getTableName());
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
    Measurementkey whereObject = new Measurementkey(rockFactory);

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
    Measurementkey whereObject = new Measurementkey(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementkey");
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
    TYPEID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("TYPEID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementkey");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the TYPEID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT TYPEID FROM Measurementkey");
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
    
    String expected = "<Measurementkey TYPEID=\"'testTYPEID'\" DATANAME=\"'testDATANAME'\" DESCRIPTION=\"'testDESCRIPTION'\" ISELEMENT=\"1\" UNIQUEKEY=\"1\" COLNUMBER=\"1\" DATATYPE=\"'testDATATYPE'\" DATASIZE=\"1\" DATASCALE=\"1\" UNIQUEVALUE=\"1\" NULLABLE=\"1\" INDEXES=\"'testINDEXES'\" INCLUDESQL=\"1\" UNIVOBJECT=\"'testUNIVOBJECT'\" JOINABLE=\"1\" DATAID=\"'testDATAID'\" ROPGRPCELL=\"0\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Measurementkey TYPEID=\"'testTYPEID'\" DATANAME=\"'testDATANAME'\" DESCRIPTION=\"'testDESCRIPTION'\" ISELEMENT=\"1\" UNIQUEKEY=\"1\" COLNUMBER=\"1\" DATATYPE=\"'testDATATYPE'\" DATASIZE=\"1\" DATASCALE=\"1\" UNIQUEVALUE=\"1\" NULLABLE=\"1\" INDEXES=\"'testINDEXES'\" INCLUDESQL=\"1\" UNIVOBJECT=\"'testUNIVOBJECT'\" JOINABLE=\"1\" DATAID=\"'testDATAID'\" ROPGRPCELL=\"0\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Measurementkey>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Measurementkey ( TYPEID, DATANAME, DESCRIPTION, ISELEMENT, UNIQUEKEY, COLNUMBER, DATATYPE, DATASIZE, DATASCALE, UNIQUEVALUE, NULLABLE, INDEXES, INCLUDESQL, UNIVOBJECT, JOINABLE, DATAID, ROPGRPCELL ) values "
      + "( 'testTYPEID', 'testDATANAME', 'testDESCRIPTION', 1, 1, 1, 'testDATATYPE', 1, 1, 1, 1, 'testINDEXES', 1, 'testUNIVOBJECT', 1, 'testDATAID', 0 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTypeid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTypeid(MeasurementkeyTest.testStringGenerator("anotherTYPEID", 255));
    assertEquals(MeasurementkeyTest.testStringGenerator("anotherTYPEID", 255), TYPEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataname(MeasurementkeyTest.testStringGenerator("anotherDATANAME", 128));
    assertEquals(MeasurementkeyTest.testStringGenerator("anotherDATANAME", 128), DATANAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(MeasurementkeyTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(MeasurementkeyTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetIselement() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIselement(555);
    assertEquals(555, ISELEMENT.get(objUnderTest));
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
    objUnderTest.setDatatype(MeasurementkeyTest.testStringGenerator("anotherDATATYPE", 50));
    assertEquals(MeasurementkeyTest.testStringGenerator("anotherDATATYPE", 50), DATATYPE.get(objUnderTest));
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
    objUnderTest.setIndexes(MeasurementkeyTest.testStringGenerator("anotherINDEXES", 20));
    assertEquals(MeasurementkeyTest.testStringGenerator("anotherINDEXES", 20), INDEXES.get(objUnderTest));
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
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUnivobject() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUnivobject(MeasurementkeyTest.testStringGenerator("anotherUNIVOBJECT", 128));
    assertEquals(MeasurementkeyTest.testStringGenerator("anotherUNIVOBJECT", 128), UNIVOBJECT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetJoinable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setJoinable(555);
    assertEquals(555, JOINABLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataid(MeasurementkeyTest.testStringGenerator("anotherDATAID", 255));
    assertEquals(MeasurementkeyTest.testStringGenerator("anotherDATAID", 255), DATAID.get(objUnderTest));
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
    String[] expectedKeys = { "TYPEID","DATANAME"};

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
    objUnderTest = new Measurementkey(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + ISELEMENT.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest)  + ", " + UNIVOBJECT.get(objUnderTest)  + ", " + JOINABLE.get(objUnderTest)  + ", " + DATAID.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", 0"  + ", "  + ", 0"  + ", " ;
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
    Measurementkey compareObj = new Measurementkey(rockFactory ,  "testTYPEID",  "testDATANAME");

    /* Testing first with null primary key value */
    TYPEID.set(objUnderTest, null);
  	DATANAME.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    TYPEID.set(objUnderTest,  "different");
  	DATANAME.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    TYPEID.set(objUnderTest,  "testTYPEID");
  	DATANAME.set(objUnderTest,  "testDATANAME");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Measurementkey with our current one. If the two
   * Measurementkeys are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeasurementkey() throws Exception {

    /* Creating another Measurementkey which will be compared to the tested one */
    Measurementkey comparedObj = new Measurementkey(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementkey with our current one. If the two
   * Measurementkeys are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeasurementkey() throws Exception {

    /* Creating another Measurementkey which will be compared to the tested one */
    Measurementkey comparedObj = new Measurementkey(rockFactory ,  "testTYPEID",  "testDATANAME");
    comparedObj.setTypeid( "DifferentTYPEID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementkey with our current one. If the two
   * Measurementkeys are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeasurementkey() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Measurementkey comparedObj = new Measurementkey(rockFactory ,  "testTYPEID",  "testDATANAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementkey with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeasurementkey() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Measurementkey comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Measurementkey was null \n");
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
    assertEquals(Measurementkey.class, actualObject.getClass());
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
    Measurementkey testAgg = new Measurementkey(rockFactory ,  "testTYPEID",  "testDATANAME");
    TYPEID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
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
   * Testing columnsize retrieving for Iselement.
   */
  @Test
  public void testGetIselementColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getIselementColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Iselement.
  */
  @Test
  public void testGetIselementDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIselementDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Iselement.
  */
  @Test
  public void testGetIselementSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getIselementSQLType());    
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
   * Testing columnsize retrieving for Univobject.
   */
  @Test
  public void testGetUnivobjectColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getUnivobjectColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Univobject.
  */
  @Test
  public void testGetUnivobjectDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUnivobjectDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Univobject.
  */
  @Test
  public void testGetUnivobjectSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUnivobjectSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Joinable.
   */
  @Test
  public void testGetJoinableColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getJoinableColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Joinable.
  */
  @Test
  public void testGetJoinableDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getJoinableDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Joinable.
  */
  @Test
  public void testGetJoinableSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getJoinableSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Dataid.
   */
  @Test
  public void testGetDataidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getDataidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dataid.
  */
  @Test
  public void testGetDataidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDataidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dataid.
  */
  @Test
  public void testGetDataidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDataidSQLType());    
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
   * Testing original Measurementkey object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Measurementkey(rockFactory, false);
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
    Measurementkey changedOriginal = new Measurementkey(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Measurementkey(rockFactory, false);
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
    Measurementkey changedOriginal = new Measurementkey(rockFactory, false);
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