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
 * Test class for Referencecolumn. Changes to Referencecolumn table are made via
 * this class.
 */
public class ReferencecolumnTest {

  private static Referencecolumn objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field TYPEID;
  
  private static Field DATANAME;
  
  private static Field COLNUMBER;
  
  private static Field DATATYPE;
  
  private static Field DATASIZE;
  
  private static Field DATASCALE;
  
  private static Field UNIQUEVALUE;
  
  private static Field NULLABLE;
  
  private static Field INDEXES;
  
  private static Field UNIQUEKEY;
  
  private static Field INCLUDESQL;
  
  private static Field INCLUDEUPD;
  
  private static Field COLTYPE;
  
  private static Field DESCRIPTION;
  
  private static Field UNIVERSECLASS;
  
  private static Field UNIVERSEOBJECT;
  
  private static Field UNIVERSECONDITION;
  
  private static Field DATAID;
  
  private static Field BASEDEF;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Referencecolumn.class.getDeclaredField("newItem");
		TYPEID = Referencecolumn.class.getDeclaredField("TYPEID");
		DATANAME = Referencecolumn.class.getDeclaredField("DATANAME");
		COLNUMBER = Referencecolumn.class.getDeclaredField("COLNUMBER");
		DATATYPE = Referencecolumn.class.getDeclaredField("DATATYPE");
		DATASIZE = Referencecolumn.class.getDeclaredField("DATASIZE");
		DATASCALE = Referencecolumn.class.getDeclaredField("DATASCALE");
		UNIQUEVALUE = Referencecolumn.class.getDeclaredField("UNIQUEVALUE");
		NULLABLE = Referencecolumn.class.getDeclaredField("NULLABLE");
		INDEXES = Referencecolumn.class.getDeclaredField("INDEXES");
		UNIQUEKEY = Referencecolumn.class.getDeclaredField("UNIQUEKEY");
		INCLUDESQL = Referencecolumn.class.getDeclaredField("INCLUDESQL");
		INCLUDEUPD = Referencecolumn.class.getDeclaredField("INCLUDEUPD");
		COLTYPE = Referencecolumn.class.getDeclaredField("COLTYPE");
		DESCRIPTION = Referencecolumn.class.getDeclaredField("DESCRIPTION");
		UNIVERSECLASS = Referencecolumn.class.getDeclaredField("UNIVERSECLASS");
		UNIVERSEOBJECT = Referencecolumn.class.getDeclaredField("UNIVERSEOBJECT");
		UNIVERSECONDITION = Referencecolumn.class.getDeclaredField("UNIVERSECONDITION");
		DATAID = Referencecolumn.class.getDeclaredField("DATAID");
		BASEDEF = Referencecolumn.class.getDeclaredField("BASEDEF");
		newItem.setAccessible(true);
		TYPEID.setAccessible(true);
		DATANAME.setAccessible(true);
		COLNUMBER.setAccessible(true);
		DATATYPE.setAccessible(true);
		DATASIZE.setAccessible(true);
		DATASCALE.setAccessible(true);
		UNIQUEVALUE.setAccessible(true);
		NULLABLE.setAccessible(true);
		INDEXES.setAccessible(true);
		UNIQUEKEY.setAccessible(true);
		INCLUDESQL.setAccessible(true);
		INCLUDEUPD.setAccessible(true);
		COLTYPE.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		UNIVERSECLASS.setAccessible(true);
		UNIVERSEOBJECT.setAccessible(true);
		UNIVERSECONDITION.setAccessible(true);
		DATAID.setAccessible(true);
		BASEDEF.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Referencecolumn ( TYPEID VARCHAR(31)  ,DATANAME VARCHAR(31) ,COLNUMBER BIGINT  ,DATATYPE VARCHAR(31) ,DATASIZE INTEGER  ,DATASCALE INTEGER  ,UNIQUEVALUE BIGINT  ,NULLABLE INTEGER  ,INDEXES VARCHAR(31) ,UNIQUEKEY INTEGER  ,INCLUDESQL INTEGER  ,INCLUDEUPD INTEGER  ,COLTYPE VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,UNIVERSECLASS VARCHAR(31) ,UNIVERSEOBJECT VARCHAR(31) ,UNIVERSECONDITION VARCHAR(31) ,DATAID VARCHAR(31) ,BASEDEF INTEGER )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Referencecolumn");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Referencecolumn VALUES( 'testTYPEID'  ,'testDATANAME'  ,1  ,'testDATATYPE'  ,1  ,1  ,1  ,1  ,'testINDEXES'  ,1  ,1  ,1  ,'testCOLTYPE'  ,'testDESCRIPTION'  ,'testUNIVERSECLASS'  ,'testUNIVERSEOBJECT'  ,'testUNIVERSECONDITION'  ,'testDATAID'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Referencecolumn(rockFactory ,  "testTYPEID",  "testDATANAME");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Referencecolumn");
    objUnderTest = null;
  }
  
  /**
   * Testing Referencecolumn constructor variable initialization with null values.
   */
  @Test
  public void testReferencecolumnConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Referencecolumn(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest)  + ", " + INCLUDEUPD.get(objUnderTest)  + ", " + COLTYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + UNIVERSECLASS.get(objUnderTest)  + ", " + UNIVERSEOBJECT.get(objUnderTest)  + ", " + UNIVERSECONDITION.get(objUnderTest)  + ", " + DATAID.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Referencecolumn constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testReferencecolumnConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Referencecolumn(rockFactory ,  "testTYPEID",  "testDATANAME");

    /* Asserting that variables are initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest)  + ", " + INCLUDEUPD.get(objUnderTest)  + ", " + COLTYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + UNIVERSECLASS.get(objUnderTest)  + ", " + UNIVERSEOBJECT.get(objUnderTest)  + ", " + UNIVERSECONDITION.get(objUnderTest)  + ", " + DATAID.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  "testTYPEID"  + ", testDATANAME"  + ", 1"  + ", testDATATYPE"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testINDEXES"  + ", 1"  + ", 1"  + ", 1"  + ", testCOLTYPE"  + ", testDESCRIPTION"  + ", testUNIVERSECLASS"  + ", testUNIVERSEOBJECT"  + ", testUNIVERSECONDITION"  + ", testDATAID"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testReferencecolumnConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Referencecolumn(null ,  "testTYPEID",  "testDATANAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Referencecolumn constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testReferencecolumnConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Referencecolumn whereObject = new Referencecolumn(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Referencecolumn(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest)  + ", " + INCLUDEUPD.get(objUnderTest)  + ", " + COLTYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + UNIVERSECLASS.get(objUnderTest)  + ", " + UNIVERSEOBJECT.get(objUnderTest)  + ", " + UNIVERSECONDITION.get(objUnderTest)  + ", " + DATAID.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  "testTYPEID"  + ", testDATANAME"  + ", 1"  + ", testDATATYPE"  + ", 1"  + ", 1"  + ", 1"  + ", 1"  + ", testINDEXES"  + ", 1"  + ", 1"  + ", 1"  + ", testCOLTYPE"  + ", testDESCRIPTION"  + ", testUNIVERSECLASS"  + ", testUNIVERSEOBJECT"  + ", testUNIVERSECONDITION"  + ", testDATAID"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testReferencecolumnConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Referencecolumn whereObject = new Referencecolumn(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Referencecolumn(null, whereObject);
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
    assertEquals("Referencecolumn", objUnderTest.getTableName());
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
    Referencecolumn whereObject = new Referencecolumn(rockFactory);

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
    Referencecolumn whereObject = new Referencecolumn(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Referencecolumn");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Referencecolumn");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the TYPEID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT TYPEID FROM Referencecolumn");
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
    
    String expected = "<Referencecolumn TYPEID=\"'testTYPEID'\" DATANAME=\"'testDATANAME'\" COLNUMBER=\"1\" DATATYPE=\"'testDATATYPE'\" DATASIZE=\"1\" DATASCALE=\"1\" UNIQUEVALUE=\"1\" NULLABLE=\"1\" INDEXES=\"'testINDEXES'\" UNIQUEKEY=\"1\" INCLUDESQL=\"1\" INCLUDEUPD=\"1\" COLTYPE=\"'testCOLTYPE'\" DESCRIPTION=\"'testDESCRIPTION'\" UNIVERSECLASS=\"'testUNIVERSECLASS'\" UNIVERSEOBJECT=\"'testUNIVERSEOBJECT'\" UNIVERSECONDITION=\"'testUNIVERSECONDITION'\" DATAID=\"'testDATAID'\" BASEDEF=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Referencecolumn TYPEID=\"'testTYPEID'\" DATANAME=\"'testDATANAME'\" COLNUMBER=\"1\" DATATYPE=\"'testDATATYPE'\" DATASIZE=\"1\" DATASCALE=\"1\" UNIQUEVALUE=\"1\" NULLABLE=\"1\" INDEXES=\"'testINDEXES'\" UNIQUEKEY=\"1\" INCLUDESQL=\"1\" INCLUDEUPD=\"1\" COLTYPE=\"'testCOLTYPE'\" DESCRIPTION=\"'testDESCRIPTION'\" UNIVERSECLASS=\"'testUNIVERSECLASS'\" UNIVERSEOBJECT=\"'testUNIVERSEOBJECT'\" UNIVERSECONDITION=\"'testUNIVERSECONDITION'\" DATAID=\"'testDATAID'\" BASEDEF=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Referencecolumn>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Referencecolumn ( TYPEID, DATANAME, COLNUMBER, DATATYPE, DATASIZE, DATASCALE, UNIQUEVALUE, NULLABLE, INDEXES, UNIQUEKEY, INCLUDESQL, INCLUDEUPD, COLTYPE, DESCRIPTION, UNIVERSECLASS, UNIVERSEOBJECT, UNIVERSECONDITION, DATAID, BASEDEF ) values "
      + "( 'testTYPEID', 'testDATANAME', 1, 'testDATATYPE', 1, 1, 1, 1, 'testINDEXES', 1, 1, 1, 'testCOLTYPE', 'testDESCRIPTION', 'testUNIVERSECLASS', 'testUNIVERSEOBJECT', 'testUNIVERSECONDITION', 'testDATAID', 1 );\n";
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
    objUnderTest.setTypeid(ReferencecolumnTest.testStringGenerator("anotherTYPEID", 255));
    assertEquals(ReferencecolumnTest.testStringGenerator("anotherTYPEID", 255), TYPEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataname(ReferencecolumnTest.testStringGenerator("anotherDATANAME", 128));
    assertEquals(ReferencecolumnTest.testStringGenerator("anotherDATANAME", 128), DATANAME.get(objUnderTest));
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
    objUnderTest.setDatatype(ReferencecolumnTest.testStringGenerator("anotherDATATYPE", 50));
    assertEquals(ReferencecolumnTest.testStringGenerator("anotherDATATYPE", 50), DATATYPE.get(objUnderTest));
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
    objUnderTest.setIndexes(ReferencecolumnTest.testStringGenerator("anotherINDEXES", 20));
    assertEquals(ReferencecolumnTest.testStringGenerator("anotherINDEXES", 20), INDEXES.get(objUnderTest));
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
  public void testSetAndGetIncludeupd() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setIncludeupd(555);
    assertEquals(555, INCLUDEUPD.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetColtype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setColtype(ReferencecolumnTest.testStringGenerator("anotherCOLTYPE", 16));
    assertEquals(ReferencecolumnTest.testStringGenerator("anotherCOLTYPE", 16), COLTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(ReferencecolumnTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(ReferencecolumnTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniverseclass() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniverseclass(ReferencecolumnTest.testStringGenerator("anotherUNIVERSECLASS", 35));
    assertEquals(ReferencecolumnTest.testStringGenerator("anotherUNIVERSECLASS", 35), UNIVERSECLASS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniverseobject() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniverseobject(ReferencecolumnTest.testStringGenerator("anotherUNIVERSEOBJECT", 128));
    assertEquals(ReferencecolumnTest.testStringGenerator("anotherUNIVERSEOBJECT", 128), UNIVERSEOBJECT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniversecondition() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniversecondition(ReferencecolumnTest.testStringGenerator("anotherUNIVERSECONDITION", 255));
    assertEquals(ReferencecolumnTest.testStringGenerator("anotherUNIVERSECONDITION", 255), UNIVERSECONDITION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataid(ReferencecolumnTest.testStringGenerator("anotherDATAID", 255));
    assertEquals(ReferencecolumnTest.testStringGenerator("anotherDATAID", 255), DATAID.get(objUnderTest));
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
    objUnderTest = new Referencecolumn(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + COLNUMBER.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + UNIQUEVALUE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest)  + ", " + INDEXES.get(objUnderTest)  + ", " + UNIQUEKEY.get(objUnderTest)  + ", " + INCLUDESQL.get(objUnderTest)  + ", " + INCLUDEUPD.get(objUnderTest)  + ", " + COLTYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + UNIVERSECLASS.get(objUnderTest)  + ", " + UNIVERSEOBJECT.get(objUnderTest)  + ", " + UNIVERSECONDITION.get(objUnderTest)  + ", " + DATAID.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", 0"  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0" ;
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
    Referencecolumn compareObj = new Referencecolumn(rockFactory ,  "testTYPEID",  "testDATANAME");

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
   * Testing comparing another Referencecolumn with our current one. If the two
   * Referencecolumns are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnReferencecolumn() throws Exception {

    /* Creating another Referencecolumn which will be compared to the tested one */
    Referencecolumn comparedObj = new Referencecolumn(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Referencecolumn with our current one. If the two
   * Referencecolumns are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentReferencecolumn() throws Exception {

    /* Creating another Referencecolumn which will be compared to the tested one */
    Referencecolumn comparedObj = new Referencecolumn(rockFactory ,  "testTYPEID",  "testDATANAME");
    comparedObj.setTypeid( "DifferentTYPEID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Referencecolumn with our current one. If the two
   * Referencecolumns are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameReferencecolumn() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Referencecolumn comparedObj = new Referencecolumn(rockFactory ,  "testTYPEID",  "testDATANAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Referencecolumn with our current one using null value.
   */
  @Test
  public void testEqualsWithNullReferencecolumn() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Referencecolumn comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Referencecolumn was null \n");
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
    assertEquals(Referencecolumn.class, actualObject.getClass());
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
    Referencecolumn testAgg = new Referencecolumn(rockFactory ,  "testTYPEID",  "testDATANAME");
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
   * Testing columnsize retrieving for Includeupd.
   */
  @Test
  public void testGetIncludeupdColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getIncludeupdColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Includeupd.
  */
  @Test
  public void testGetIncludeupdDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getIncludeupdDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Includeupd.
  */
  @Test
  public void testGetIncludeupdSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getIncludeupdSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Coltype.
   */
  @Test
  public void testGetColtypeColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getColtypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Coltype.
  */
  @Test
  public void testGetColtypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getColtypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Coltype.
  */
  @Test
  public void testGetColtypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getColtypeSQLType());    
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
   * Testing columnsize retrieving for Universeclass.
   */
  @Test
  public void testGetUniverseclassColumnSize() throws Exception {
    
     assertEquals(35, objUnderTest.getUniverseclassColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Universeclass.
  */
  @Test
  public void testGetUniverseclassDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUniverseclassDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Universeclass.
  */
  @Test
  public void testGetUniverseclassSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUniverseclassSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Universeobject.
   */
  @Test
  public void testGetUniverseobjectColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getUniverseobjectColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Universeobject.
  */
  @Test
  public void testGetUniverseobjectDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUniverseobjectDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Universeobject.
  */
  @Test
  public void testGetUniverseobjectSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUniverseobjectSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Universecondition.
   */
  @Test
  public void testGetUniverseconditionColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getUniverseconditionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Universecondition.
  */
  @Test
  public void testGetUniverseconditionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUniverseconditionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Universecondition.
  */
  @Test
  public void testGetUniverseconditionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUniverseconditionSQLType());    
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
   * Testing original Referencecolumn object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Referencecolumn(rockFactory, false);
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
    Referencecolumn changedOriginal = new Referencecolumn(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Referencecolumn(rockFactory, false);
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
    Referencecolumn changedOriginal = new Referencecolumn(rockFactory, false);
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