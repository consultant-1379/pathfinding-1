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
 * Test class for Referencetable. Changes to Referencetable table are made via
 * this class.
 */
public class ReferencetableTest {

  private static Referencetable objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field TYPEID;
  
  private static Field VERSIONID;
  
  private static Field TYPENAME;
  
  private static Field OBJECTID;
  
  private static Field OBJECTNAME;
  
  private static Field OBJECTVERSION;
  
  private static Field OBJECTTYPE;
  
  private static Field DESCRIPTION;
  
  private static Field STATUS;
  
  private static Field UPDATE_POLICY;
  
  private static Field TABLE_TYPE;
  
  private static Field DATAFORMATSUPPORT;
  
  private static Field BASEDEF;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Referencetable.class.getDeclaredField("newItem");
		TYPEID = Referencetable.class.getDeclaredField("TYPEID");
		VERSIONID = Referencetable.class.getDeclaredField("VERSIONID");
		TYPENAME = Referencetable.class.getDeclaredField("TYPENAME");
		OBJECTID = Referencetable.class.getDeclaredField("OBJECTID");
		OBJECTNAME = Referencetable.class.getDeclaredField("OBJECTNAME");
		OBJECTVERSION = Referencetable.class.getDeclaredField("OBJECTVERSION");
		OBJECTTYPE = Referencetable.class.getDeclaredField("OBJECTTYPE");
		DESCRIPTION = Referencetable.class.getDeclaredField("DESCRIPTION");
		STATUS = Referencetable.class.getDeclaredField("STATUS");
		UPDATE_POLICY = Referencetable.class.getDeclaredField("UPDATE_POLICY");
		TABLE_TYPE = Referencetable.class.getDeclaredField("TABLE_TYPE");
		DATAFORMATSUPPORT = Referencetable.class.getDeclaredField("DATAFORMATSUPPORT");
		BASEDEF = Referencetable.class.getDeclaredField("BASEDEF");
		newItem.setAccessible(true);
		TYPEID.setAccessible(true);
		VERSIONID.setAccessible(true);
		TYPENAME.setAccessible(true);
		OBJECTID.setAccessible(true);
		OBJECTNAME.setAccessible(true);
		OBJECTVERSION.setAccessible(true);
		OBJECTTYPE.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		STATUS.setAccessible(true);
		UPDATE_POLICY.setAccessible(true);
		TABLE_TYPE.setAccessible(true);
		DATAFORMATSUPPORT.setAccessible(true);
		BASEDEF.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Referencetable ( TYPEID VARCHAR(31)  ,VERSIONID VARCHAR(31) ,TYPENAME VARCHAR(31) ,OBJECTID VARCHAR(31) ,OBJECTNAME VARCHAR(31) ,OBJECTVERSION VARCHAR(31) ,OBJECTTYPE VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,STATUS BIGINT  ,UPDATE_POLICY BIGINT  ,TABLE_TYPE VARCHAR(31) ,DATAFORMATSUPPORT INTEGER  ,BASEDEF INTEGER )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Referencetable");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Referencetable VALUES( 'testTYPEID'  ,'testVERSIONID'  ,'testTYPENAME'  ,'testOBJECTID'  ,'testOBJECTNAME'  ,'testOBJECTVERSION'  ,'testOBJECTTYPE'  ,'testDESCRIPTION'  ,1  ,1  ,'testTABLE_TYPE'  ,1  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Referencetable(rockFactory ,  "testTYPEID");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Referencetable");
    objUnderTest = null;
  }
  
  /**
   * Testing Referencetable constructor variable initialization with null values.
   */
  @Test
  public void testReferencetableConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Referencetable(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + OBJECTID.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + OBJECTVERSION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + UPDATE_POLICY.get(objUnderTest)  + ", " + TABLE_TYPE.get(objUnderTest)  + ", " + DATAFORMATSUPPORT.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Referencetable constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testReferencetableConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Referencetable(rockFactory ,  "testTYPEID");

    /* Asserting that variables are initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + OBJECTID.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + OBJECTVERSION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + UPDATE_POLICY.get(objUnderTest)  + ", " + TABLE_TYPE.get(objUnderTest)  + ", " + DATAFORMATSUPPORT.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  "testTYPEID"  + ", testVERSIONID"  + ", testTYPENAME"  + ", testOBJECTID"  + ", testOBJECTNAME"  + ", testOBJECTVERSION"  + ", testOBJECTTYPE"  + ", testDESCRIPTION"  + ", 1"  + ", 1"  + ", testTABLE_TYPE"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testReferencetableConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Referencetable(null ,  "testTYPEID");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Referencetable constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testReferencetableConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Referencetable whereObject = new Referencetable(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Referencetable(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + OBJECTID.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + OBJECTVERSION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + UPDATE_POLICY.get(objUnderTest)  + ", " + TABLE_TYPE.get(objUnderTest)  + ", " + DATAFORMATSUPPORT.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  "testTYPEID"  + ", testVERSIONID"  + ", testTYPENAME"  + ", testOBJECTID"  + ", testOBJECTNAME"  + ", testOBJECTVERSION"  + ", testOBJECTTYPE"  + ", testDESCRIPTION"  + ", 1"  + ", 1"  + ", testTABLE_TYPE"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testReferencetableConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Referencetable whereObject = new Referencetable(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Referencetable(null, whereObject);
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
    assertEquals("Referencetable", objUnderTest.getTableName());
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
    Referencetable whereObject = new Referencetable(rockFactory);

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
    Referencetable whereObject = new Referencetable(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Referencetable");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Referencetable");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the TYPEID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT TYPEID FROM Referencetable");
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
    
    String expected = "<Referencetable TYPEID=\"'testTYPEID'\" VERSIONID=\"'testVERSIONID'\" TYPENAME=\"'testTYPENAME'\" OBJECTID=\"'testOBJECTID'\" OBJECTNAME=\"'testOBJECTNAME'\" OBJECTVERSION=\"'testOBJECTVERSION'\" OBJECTTYPE=\"'testOBJECTTYPE'\" DESCRIPTION=\"'testDESCRIPTION'\" STATUS=\"1\" UPDATE_POLICY=\"1\" TABLE_TYPE=\"'testTABLE_TYPE'\" DATAFORMATSUPPORT=\"1\" BASEDEF=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Referencetable TYPEID=\"'testTYPEID'\" VERSIONID=\"'testVERSIONID'\" TYPENAME=\"'testTYPENAME'\" OBJECTID=\"'testOBJECTID'\" OBJECTNAME=\"'testOBJECTNAME'\" OBJECTVERSION=\"'testOBJECTVERSION'\" OBJECTTYPE=\"'testOBJECTTYPE'\" DESCRIPTION=\"'testDESCRIPTION'\" STATUS=\"1\" UPDATE_POLICY=\"1\" TABLE_TYPE=\"'testTABLE_TYPE'\" DATAFORMATSUPPORT=\"1\" BASEDEF=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Referencetable>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Referencetable ( TYPEID, VERSIONID, TYPENAME, OBJECTID, OBJECTNAME, OBJECTVERSION, OBJECTTYPE, DESCRIPTION, STATUS, UPDATE_POLICY, TABLE_TYPE, DATAFORMATSUPPORT, BASEDEF ) values "
      + "( 'testTYPEID', 'testVERSIONID', 'testTYPENAME', 'testOBJECTID', 'testOBJECTNAME', 'testOBJECTVERSION', 'testOBJECTTYPE', 'testDESCRIPTION', 1, 1, 'testTABLE_TYPE', 1, 1 );\n";
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
    objUnderTest.setTypeid(ReferencetableTest.testStringGenerator("anotherTYPEID", 255));
    assertEquals(ReferencetableTest.testStringGenerator("anotherTYPEID", 255), TYPEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersionid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersionid(ReferencetableTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(ReferencetableTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTypename() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTypename(ReferencetableTest.testStringGenerator("anotherTYPENAME", 255));
    assertEquals(ReferencetableTest.testStringGenerator("anotherTYPENAME", 255), TYPENAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjectid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjectid(ReferencetableTest.testStringGenerator("anotherOBJECTID", 255));
    assertEquals(ReferencetableTest.testStringGenerator("anotherOBJECTID", 255), OBJECTID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjectname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjectname(ReferencetableTest.testStringGenerator("anotherOBJECTNAME", 255));
    assertEquals(ReferencetableTest.testStringGenerator("anotherOBJECTNAME", 255), OBJECTNAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjectversion() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjectversion(ReferencetableTest.testStringGenerator("anotherOBJECTVERSION", 50));
    assertEquals(ReferencetableTest.testStringGenerator("anotherOBJECTVERSION", 50), OBJECTVERSION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjecttype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjecttype(ReferencetableTest.testStringGenerator("anotherOBJECTTYPE", 255));
    assertEquals(ReferencetableTest.testStringGenerator("anotherOBJECTTYPE", 255), OBJECTTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(ReferencetableTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(ReferencetableTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
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
  public void testSetAndGetUpdate_policy() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUpdate_policy(555L);
    assertEquals(555L, UPDATE_POLICY.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTable_type() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTable_type(ReferencetableTest.testStringGenerator("anotherTABLE_TYPE", 12));
    assertEquals(ReferencetableTest.testStringGenerator("anotherTABLE_TYPE", 12), TABLE_TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataformatsupport() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataformatsupport(555);
    assertEquals(555, DATAFORMATSUPPORT.get(objUnderTest));
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
    String[] expectedKeys = { "TYPEID"};

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
    objUnderTest = new Referencetable(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  TYPEID.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + OBJECTID.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + OBJECTVERSION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + UPDATE_POLICY.get(objUnderTest)  + ", " + TABLE_TYPE.get(objUnderTest)  + ", " + DATAFORMATSUPPORT.get(objUnderTest)  + ", " + BASEDEF.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", "  + ", 0"  + ", 0" ;
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
    Referencetable compareObj = new Referencetable(rockFactory ,  "testTYPEID");

    /* Testing first with null primary key value */
    TYPEID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    TYPEID.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    TYPEID.set(objUnderTest,  "testTYPEID");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Referencetable with our current one. If the two
   * Referencetables are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnReferencetable() throws Exception {

    /* Creating another Referencetable which will be compared to the tested one */
    Referencetable comparedObj = new Referencetable(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Referencetable with our current one. If the two
   * Referencetables are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentReferencetable() throws Exception {

    /* Creating another Referencetable which will be compared to the tested one */
    Referencetable comparedObj = new Referencetable(rockFactory ,  "testTYPEID");
    comparedObj.setTypeid( "DifferentTYPEID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Referencetable with our current one. If the two
   * Referencetables are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameReferencetable() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Referencetable comparedObj = new Referencetable(rockFactory ,  "testTYPEID");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Referencetable with our current one using null value.
   */
  @Test
  public void testEqualsWithNullReferencetable() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Referencetable comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Referencetable was null \n");
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
    assertEquals(Referencetable.class, actualObject.getClass());
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
    Referencetable testAgg = new Referencetable(rockFactory ,  "testTYPEID");
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
   * Testing columnsize retrieving for Typename.
   */
  @Test
  public void testGetTypenameColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getTypenameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Typename.
  */
  @Test
  public void testGetTypenameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTypenameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Typename.
  */
  @Test
  public void testGetTypenameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTypenameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objectid.
   */
  @Test
  public void testGetObjectidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getObjectidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objectid.
  */
  @Test
  public void testGetObjectidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjectidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objectid.
  */
  @Test
  public void testGetObjectidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjectidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objectname.
   */
  @Test
  public void testGetObjectnameColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getObjectnameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objectname.
  */
  @Test
  public void testGetObjectnameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjectnameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objectname.
  */
  @Test
  public void testGetObjectnameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjectnameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objectversion.
   */
  @Test
  public void testGetObjectversionColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getObjectversionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objectversion.
  */
  @Test
  public void testGetObjectversionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjectversionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objectversion.
  */
  @Test
  public void testGetObjectversionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjectversionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objecttype.
   */
  @Test
  public void testGetObjecttypeColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getObjecttypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objecttype.
  */
  @Test
  public void testGetObjecttypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjecttypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objecttype.
  */
  @Test
  public void testGetObjecttypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjecttypeSQLType());    
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
   * Testing columnsize retrieving for Update_policy.
   */
  @Test
  public void testGetUpdate_policyColumnSize() throws Exception {
    
     assertEquals(9, objUnderTest.getUpdate_policyColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Update_policy.
  */
  @Test
  public void testGetUpdate_policyDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUpdate_policyDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Update_policy.
  */
  @Test
  public void testGetUpdate_policySQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getUpdate_policySQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Table_type.
   */
  @Test
  public void testGetTable_typeColumnSize() throws Exception {
    
     assertEquals(12, objUnderTest.getTable_typeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Table_type.
  */
  @Test
  public void testGetTable_typeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTable_typeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Table_type.
  */
  @Test
  public void testGetTable_typeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTable_typeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Dataformatsupport.
   */
  @Test
  public void testGetDataformatsupportColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getDataformatsupportColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Dataformatsupport.
  */
  @Test
  public void testGetDataformatsupportDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDataformatsupportDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Dataformatsupport.
  */
  @Test
  public void testGetDataformatsupportSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getDataformatsupportSQLType());    
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
   * Testing original Referencetable object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Referencetable(rockFactory, false);
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
    Referencetable changedOriginal = new Referencetable(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Referencetable(rockFactory, false);
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
    Referencetable changedOriginal = new Referencetable(rockFactory, false);
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