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
 * Test class for Grouptypes. Changes to Grouptypes table are made via
 * this class.
 */
public class GrouptypesTest {

  private static Grouptypes objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field GROUPTYPE;
  
  private static Field VERSIONID;
  
  private static Field DATANAME;
  
  private static Field DATATYPE;
  
  private static Field DATASIZE;
  
  private static Field DATASCALE;
  
  private static Field NULLABLE;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Grouptypes.class.getDeclaredField("newItem");
		GROUPTYPE = Grouptypes.class.getDeclaredField("GROUPTYPE");
		VERSIONID = Grouptypes.class.getDeclaredField("VERSIONID");
		DATANAME = Grouptypes.class.getDeclaredField("DATANAME");
		DATATYPE = Grouptypes.class.getDeclaredField("DATATYPE");
		DATASIZE = Grouptypes.class.getDeclaredField("DATASIZE");
		DATASCALE = Grouptypes.class.getDeclaredField("DATASCALE");
		NULLABLE = Grouptypes.class.getDeclaredField("NULLABLE");
		newItem.setAccessible(true);
		GROUPTYPE.setAccessible(true);
		VERSIONID.setAccessible(true);
		DATANAME.setAccessible(true);
		DATATYPE.setAccessible(true);
		DATASIZE.setAccessible(true);
		DATASCALE.setAccessible(true);
		NULLABLE.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Grouptypes ( GROUPTYPE VARCHAR(31)  ,VERSIONID VARCHAR(31) ,DATANAME VARCHAR(31) ,DATATYPE VARCHAR(31) ,DATASIZE INTEGER  ,DATASCALE INTEGER  ,NULLABLE INTEGER )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Grouptypes");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Grouptypes VALUES( 'testGROUPTYPE'  ,'testVERSIONID'  ,'testDATANAME'  ,'testDATATYPE'  ,1  ,1  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Grouptypes(rockFactory ,  "testGROUPTYPE",  "testVERSIONID",  "testDATANAME");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Grouptypes");
    objUnderTest = null;
  }
  
  /**
   * Testing Grouptypes constructor variable initialization with null values.
   */
  @Test
  public void testGrouptypesConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Grouptypes(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  GROUPTYPE.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Grouptypes constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testGrouptypesConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Grouptypes(rockFactory ,  "testGROUPTYPE",  "testVERSIONID",  "testDATANAME");

    /* Asserting that variables are initialized */
    String actual =  GROUPTYPE.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest) ;
    String expected =  "testGROUPTYPE"  + ", testVERSIONID"  + ", testDATANAME"  + ", testDATATYPE"  + ", 1"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testGrouptypesConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Grouptypes(null ,  "testGROUPTYPE",  "testVERSIONID",  "testDATANAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Grouptypes constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testGrouptypesConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Grouptypes whereObject = new Grouptypes(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Grouptypes(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  GROUPTYPE.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest) ;
    String expected =  "testGROUPTYPE"  + ", testVERSIONID"  + ", testDATANAME"  + ", testDATATYPE"  + ", 1"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testGrouptypesConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Grouptypes whereObject = new Grouptypes(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Grouptypes(null, whereObject);
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
    assertEquals("Grouptypes", objUnderTest.getTableName());
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
    Grouptypes whereObject = new Grouptypes(rockFactory);

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
    Grouptypes whereObject = new Grouptypes(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Grouptypes");
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
    GROUPTYPE.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("GROUPTYPE");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Grouptypes");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the GROUPTYPE column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT GROUPTYPE FROM Grouptypes");
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
    
    String expected = "<Grouptypes GROUPTYPE=\"'testGROUPTYPE'\" VERSIONID=\"'testVERSIONID'\" DATANAME=\"'testDATANAME'\" DATATYPE=\"'testDATATYPE'\" DATASIZE=\"1\" DATASCALE=\"1\" NULLABLE=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Grouptypes GROUPTYPE=\"'testGROUPTYPE'\" VERSIONID=\"'testVERSIONID'\" DATANAME=\"'testDATANAME'\" DATATYPE=\"'testDATATYPE'\" DATASIZE=\"1\" DATASCALE=\"1\" NULLABLE=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Grouptypes>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Grouptypes ( GROUPTYPE, VERSIONID, DATANAME, DATATYPE, DATASIZE, DATASCALE, NULLABLE ) values "
      + "( 'testGROUPTYPE', 'testVERSIONID', 'testDATANAME', 'testDATATYPE', 1, 1, 1 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetGrouptype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setGrouptype(GrouptypesTest.testStringGenerator("anotherGROUPTYPE", 64));
    assertEquals(GrouptypesTest.testStringGenerator("anotherGROUPTYPE", 64), GROUPTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVersionid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVersionid(GrouptypesTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(GrouptypesTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataname(GrouptypesTest.testStringGenerator("anotherDATANAME", 128));
    assertEquals(GrouptypesTest.testStringGenerator("anotherDATANAME", 128), DATANAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDatatype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDatatype(GrouptypesTest.testStringGenerator("anotherDATATYPE", 50));
    assertEquals(GrouptypesTest.testStringGenerator("anotherDATATYPE", 50), DATATYPE.get(objUnderTest));
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
  public void testSetAndGetNullable() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setNullable(555);
    assertEquals(555, NULLABLE.get(objUnderTest));
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
    String[] expectedKeys = { "GROUPTYPE","VERSIONID","DATANAME"};

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
    objUnderTest = new Grouptypes(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  GROUPTYPE.get(objUnderTest)  + ", " + VERSIONID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + DATATYPE.get(objUnderTest)  + ", " + DATASIZE.get(objUnderTest)  + ", " + DATASCALE.get(objUnderTest)  + ", " + NULLABLE.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", 0" ;
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
    Grouptypes compareObj = new Grouptypes(rockFactory ,  "testGROUPTYPE",  "testVERSIONID",  "testDATANAME");

    /* Testing first with null primary key value */
    GROUPTYPE.set(objUnderTest, null);
  	VERSIONID.set(objUnderTest, null);
  	DATANAME.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    GROUPTYPE.set(objUnderTest,  "different");
  	VERSIONID.set(objUnderTest,  "different");
  	DATANAME.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    GROUPTYPE.set(objUnderTest,  "testGROUPTYPE");
  	VERSIONID.set(objUnderTest,  "testVERSIONID");
  	DATANAME.set(objUnderTest,  "testDATANAME");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Grouptypes with our current one. If the two
   * Grouptypess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnGrouptypes() throws Exception {

    /* Creating another Grouptypes which will be compared to the tested one */
    Grouptypes comparedObj = new Grouptypes(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Grouptypes with our current one. If the two
   * Grouptypess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentGrouptypes() throws Exception {

    /* Creating another Grouptypes which will be compared to the tested one */
    Grouptypes comparedObj = new Grouptypes(rockFactory ,  "testGROUPTYPE",  "testVERSIONID",  "testDATANAME");
    comparedObj.setGrouptype( "DifferentGROUPTYPE");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Grouptypes with our current one. If the two
   * Grouptypess are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameGrouptypes() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Grouptypes comparedObj = new Grouptypes(rockFactory ,  "testGROUPTYPE",  "testVERSIONID",  "testDATANAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Grouptypes with our current one using null value.
   */
  @Test
  public void testEqualsWithNullGrouptypes() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Grouptypes comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Grouptypes was null \n");
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
    assertEquals(Grouptypes.class, actualObject.getClass());
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
    Grouptypes testAgg = new Grouptypes(rockFactory ,  "testGROUPTYPE",  "testVERSIONID",  "testDATANAME");
    GROUPTYPE.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Grouptype.
   */
  @Test
  public void testGetGrouptypeColumnSize() throws Exception {
    
     assertEquals(64, objUnderTest.getGrouptypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Grouptype.
  */
  @Test
  public void testGetGrouptypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getGrouptypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Grouptype.
  */
  @Test
  public void testGetGrouptypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getGrouptypeSQLType());    
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
   * Testing columnsize retrieving for Nullable.
   */
  @Test
  public void testGetNullableColumnSize() throws Exception {
    
     assertEquals(5, objUnderTest.getNullableColumnSize());   
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
    
    assertEquals(5, objUnderTest.getNullableSQLType());    
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
   * Testing original Grouptypes object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Grouptypes(rockFactory, false);
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
    Grouptypes changedOriginal = new Grouptypes(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Grouptypes(rockFactory, false);
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
    Grouptypes changedOriginal = new Grouptypes(rockFactory, false);
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