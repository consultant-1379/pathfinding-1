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
 * Test class for Measurementvector. Changes to Measurementvector table are made via
 * this class.
 */
public class MeasurementvectorTest {

  private static Measurementvector objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field TYPEID;
  
  private static Field DATANAME;
  
  private static Field VENDORRELEASE;
  
  private static Field VINDEX;
  
  private static Field VFROM;
  
  private static Field VTO;
  
  private static Field MEASURE;
  
  private static Field QUANTITY;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Measurementvector.class.getDeclaredField("newItem");
		TYPEID = Measurementvector.class.getDeclaredField("TYPEID");
		DATANAME = Measurementvector.class.getDeclaredField("DATANAME");
		VENDORRELEASE = Measurementvector.class.getDeclaredField("VENDORRELEASE");
		VINDEX = Measurementvector.class.getDeclaredField("VINDEX");
		VFROM = Measurementvector.class.getDeclaredField("VFROM");
		VTO = Measurementvector.class.getDeclaredField("VTO");
		MEASURE = Measurementvector.class.getDeclaredField("MEASURE");
		QUANTITY = Measurementvector.class.getDeclaredField("QUANTITY");
		newItem.setAccessible(true);
		TYPEID.setAccessible(true);
		DATANAME.setAccessible(true);
		VENDORRELEASE.setAccessible(true);
		VINDEX.setAccessible(true);
		VFROM.setAccessible(true);
		VTO.setAccessible(true);
		MEASURE.setAccessible(true);
		QUANTITY.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Measurementvector ( TYPEID VARCHAR(31)  ,DATANAME VARCHAR(31) ,VENDORRELEASE VARCHAR(31) ,VINDEX BIGINT  ,VFROM VARCHAR(31) ,VTO VARCHAR(31) ,MEASURE VARCHAR(31) ,QUANTITY BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Measurementvector");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Measurementvector VALUES( 'testTYPEID'  ,'testDATANAME'  ,'testVENDORRELEASE'  ,1  ,'testVFROM'  ,'testVTO'  ,'testMEASURE'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Measurementvector(rockFactory ,  "testTYPEID",  "testDATANAME",  "testVENDORRELEASE",  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Measurementvector");
    objUnderTest = null;
  }
  
  /**
   * Testing Measurementvector constructor variable initialization with null values.
   */
  @Test
  public void testMeasurementvectorConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Measurementvector(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + VENDORRELEASE.get(objUnderTest)  + ", " + VINDEX.get(objUnderTest)  + ", " + VFROM.get(objUnderTest)  + ", " + VTO.get(objUnderTest)  + ", " + MEASURE.get(objUnderTest)  + ", " + QUANTITY.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Measurementvector constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeasurementvectorConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Measurementvector(rockFactory ,  "testTYPEID",  "testDATANAME",  "testVENDORRELEASE",  1L );

    /* Asserting that variables are initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + VENDORRELEASE.get(objUnderTest)  + ", " + VINDEX.get(objUnderTest)  + ", " + VFROM.get(objUnderTest)  + ", " + VTO.get(objUnderTest)  + ", " + MEASURE.get(objUnderTest)  + ", " + QUANTITY.get(objUnderTest) ;
    String expected =  "testTYPEID"  + ", testDATANAME"  + ", testVENDORRELEASE"  + ", 1"  + ", testVFROM"  + ", testVTO"  + ", testMEASURE"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementvectorConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Measurementvector(null ,  "testTYPEID",  "testDATANAME",  "testVENDORRELEASE",  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Measurementvector constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testMeasurementvectorConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Measurementvector whereObject = new Measurementvector(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Measurementvector(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + VENDORRELEASE.get(objUnderTest)  + ", " + VINDEX.get(objUnderTest)  + ", " + VFROM.get(objUnderTest)  + ", " + VTO.get(objUnderTest)  + ", " + MEASURE.get(objUnderTest)  + ", " + QUANTITY.get(objUnderTest) ;
    String expected =  "testTYPEID"  + ", testDATANAME"  + ", testVENDORRELEASE"  + ", 1"  + ", testVFROM"  + ", testVTO"  + ", testMEASURE"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testMeasurementvectorConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Measurementvector whereObject = new Measurementvector(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Measurementvector(null, whereObject);
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
    assertEquals("Measurementvector", objUnderTest.getTableName());
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
    Measurementvector whereObject = new Measurementvector(rockFactory);

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
    Measurementvector whereObject = new Measurementvector(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementvector");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Measurementvector");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the TYPEID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT TYPEID FROM Measurementvector");
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
    
    String expected = "<Measurementvector TYPEID=\"'testTYPEID'\" DATANAME=\"'testDATANAME'\" VENDORRELEASE=\"'testVENDORRELEASE'\" VINDEX=\"1\" VFROM=\"'testVFROM'\" VTO=\"'testVTO'\" MEASURE=\"'testMEASURE'\" QUANTITY=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Measurementvector TYPEID=\"'testTYPEID'\" DATANAME=\"'testDATANAME'\" VENDORRELEASE=\"'testVENDORRELEASE'\" VINDEX=\"1\" VFROM=\"'testVFROM'\" VTO=\"'testVTO'\" MEASURE=\"'testMEASURE'\" QUANTITY=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Measurementvector>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Measurementvector ( TYPEID, DATANAME, VENDORRELEASE, VINDEX, VFROM, VTO, MEASURE, QUANTITY ) values "
      + "( 'testTYPEID', 'testDATANAME', 'testVENDORRELEASE', 1, 'testVFROM', 'testVTO', 'testMEASURE', 1 );\n";
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
    objUnderTest.setTypeid(MeasurementvectorTest.testStringGenerator("anotherTYPEID", 255));
    assertEquals(MeasurementvectorTest.testStringGenerator("anotherTYPEID", 255), TYPEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDataname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDataname(MeasurementvectorTest.testStringGenerator("anotherDATANAME", 128));
    assertEquals(MeasurementvectorTest.testStringGenerator("anotherDATANAME", 128), DATANAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVendorrelease() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVendorrelease(MeasurementvectorTest.testStringGenerator("anotherVENDORRELEASE", 16));
    assertEquals(MeasurementvectorTest.testStringGenerator("anotherVENDORRELEASE", 16), VENDORRELEASE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVindex() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVindex(555L);
    assertEquals(555L, VINDEX.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVfrom() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVfrom(MeasurementvectorTest.testStringGenerator("anotherVFROM", 255));
    assertEquals(MeasurementvectorTest.testStringGenerator("anotherVFROM", 255), VFROM.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetVto() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setVto(MeasurementvectorTest.testStringGenerator("anotherVTO", 255));
    assertEquals(MeasurementvectorTest.testStringGenerator("anotherVTO", 255), VTO.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMeasure() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMeasure(MeasurementvectorTest.testStringGenerator("anotherMEASURE", 255));
    assertEquals(MeasurementvectorTest.testStringGenerator("anotherMEASURE", 255), MEASURE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetQuantity() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setQuantity(555L);
    assertEquals(555L, QUANTITY.get(objUnderTest));
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
    String[] expectedKeys = { "TYPEID","DATANAME","VENDORRELEASE","VINDEX"};

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
    objUnderTest = new Measurementvector(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  TYPEID.get(objUnderTest)  + ", " + DATANAME.get(objUnderTest)  + ", " + VENDORRELEASE.get(objUnderTest)  + ", " + VINDEX.get(objUnderTest)  + ", " + VFROM.get(objUnderTest)  + ", " + VTO.get(objUnderTest)  + ", " + MEASURE.get(objUnderTest)  + ", " + QUANTITY.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", 0"  + ", "  + ", "  + ", "  + ", 0" ;
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
    Measurementvector compareObj = new Measurementvector(rockFactory ,  "testTYPEID",  "testDATANAME",  "testVENDORRELEASE",  1L );

    /* Testing first with null primary key value */
    TYPEID.set(objUnderTest, null);
  	DATANAME.set(objUnderTest, null);
  	VENDORRELEASE.set(objUnderTest, null);
  	VINDEX.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    TYPEID.set(objUnderTest,  "different");
  	DATANAME.set(objUnderTest,  "different");
  	VENDORRELEASE.set(objUnderTest,  "different");
  	VINDEX.set(objUnderTest,  7L );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    TYPEID.set(objUnderTest,  "testTYPEID");
  	DATANAME.set(objUnderTest,  "testDATANAME");
  	VENDORRELEASE.set(objUnderTest,  "testVENDORRELEASE");
  	VINDEX.set(objUnderTest,  1L );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Measurementvector with our current one. If the two
   * Measurementvectors are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnMeasurementvector() throws Exception {

    /* Creating another Measurementvector which will be compared to the tested one */
    Measurementvector comparedObj = new Measurementvector(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementvector with our current one. If the two
   * Measurementvectors are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentMeasurementvector() throws Exception {

    /* Creating another Measurementvector which will be compared to the tested one */
    Measurementvector comparedObj = new Measurementvector(rockFactory ,  "testTYPEID",  "testDATANAME",  "testVENDORRELEASE",  1L );
    comparedObj.setTypeid( "DifferentTYPEID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementvector with our current one. If the two
   * Measurementvectors are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameMeasurementvector() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Measurementvector comparedObj = new Measurementvector(rockFactory ,  "testTYPEID",  "testDATANAME",  "testVENDORRELEASE",  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Measurementvector with our current one using null value.
   */
  @Test
  public void testEqualsWithNullMeasurementvector() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Measurementvector comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Measurementvector was null \n");
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
    assertEquals(Measurementvector.class, actualObject.getClass());
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
    Measurementvector testAgg = new Measurementvector(rockFactory ,  "testTYPEID",  "testDATANAME",  "testVENDORRELEASE",  1L );
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
   * Testing columnsize retrieving for Vendorrelease.
   */
  @Test
  public void testGetVendorreleaseColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getVendorreleaseColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Vendorrelease.
  */
  @Test
  public void testGetVendorreleaseDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getVendorreleaseDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Vendorrelease.
  */
  @Test
  public void testGetVendorreleaseSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getVendorreleaseSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Vindex.
   */
  @Test
  public void testGetVindexColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getVindexColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Vindex.
  */
  @Test
  public void testGetVindexDecimalDigits() throws Exception {
    
     assertEquals(6, objUnderTest.getVindexDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Vindex.
  */
  @Test
  public void testGetVindexSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getVindexSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Vfrom.
   */
  @Test
  public void testGetVfromColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getVfromColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Vfrom.
  */
  @Test
  public void testGetVfromDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getVfromDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Vfrom.
  */
  @Test
  public void testGetVfromSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getVfromSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Vto.
   */
  @Test
  public void testGetVtoColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getVtoColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Vto.
  */
  @Test
  public void testGetVtoDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getVtoDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Vto.
  */
  @Test
  public void testGetVtoSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getVtoSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Measure.
   */
  @Test
  public void testGetMeasureColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getMeasureColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Measure.
  */
  @Test
  public void testGetMeasureDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMeasureDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Measure.
  */
  @Test
  public void testGetMeasureSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getMeasureSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Quantity.
   */
  @Test
  public void testGetQuantityColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getQuantityColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Quantity.
  */
  @Test
  public void testGetQuantityDecimalDigits() throws Exception {
    
     assertEquals(6, objUnderTest.getQuantityDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Quantity.
  */
  @Test
  public void testGetQuantitySQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getQuantitySQLType());    
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
   * Testing original Measurementvector object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Measurementvector(rockFactory, false);
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
    Measurementvector changedOriginal = new Measurementvector(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Measurementvector(rockFactory, false);
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
    Measurementvector changedOriginal = new Measurementvector(rockFactory, false);
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