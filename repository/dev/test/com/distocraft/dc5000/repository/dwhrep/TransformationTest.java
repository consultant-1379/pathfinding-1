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
 * Test class for Transformation. Changes to Transformation table are made via
 * this class.
 */
public class TransformationTest {

  private static Transformation objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field TRANSFORMERID;
  
  private static Field ORDERNO;
  
  private static Field TYPE;
  
  private static Field SOURCE;
  
  private static Field TARGET;
  
  private static Field CONFIG;
  
  private static Field DESCRIPTION;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Transformation.class.getDeclaredField("newItem");
		TRANSFORMERID = Transformation.class.getDeclaredField("TRANSFORMERID");
		ORDERNO = Transformation.class.getDeclaredField("ORDERNO");
		TYPE = Transformation.class.getDeclaredField("TYPE");
		SOURCE = Transformation.class.getDeclaredField("SOURCE");
		TARGET = Transformation.class.getDeclaredField("TARGET");
		CONFIG = Transformation.class.getDeclaredField("CONFIG");
		DESCRIPTION = Transformation.class.getDeclaredField("DESCRIPTION");
		newItem.setAccessible(true);
		TRANSFORMERID.setAccessible(true);
		ORDERNO.setAccessible(true);
		TYPE.setAccessible(true);
		SOURCE.setAccessible(true);
		TARGET.setAccessible(true);
		CONFIG.setAccessible(true);
		DESCRIPTION.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Transformation ( TRANSFORMERID VARCHAR(31)  ,ORDERNO BIGINT  ,TYPE VARCHAR(31) ,SOURCE VARCHAR(31) ,TARGET VARCHAR(31) ,CONFIG VARCHAR(31) ,DESCRIPTION VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Transformation");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Transformation VALUES( 'testTRANSFORMERID'  ,1  ,'testTYPE'  ,'testSOURCE'  ,'testTARGET'  ,'testCONFIG'  ,'testDESCRIPTION' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Transformation(rockFactory ,  "testTRANSFORMERID",  1L );
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Transformation");
    objUnderTest = null;
  }
  
  /**
   * Testing Transformation constructor variable initialization with null values.
   */
  @Test
  public void testTransformationConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Transformation(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  TRANSFORMERID.get(objUnderTest)  + ", " + ORDERNO.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + SOURCE.get(objUnderTest)  + ", " + TARGET.get(objUnderTest)  + ", " + CONFIG.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Transformation constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testTransformationConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Transformation(rockFactory ,  "testTRANSFORMERID",  1L );

    /* Asserting that variables are initialized */
    String actual =  TRANSFORMERID.get(objUnderTest)  + ", " + ORDERNO.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + SOURCE.get(objUnderTest)  + ", " + TARGET.get(objUnderTest)  + ", " + CONFIG.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest) ;
    String expected =  "testTRANSFORMERID"  + ", 1"  + ", testTYPE"  + ", testSOURCE"  + ", testTARGET"  + ", testCONFIG"  + ", testDESCRIPTION" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testTransformationConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Transformation(null ,  "testTRANSFORMERID",  1L );
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Transformation constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testTransformationConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Transformation whereObject = new Transformation(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Transformation(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  TRANSFORMERID.get(objUnderTest)  + ", " + ORDERNO.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + SOURCE.get(objUnderTest)  + ", " + TARGET.get(objUnderTest)  + ", " + CONFIG.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest) ;
    String expected =  "testTRANSFORMERID"  + ", 1"  + ", testTYPE"  + ", testSOURCE"  + ", testTARGET"  + ", testCONFIG"  + ", testDESCRIPTION" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testTransformationConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Transformation whereObject = new Transformation(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Transformation(null, whereObject);
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
    assertEquals("Transformation", objUnderTest.getTableName());
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
    Transformation whereObject = new Transformation(rockFactory);

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
    Transformation whereObject = new Transformation(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Transformation");
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
    TRANSFORMERID.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("TRANSFORMERID");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Transformation");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the TRANSFORMERID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT TRANSFORMERID FROM Transformation");
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
    
    String expected = "<Transformation TRANSFORMERID=\"'testTRANSFORMERID'\" ORDERNO=\"1\" TYPE=\"'testTYPE'\" SOURCE=\"'testSOURCE'\" TARGET=\"'testTARGET'\" CONFIG=\"'testCONFIG'\" DESCRIPTION=\"'testDESCRIPTION'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Transformation TRANSFORMERID=\"'testTRANSFORMERID'\" ORDERNO=\"1\" TYPE=\"'testTYPE'\" SOURCE=\"'testSOURCE'\" TARGET=\"'testTARGET'\" CONFIG=\"'testCONFIG'\" DESCRIPTION=\"'testDESCRIPTION'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Transformation>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Transformation ( TRANSFORMERID, ORDERNO, TYPE, SOURCE, TARGET, CONFIG, DESCRIPTION ) values "
      + "( 'testTRANSFORMERID', 1, 'testTYPE', 'testSOURCE', 'testTARGET', 'testCONFIG', 'testDESCRIPTION' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTransformerid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTransformerid(TransformationTest.testStringGenerator("anotherTRANSFORMERID", 255));
    assertEquals(TransformationTest.testStringGenerator("anotherTRANSFORMERID", 255), TRANSFORMERID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetOrderno() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setOrderno(555L);
    assertEquals(555L, ORDERNO.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetType() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setType(TransformationTest.testStringGenerator("anotherTYPE", 128));
    assertEquals(TransformationTest.testStringGenerator("anotherTYPE", 128), TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetSource() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setSource(TransformationTest.testStringGenerator("anotherSOURCE", 128));
    assertEquals(TransformationTest.testStringGenerator("anotherSOURCE", 128), SOURCE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTarget() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTarget(TransformationTest.testStringGenerator("anotherTARGET", 128));
    assertEquals(TransformationTest.testStringGenerator("anotherTARGET", 128), TARGET.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetConfig() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setConfig(TransformationTest.testStringGenerator("anotherCONFIG", 32000));
    assertEquals(TransformationTest.testStringGenerator("anotherCONFIG", 32000), CONFIG.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(TransformationTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(TransformationTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
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
    String[] expectedKeys = { "TRANSFORMERID","ORDERNO"};

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
    objUnderTest = new Transformation(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  TRANSFORMERID.get(objUnderTest)  + ", " + ORDERNO.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + SOURCE.get(objUnderTest)  + ", " + TARGET.get(objUnderTest)  + ", " + CONFIG.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest) ;
    String expected =  ""  + ", 0"  + ", "  + ", "  + ", "  + ", "  + ", " ;
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
    Transformation compareObj = new Transformation(rockFactory ,  "testTRANSFORMERID",  1L );

    /* Testing first with null primary key value */
    TRANSFORMERID.set(objUnderTest, null);
  	ORDERNO.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    TRANSFORMERID.set(objUnderTest,  "different");
  	ORDERNO.set(objUnderTest,  7L );
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    TRANSFORMERID.set(objUnderTest,  "testTRANSFORMERID");
  	ORDERNO.set(objUnderTest,  1L );
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Transformation with our current one. If the two
   * Transformations are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnTransformation() throws Exception {

    /* Creating another Transformation which will be compared to the tested one */
    Transformation comparedObj = new Transformation(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Transformation with our current one. If the two
   * Transformations are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentTransformation() throws Exception {

    /* Creating another Transformation which will be compared to the tested one */
    Transformation comparedObj = new Transformation(rockFactory ,  "testTRANSFORMERID",  1L );
    comparedObj.setTransformerid( "DifferentTRANSFORMERID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Transformation with our current one. If the two
   * Transformations are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameTransformation() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Transformation comparedObj = new Transformation(rockFactory ,  "testTRANSFORMERID",  1L );

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Transformation with our current one using null value.
   */
  @Test
  public void testEqualsWithNullTransformation() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Transformation comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Transformation was null \n");
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
    assertEquals(Transformation.class, actualObject.getClass());
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
    Transformation testAgg = new Transformation(rockFactory ,  "testTRANSFORMERID",  1L );
    TRANSFORMERID.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Transformerid.
   */
  @Test
  public void testGetTransformeridColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getTransformeridColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Transformerid.
  */
  @Test
  public void testGetTransformeridDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTransformeridDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Transformerid.
  */
  @Test
  public void testGetTransformeridSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTransformeridSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Orderno.
   */
  @Test
  public void testGetOrdernoColumnSize() throws Exception {
    
     assertEquals(9, objUnderTest.getOrdernoColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Orderno.
  */
  @Test
  public void testGetOrdernoDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getOrdernoDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Orderno.
  */
  @Test
  public void testGetOrdernoSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getOrdernoSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Type.
   */
  @Test
  public void testGetTypeColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getTypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Type.
  */
  @Test
  public void testGetTypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Type.
  */
  @Test
  public void testGetTypeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTypeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Source.
   */
  @Test
  public void testGetSourceColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getSourceColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Source.
  */
  @Test
  public void testGetSourceDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getSourceDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Source.
  */
  @Test
  public void testGetSourceSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getSourceSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Target.
   */
  @Test
  public void testGetTargetColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getTargetColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Target.
  */
  @Test
  public void testGetTargetDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTargetDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Target.
  */
  @Test
  public void testGetTargetSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTargetSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Config.
   */
  @Test
  public void testGetConfigColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getConfigColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Config.
  */
  @Test
  public void testGetConfigDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getConfigDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Config.
  */
  @Test
  public void testGetConfigSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getConfigSQLType());    
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
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Transformation object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Transformation(rockFactory, false);
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
    Transformation changedOriginal = new Transformation(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Transformation(rockFactory, false);
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
    Transformation changedOriginal = new Transformation(rockFactory, false);
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