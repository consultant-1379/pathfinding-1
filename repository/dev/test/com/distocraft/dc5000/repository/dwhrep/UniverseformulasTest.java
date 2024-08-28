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
 * Test class for Universeformulas. Changes to Universeformulas table are made via
 * this class.
 */
public class UniverseformulasTest {

  private static Universeformulas objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSIONID;
  
  private static Field TECHPACK_TYPE;
  
  private static Field NAME;
  
  private static Field FORMULA;
  
  private static Field OBJECTTYPE;
  
  private static Field QUALIFICATION;
  
  private static Field AGGREGATION;
  
  private static Field ORDERNRO;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Universeformulas.class.getDeclaredField("newItem");
		VERSIONID = Universeformulas.class.getDeclaredField("VERSIONID");
		TECHPACK_TYPE = Universeformulas.class.getDeclaredField("TECHPACK_TYPE");
		NAME = Universeformulas.class.getDeclaredField("NAME");
		FORMULA = Universeformulas.class.getDeclaredField("FORMULA");
		OBJECTTYPE = Universeformulas.class.getDeclaredField("OBJECTTYPE");
		QUALIFICATION = Universeformulas.class.getDeclaredField("QUALIFICATION");
		AGGREGATION = Universeformulas.class.getDeclaredField("AGGREGATION");
		ORDERNRO = Universeformulas.class.getDeclaredField("ORDERNRO");
		newItem.setAccessible(true);
		VERSIONID.setAccessible(true);
		TECHPACK_TYPE.setAccessible(true);
		NAME.setAccessible(true);
		FORMULA.setAccessible(true);
		OBJECTTYPE.setAccessible(true);
		QUALIFICATION.setAccessible(true);
		AGGREGATION.setAccessible(true);
		ORDERNRO.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Universeformulas ( VERSIONID VARCHAR(31)  ,TECHPACK_TYPE VARCHAR(31) ,NAME VARCHAR(31) ,FORMULA VARCHAR(31) ,OBJECTTYPE VARCHAR(31) ,QUALIFICATION VARCHAR(31) ,AGGREGATION VARCHAR(31) ,ORDERNRO BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Universeformulas");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Universeformulas VALUES( 'testVERSIONID'  ,'testTECHPACK_TYPE'  ,'testNAME'  ,'testFORMULA'  ,'testOBJECTTYPE'  ,'testQUALIFICATION'  ,'testAGGREGATION'  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Universeformulas(rockFactory ,  "testVERSIONID",  "testNAME");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Universeformulas");
    objUnderTest = null;
  }
  
  /**
   * Testing Universeformulas constructor variable initialization with null values.
   */
  @Test
  public void testUniverseformulasConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Universeformulas(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + TECHPACK_TYPE.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + FORMULA.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + QUALIFICATION.get(objUnderTest)  + ", " + AGGREGATION.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Universeformulas constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testUniverseformulasConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Universeformulas(rockFactory ,  "testVERSIONID",  "testNAME");

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + TECHPACK_TYPE.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + FORMULA.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + QUALIFICATION.get(objUnderTest)  + ", " + AGGREGATION.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testTECHPACK_TYPE"  + ", testNAME"  + ", testFORMULA"  + ", testOBJECTTYPE"  + ", testQUALIFICATION"  + ", testAGGREGATION"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniverseformulasConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Universeformulas(null ,  "testVERSIONID",  "testNAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Universeformulas constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testUniverseformulasConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Universeformulas whereObject = new Universeformulas(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Universeformulas(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + TECHPACK_TYPE.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + FORMULA.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + QUALIFICATION.get(objUnderTest)  + ", " + AGGREGATION.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testTECHPACK_TYPE"  + ", testNAME"  + ", testFORMULA"  + ", testOBJECTTYPE"  + ", testQUALIFICATION"  + ", testAGGREGATION"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniverseformulasConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Universeformulas whereObject = new Universeformulas(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Universeformulas(null, whereObject);
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
    assertEquals("Universeformulas", objUnderTest.getTableName());
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
    Universeformulas whereObject = new Universeformulas(rockFactory);

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
    Universeformulas whereObject = new Universeformulas(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universeformulas");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universeformulas");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSIONID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSIONID FROM Universeformulas");
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
    
    String expected = "<Universeformulas VERSIONID=\"'testVERSIONID'\" TECHPACK_TYPE=\"'testTECHPACK_TYPE'\" NAME=\"'testNAME'\" FORMULA=\"'testFORMULA'\" OBJECTTYPE=\"'testOBJECTTYPE'\" QUALIFICATION=\"'testQUALIFICATION'\" AGGREGATION=\"'testAGGREGATION'\" ORDERNRO=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Universeformulas VERSIONID=\"'testVERSIONID'\" TECHPACK_TYPE=\"'testTECHPACK_TYPE'\" NAME=\"'testNAME'\" FORMULA=\"'testFORMULA'\" OBJECTTYPE=\"'testOBJECTTYPE'\" QUALIFICATION=\"'testQUALIFICATION'\" AGGREGATION=\"'testAGGREGATION'\" ORDERNRO=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Universeformulas>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Universeformulas ( VERSIONID, TECHPACK_TYPE, NAME, FORMULA, OBJECTTYPE, QUALIFICATION, AGGREGATION, ORDERNRO ) values "
      + "( 'testVERSIONID', 'testTECHPACK_TYPE', 'testNAME', 'testFORMULA', 'testOBJECTTYPE', 'testQUALIFICATION', 'testAGGREGATION', 1 );\n";
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
    objUnderTest.setVersionid(UniverseformulasTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(UniverseformulasTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTechpack_type() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTechpack_type(UniverseformulasTest.testStringGenerator("anotherTECHPACK_TYPE", 32));
    assertEquals(UniverseformulasTest.testStringGenerator("anotherTECHPACK_TYPE", 32), TECHPACK_TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetName() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setName(UniverseformulasTest.testStringGenerator("anotherNAME", 255));
    assertEquals(UniverseformulasTest.testStringGenerator("anotherNAME", 255), NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetFormula() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setFormula(UniverseformulasTest.testStringGenerator("anotherFORMULA", 32000));
    assertEquals(UniverseformulasTest.testStringGenerator("anotherFORMULA", 32000), FORMULA.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjecttype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjecttype(UniverseformulasTest.testStringGenerator("anotherOBJECTTYPE", 16));
    assertEquals(UniverseformulasTest.testStringGenerator("anotherOBJECTTYPE", 16), OBJECTTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetQualification() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setQualification(UniverseformulasTest.testStringGenerator("anotherQUALIFICATION", 16));
    assertEquals(UniverseformulasTest.testStringGenerator("anotherQUALIFICATION", 16), QUALIFICATION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetAggregation() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setAggregation(UniverseformulasTest.testStringGenerator("anotherAGGREGATION", 16));
    assertEquals(UniverseformulasTest.testStringGenerator("anotherAGGREGATION", 16), AGGREGATION.get(objUnderTest));
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
    String[] expectedKeys = { "VERSIONID","NAME"};

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
    objUnderTest = new Universeformulas(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSIONID.get(objUnderTest)  + ", " + TECHPACK_TYPE.get(objUnderTest)  + ", " + NAME.get(objUnderTest)  + ", " + FORMULA.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + QUALIFICATION.get(objUnderTest)  + ", " + AGGREGATION.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0" ;
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
    Universeformulas compareObj = new Universeformulas(rockFactory ,  "testVERSIONID",  "testNAME");

    /* Testing first with null primary key value */
    VERSIONID.set(objUnderTest, null);
  	NAME.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSIONID.set(objUnderTest,  "different");
  	NAME.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSIONID.set(objUnderTest,  "testVERSIONID");
  	NAME.set(objUnderTest,  "testNAME");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Universeformulas with our current one. If the two
   * Universeformulass are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnUniverseformulas() throws Exception {

    /* Creating another Universeformulas which will be compared to the tested one */
    Universeformulas comparedObj = new Universeformulas(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universeformulas with our current one. If the two
   * Universeformulass are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentUniverseformulas() throws Exception {

    /* Creating another Universeformulas which will be compared to the tested one */
    Universeformulas comparedObj = new Universeformulas(rockFactory ,  "testVERSIONID",  "testNAME");
    comparedObj.setVersionid( "DifferentVERSIONID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universeformulas with our current one. If the two
   * Universeformulass are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameUniverseformulas() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Universeformulas comparedObj = new Universeformulas(rockFactory ,  "testVERSIONID",  "testNAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universeformulas with our current one using null value.
   */
  @Test
  public void testEqualsWithNullUniverseformulas() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Universeformulas comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Universeformulas was null \n");
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
    assertEquals(Universeformulas.class, actualObject.getClass());
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
    Universeformulas testAgg = new Universeformulas(rockFactory ,  "testVERSIONID",  "testNAME");
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
   * Testing columnsize retrieving for Techpack_type.
   */
  @Test
  public void testGetTechpack_typeColumnSize() throws Exception {
    
     assertEquals(32, objUnderTest.getTechpack_typeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Techpack_type.
  */
  @Test
  public void testGetTechpack_typeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTechpack_typeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Techpack_type.
  */
  @Test
  public void testGetTechpack_typeSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTechpack_typeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Name.
   */
  @Test
  public void testGetNameColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getNameColumnSize());   
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
   * Testing columnsize retrieving for Formula.
   */
  @Test
  public void testGetFormulaColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getFormulaColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Formula.
  */
  @Test
  public void testGetFormulaDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getFormulaDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Formula.
  */
  @Test
  public void testGetFormulaSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getFormulaSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objecttype.
   */
  @Test
  public void testGetObjecttypeColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getObjecttypeColumnSize());   
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
   * Testing columnsize retrieving for Qualification.
   */
  @Test
  public void testGetQualificationColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getQualificationColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Qualification.
  */
  @Test
  public void testGetQualificationDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getQualificationDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Qualification.
  */
  @Test
  public void testGetQualificationSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getQualificationSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Aggregation.
   */
  @Test
  public void testGetAggregationColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getAggregationColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Aggregation.
  */
  @Test
  public void testGetAggregationDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getAggregationDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Aggregation.
  */
  @Test
  public void testGetAggregationSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getAggregationSQLType());    
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
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Universeformulas object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Universeformulas(rockFactory, false);
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
    Universeformulas changedOriginal = new Universeformulas(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Universeformulas(rockFactory, false);
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
    Universeformulas changedOriginal = new Universeformulas(rockFactory, false);
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