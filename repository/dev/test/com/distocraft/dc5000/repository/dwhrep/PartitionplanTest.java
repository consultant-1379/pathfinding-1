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
 * Test class for Partitionplan. Changes to Partitionplan table are made via
 * this class.
 */
public class PartitionplanTest {

  private static Partitionplan objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field PARTITIONPLAN;
  
  private static Field DEFAULTSTORAGETIME;
  
  private static Field DEFAULTPARTITIONSIZE;
  
  private static Field MAXSTORAGETIME;
  
  private static Field PARTITIONTYPE;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Partitionplan.class.getDeclaredField("newItem");
		PARTITIONPLAN = Partitionplan.class.getDeclaredField("PARTITIONPLAN");
		DEFAULTSTORAGETIME = Partitionplan.class.getDeclaredField("DEFAULTSTORAGETIME");
		DEFAULTPARTITIONSIZE = Partitionplan.class.getDeclaredField("DEFAULTPARTITIONSIZE");
		MAXSTORAGETIME = Partitionplan.class.getDeclaredField("MAXSTORAGETIME");
		PARTITIONTYPE = Partitionplan.class.getDeclaredField("PARTITIONTYPE");
		newItem.setAccessible(true);
		PARTITIONPLAN.setAccessible(true);
		DEFAULTSTORAGETIME.setAccessible(true);
		DEFAULTPARTITIONSIZE.setAccessible(true);
		MAXSTORAGETIME.setAccessible(true);
		PARTITIONTYPE.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Partitionplan ( PARTITIONPLAN VARCHAR(31)  ,DEFAULTSTORAGETIME BIGINT  ,DEFAULTPARTITIONSIZE BIGINT  ,MAXSTORAGETIME BIGINT  ,PARTITIONTYPE SMALLINT)");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Partitionplan");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Partitionplan VALUES( 'testPARTITIONPLAN'  ,1  ,1  ,1  ,3 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Partitionplan(rockFactory ,  "testPARTITIONPLAN");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Partitionplan");
    objUnderTest = null;
  }
  
  /**
   * Testing Partitionplan constructor variable initialization with null values.
   */
  @Test
  public void testPartitionplanConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Partitionplan(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  PARTITIONPLAN.get(objUnderTest)  + ", " + DEFAULTSTORAGETIME.get(objUnderTest)  + ", " + DEFAULTPARTITIONSIZE.get(objUnderTest)  + ", " + MAXSTORAGETIME.get(objUnderTest)  + ", " + PARTITIONTYPE.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Partitionplan constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testPartitionplanConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Partitionplan(rockFactory ,  "testPARTITIONPLAN");

    /* Asserting that variables are initialized */
    String actual =  PARTITIONPLAN.get(objUnderTest)  + ", " + DEFAULTSTORAGETIME.get(objUnderTest)  + ", " + DEFAULTPARTITIONSIZE.get(objUnderTest)  + ", " + MAXSTORAGETIME.get(objUnderTest)  + ", " + PARTITIONTYPE.get(objUnderTest) ;
    String expected =  "testPARTITIONPLAN"  + ", 1"  + ", 1"  + ", 1"  + ", 3" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testPartitionplanConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Partitionplan(null ,  "testPARTITIONPLAN");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Partitionplan constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testPartitionplanConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Partitionplan whereObject = new Partitionplan(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Partitionplan(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  PARTITIONPLAN.get(objUnderTest)  + ", " + DEFAULTSTORAGETIME.get(objUnderTest)  + ", " + DEFAULTPARTITIONSIZE.get(objUnderTest)  + ", " + MAXSTORAGETIME.get(objUnderTest)  + ", " + PARTITIONTYPE.get(objUnderTest) ;
    String expected =  "testPARTITIONPLAN"  + ", 1"  + ", 1"  + ", 1"  + ", 3" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testPartitionplanConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Partitionplan whereObject = new Partitionplan(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Partitionplan(null, whereObject);
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
    assertEquals("Partitionplan", objUnderTest.getTableName());
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
    Partitionplan whereObject = new Partitionplan(rockFactory);

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
    Partitionplan whereObject = new Partitionplan(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Partitionplan");
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
    PARTITIONPLAN.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("PARTITIONPLAN");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Partitionplan");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the PARTITIONPLAN column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT PARTITIONPLAN FROM Partitionplan");
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
    
    String expected = "<Partitionplan PARTITIONPLAN=\"'testPARTITIONPLAN'\" DEFAULTSTORAGETIME=\"1\" DEFAULTPARTITIONSIZE=\"1\" MAXSTORAGETIME=\"1\" PARTITIONTYPE=\"3\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Partitionplan PARTITIONPLAN=\"'testPARTITIONPLAN'\" DEFAULTSTORAGETIME=\"1\" DEFAULTPARTITIONSIZE=\"1\" MAXSTORAGETIME=\"1\" PARTITIONTYPE=\"3\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Partitionplan>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Partitionplan ( PARTITIONPLAN, DEFAULTSTORAGETIME, DEFAULTPARTITIONSIZE, MAXSTORAGETIME, PARTITIONTYPE ) values "
      + "( 'testPARTITIONPLAN', 1, 1, 1, 3 );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPartitionplan() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPartitionplan(PartitionplanTest.testStringGenerator("anotherPARTITIONPLAN", 128));
    assertEquals(PartitionplanTest.testStringGenerator("anotherPARTITIONPLAN", 128), PARTITIONPLAN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDefaultstoragetime() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDefaultstoragetime(555L);
    assertEquals(555L, DEFAULTSTORAGETIME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDefaultpartitionsize() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDefaultpartitionsize(555L);
    assertEquals(555L, DEFAULTPARTITIONSIZE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetMaxstoragetime() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setMaxstoragetime(555L);
    assertEquals(555L, MAXSTORAGETIME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPartitiontype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPartitiontype( (short)3);
    assertEquals( (short)3, PARTITIONTYPE.get(objUnderTest));
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
    String[] expectedKeys = { "PARTITIONPLAN"};

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
    objUnderTest = new Partitionplan(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  PARTITIONPLAN.get(objUnderTest)  + ", " + DEFAULTSTORAGETIME.get(objUnderTest)  + ", " + DEFAULTPARTITIONSIZE.get(objUnderTest)  + ", " + MAXSTORAGETIME.get(objUnderTest)  + ", " + PARTITIONTYPE.get(objUnderTest) ;
    String expected =  ""  + ", 0"  + ", 0"  + ", 0"  + ", 0" ;
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
    Partitionplan compareObj = new Partitionplan(rockFactory ,  "testPARTITIONPLAN");

    /* Testing first with null primary key value */
    PARTITIONPLAN.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    PARTITIONPLAN.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    PARTITIONPLAN.set(objUnderTest,  "testPARTITIONPLAN");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Partitionplan with our current one. If the two
   * Partitionplans are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnPartitionplan() throws Exception {

    /* Creating another Partitionplan which will be compared to the tested one */
    Partitionplan comparedObj = new Partitionplan(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Partitionplan with our current one. If the two
   * Partitionplans are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentPartitionplan() throws Exception {

    /* Creating another Partitionplan which will be compared to the tested one */
    Partitionplan comparedObj = new Partitionplan(rockFactory ,  "testPARTITIONPLAN");
    comparedObj.setPartitionplan( "DifferentPARTITIONPLAN");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Partitionplan with our current one. If the two
   * Partitionplans are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSamePartitionplan() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Partitionplan comparedObj = new Partitionplan(rockFactory ,  "testPARTITIONPLAN");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Partitionplan with our current one using null value.
   */
  @Test
  public void testEqualsWithNullPartitionplan() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Partitionplan comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Partitionplan was null \n");
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
    assertEquals(Partitionplan.class, actualObject.getClass());
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
    Partitionplan testAgg = new Partitionplan(rockFactory ,  "testPARTITIONPLAN");
    PARTITIONPLAN.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Partitionplan.
   */
  @Test
  public void testGetPartitionplanColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getPartitionplanColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Partitionplan.
  */
  @Test
  public void testGetPartitionplanDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPartitionplanDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Partitionplan.
  */
  @Test
  public void testGetPartitionplanSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPartitionplanSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Defaultstoragetime.
   */
  @Test
  public void testGetDefaultstoragetimeColumnSize() throws Exception {
    
     assertEquals(20, objUnderTest.getDefaultstoragetimeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Defaultstoragetime.
  */
  @Test
  public void testGetDefaultstoragetimeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDefaultstoragetimeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Defaultstoragetime.
  */
  @Test
  public void testGetDefaultstoragetimeSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getDefaultstoragetimeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Defaultpartitionsize.
   */
  @Test
  public void testGetDefaultpartitionsizeColumnSize() throws Exception {
    
     assertEquals(20, objUnderTest.getDefaultpartitionsizeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Defaultpartitionsize.
  */
  @Test
  public void testGetDefaultpartitionsizeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDefaultpartitionsizeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Defaultpartitionsize.
  */
  @Test
  public void testGetDefaultpartitionsizeSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getDefaultpartitionsizeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Maxstoragetime.
   */
  @Test
  public void testGetMaxstoragetimeColumnSize() throws Exception {
    
     assertEquals(20, objUnderTest.getMaxstoragetimeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Maxstoragetime.
  */
  @Test
  public void testGetMaxstoragetimeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getMaxstoragetimeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Maxstoragetime.
  */
  @Test
  public void testGetMaxstoragetimeSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getMaxstoragetimeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Partitiontype.
   */
  @Test
  public void testGetPartitiontypeColumnSize() throws Exception {
    
     assertEquals(3, objUnderTest.getPartitiontypeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Partitiontype.
  */
  @Test
  public void testGetPartitiontypeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPartitiontypeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Partitiontype.
  */
  @Test
  public void testGetPartitiontypeSQLType() throws Exception {
    
    assertEquals(-6, objUnderTest.getPartitiontypeSQLType());    
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
   * Testing original Partitionplan object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Partitionplan(rockFactory, false);
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
    Partitionplan changedOriginal = new Partitionplan(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Partitionplan(rockFactory, false);
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
    Partitionplan changedOriginal = new Partitionplan(rockFactory, false);
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