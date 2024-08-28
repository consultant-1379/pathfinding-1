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
 * Test class for Useraccount. Changes to Useraccount table are made via
 * this class.
 */
public class UseraccountTest {

  private static Useraccount objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field NAME;
  
  private static Field PASSWORD;
  
  private static Field ROLE;
  
  private static Field LASTLOGIN;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Useraccount.class.getDeclaredField("newItem");
		NAME = Useraccount.class.getDeclaredField("NAME");
		PASSWORD = Useraccount.class.getDeclaredField("PASSWORD");
		ROLE = Useraccount.class.getDeclaredField("ROLE");
		LASTLOGIN = Useraccount.class.getDeclaredField("LASTLOGIN");
		newItem.setAccessible(true);
		NAME.setAccessible(true);
		PASSWORD.setAccessible(true);
		ROLE.setAccessible(true);
		LASTLOGIN.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Useraccount ( NAME VARCHAR(31)  ,PASSWORD VARCHAR(31) ,ROLE VARCHAR(31) ,LASTLOGIN TIMESTAMP )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Useraccount");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Useraccount VALUES( 'testNAME'  ,'testPASSWORD'  ,'testROLE'  ,'2000-01-01 00:00:00.0' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Useraccount(rockFactory ,  "testNAME");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Useraccount");
    objUnderTest = null;
  }
  
  /**
   * Testing Useraccount constructor variable initialization with null values.
   */
  @Test
  public void testUseraccountConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Useraccount(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  NAME.get(objUnderTest)  + ", " + PASSWORD.get(objUnderTest)  + ", " + ROLE.get(objUnderTest)  + ", " + LASTLOGIN.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Useraccount constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testUseraccountConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Useraccount(rockFactory ,  "testNAME");

    /* Asserting that variables are initialized */
    String actual =  NAME.get(objUnderTest)  + ", " + PASSWORD.get(objUnderTest)  + ", " + ROLE.get(objUnderTest)  + ", " + LASTLOGIN.get(objUnderTest) ;
    String expected =  "testNAME"  + ", testPASSWORD"  + ", testROLE"  + ", 2000-01-01 00:00:00.0" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUseraccountConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Useraccount(null ,  "testNAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Useraccount constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testUseraccountConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Useraccount whereObject = new Useraccount(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Useraccount(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  NAME.get(objUnderTest)  + ", " + PASSWORD.get(objUnderTest)  + ", " + ROLE.get(objUnderTest)  + ", " + LASTLOGIN.get(objUnderTest) ;
    String expected =  "testNAME"  + ", testPASSWORD"  + ", testROLE"  + ", 2000-01-01 00:00:00.0" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUseraccountConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Useraccount whereObject = new Useraccount(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Useraccount(null, whereObject);
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
    assertEquals("Useraccount", objUnderTest.getTableName());
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
    Useraccount whereObject = new Useraccount(rockFactory);

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
    Useraccount whereObject = new Useraccount(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Useraccount");
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
    NAME.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("NAME");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Useraccount");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the NAME column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT NAME FROM Useraccount");
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
    
    String expected = "<Useraccount NAME=\"'testNAME'\" PASSWORD=\"'testPASSWORD'\" ROLE=\"'testROLE'\" LASTLOGIN=\"'2000-01-01 00:00:00.0'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Useraccount NAME=\"'testNAME'\" PASSWORD=\"'testPASSWORD'\" ROLE=\"'testROLE'\" LASTLOGIN=\"'2000-01-01 00:00:00.0'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Useraccount>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Useraccount ( NAME, PASSWORD, ROLE, LASTLOGIN ) values "
      + "( 'testNAME', 'testPASSWORD', 'testROLE', '2000-01-01 00:00:00.0' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetName() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setName(UseraccountTest.testStringGenerator("anotherNAME", 255));
    assertEquals(UseraccountTest.testStringGenerator("anotherNAME", 255), NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPassword() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPassword(UseraccountTest.testStringGenerator("anotherPASSWORD", 16));
    assertEquals(UseraccountTest.testStringGenerator("anotherPASSWORD", 16), PASSWORD.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetRole() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setRole(UseraccountTest.testStringGenerator("anotherROLE", 16));
    assertEquals(UseraccountTest.testStringGenerator("anotherROLE", 16), ROLE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetLastlogin() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setLastlogin(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), LASTLOGIN.get(objUnderTest));
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
    String[] expectedKeys = { "NAME"};

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
    objUnderTest = new Useraccount(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  NAME.get(objUnderTest)  + ", " + PASSWORD.get(objUnderTest)  + ", " + ROLE.get(objUnderTest)  + ", " + LASTLOGIN.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", " + new Timestamp(0) ;
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
    Useraccount compareObj = new Useraccount(rockFactory ,  "testNAME");

    /* Testing first with null primary key value */
    NAME.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    NAME.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    NAME.set(objUnderTest,  "testNAME");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Useraccount with our current one. If the two
   * Useraccounts are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnUseraccount() throws Exception {

    /* Creating another Useraccount which will be compared to the tested one */
    Useraccount comparedObj = new Useraccount(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Useraccount with our current one. If the two
   * Useraccounts are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentUseraccount() throws Exception {

    /* Creating another Useraccount which will be compared to the tested one */
    Useraccount comparedObj = new Useraccount(rockFactory ,  "testNAME");
    comparedObj.setName( "DifferentNAME");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Useraccount with our current one. If the two
   * Useraccounts are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameUseraccount() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Useraccount comparedObj = new Useraccount(rockFactory ,  "testNAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Useraccount with our current one using null value.
   */
  @Test
  public void testEqualsWithNullUseraccount() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Useraccount comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Useraccount was null \n");
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
    assertEquals(Useraccount.class, actualObject.getClass());
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
    Useraccount testAgg = new Useraccount(rockFactory ,  "testNAME");
    NAME.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
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
   * Testing columnsize retrieving for Password.
   */
  @Test
  public void testGetPasswordColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getPasswordColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Password.
  */
  @Test
  public void testGetPasswordDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPasswordDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Password.
  */
  @Test
  public void testGetPasswordSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPasswordSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Role.
   */
  @Test
  public void testGetRoleColumnSize() throws Exception {
    
     assertEquals(16, objUnderTest.getRoleColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Role.
  */
  @Test
  public void testGetRoleDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getRoleDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Role.
  */
  @Test
  public void testGetRoleSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getRoleSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Lastlogin.
   */
  @Test
  public void testGetLastloginColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getLastloginColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Lastlogin.
  */
  @Test
  public void testGetLastloginDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getLastloginDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Lastlogin.
  */
  @Test
  public void testGetLastloginSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getLastloginSQLType());    
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
   * Testing original Useraccount object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Useraccount(rockFactory, false);
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
    Useraccount changedOriginal = new Useraccount(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Useraccount(rockFactory, false);
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
    Useraccount changedOriginal = new Useraccount(rockFactory, false);
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