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
 * Test class for Universecomputedobject. Changes to Universecomputedobject table are made via
 * this class.
 */
public class UniversecomputedobjectTest {

  private static Universecomputedobject objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field VERSIONID;
  
  private static Field CLASSNAME;
  
  private static Field UNIVERSEEXTENSION;
  
  private static Field OBJECTNAME;
  
  private static Field DESCRIPTION;
  
  private static Field OBJECTTYPE;
  
  private static Field QUALIFICATION;
  
  private static Field AGGREGATION;
  
  private static Field OBJSELECT;
  
  private static Field OBJWHERE;
  
  private static Field PROMPTHIERARCHY;
  
  private static Field OBJ_BH_REL;
  
  private static Field ELEM_BH_REL;
  
  private static Field INHERITANCE;
  
  private static Field ORDERNRO;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Universecomputedobject.class.getDeclaredField("newItem");
		VERSIONID = Universecomputedobject.class.getDeclaredField("VERSIONID");
		CLASSNAME = Universecomputedobject.class.getDeclaredField("CLASSNAME");
		UNIVERSEEXTENSION = Universecomputedobject.class.getDeclaredField("UNIVERSEEXTENSION");
		OBJECTNAME = Universecomputedobject.class.getDeclaredField("OBJECTNAME");
		DESCRIPTION = Universecomputedobject.class.getDeclaredField("DESCRIPTION");
		OBJECTTYPE = Universecomputedobject.class.getDeclaredField("OBJECTTYPE");
		QUALIFICATION = Universecomputedobject.class.getDeclaredField("QUALIFICATION");
		AGGREGATION = Universecomputedobject.class.getDeclaredField("AGGREGATION");
		OBJSELECT = Universecomputedobject.class.getDeclaredField("OBJSELECT");
		OBJWHERE = Universecomputedobject.class.getDeclaredField("OBJWHERE");
		PROMPTHIERARCHY = Universecomputedobject.class.getDeclaredField("PROMPTHIERARCHY");
		OBJ_BH_REL = Universecomputedobject.class.getDeclaredField("OBJ_BH_REL");
		ELEM_BH_REL = Universecomputedobject.class.getDeclaredField("ELEM_BH_REL");
		INHERITANCE = Universecomputedobject.class.getDeclaredField("INHERITANCE");
		ORDERNRO = Universecomputedobject.class.getDeclaredField("ORDERNRO");
		newItem.setAccessible(true);
		VERSIONID.setAccessible(true);
		CLASSNAME.setAccessible(true);
		UNIVERSEEXTENSION.setAccessible(true);
		OBJECTNAME.setAccessible(true);
		DESCRIPTION.setAccessible(true);
		OBJECTTYPE.setAccessible(true);
		QUALIFICATION.setAccessible(true);
		AGGREGATION.setAccessible(true);
		OBJSELECT.setAccessible(true);
		OBJWHERE.setAccessible(true);
		PROMPTHIERARCHY.setAccessible(true);
		OBJ_BH_REL.setAccessible(true);
		ELEM_BH_REL.setAccessible(true);
		INHERITANCE.setAccessible(true);
		ORDERNRO.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Universecomputedobject ( VERSIONID VARCHAR(31)  ,CLASSNAME VARCHAR(31) ,UNIVERSEEXTENSION VARCHAR(31) ,OBJECTNAME VARCHAR(31) ,DESCRIPTION VARCHAR(31) ,OBJECTTYPE VARCHAR(31) ,QUALIFICATION VARCHAR(31) ,AGGREGATION VARCHAR(31) ,OBJSELECT VARCHAR(31) ,OBJWHERE VARCHAR(31) ,PROMPTHIERARCHY VARCHAR(31) ,OBJ_BH_REL INTEGER  ,ELEM_BH_REL INTEGER  ,INHERITANCE INTEGER  ,ORDERNRO BIGINT )");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Universecomputedobject");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Universecomputedobject VALUES( 'testVERSIONID'  ,'testCLASSNAME'  ,'testUNIVERSEEXTENSION'  ,'testOBJECTNAME'  ,'testDESCRIPTION'  ,'testOBJECTTYPE'  ,'testQUALIFICATION'  ,'testAGGREGATION'  ,'testOBJSELECT'  ,'testOBJWHERE'  ,'testPROMPTHIERARCHY'  ,1  ,1  ,1  ,1 )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Universecomputedobject(rockFactory ,  "testVERSIONID",  "testCLASSNAME",  "testUNIVERSEEXTENSION",  "testOBJECTNAME");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Universecomputedobject");
    objUnderTest = null;
  }
  
  /**
   * Testing Universecomputedobject constructor variable initialization with null values.
   */
  @Test
  public void testUniversecomputedobjectConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Universecomputedobject(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + CLASSNAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + QUALIFICATION.get(objUnderTest)  + ", " + AGGREGATION.get(objUnderTest)  + ", " + OBJSELECT.get(objUnderTest)  + ", " + OBJWHERE.get(objUnderTest)  + ", " + PROMPTHIERARCHY.get(objUnderTest)  + ", " + OBJ_BH_REL.get(objUnderTest)  + ", " + ELEM_BH_REL.get(objUnderTest)  + ", " + INHERITANCE.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Universecomputedobject constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testUniversecomputedobjectConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Universecomputedobject(rockFactory ,  "testVERSIONID",  "testCLASSNAME",  "testUNIVERSEEXTENSION",  "testOBJECTNAME");

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + CLASSNAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + QUALIFICATION.get(objUnderTest)  + ", " + AGGREGATION.get(objUnderTest)  + ", " + OBJSELECT.get(objUnderTest)  + ", " + OBJWHERE.get(objUnderTest)  + ", " + PROMPTHIERARCHY.get(objUnderTest)  + ", " + OBJ_BH_REL.get(objUnderTest)  + ", " + ELEM_BH_REL.get(objUnderTest)  + ", " + INHERITANCE.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testCLASSNAME"  + ", testUNIVERSEEXTENSION"  + ", testOBJECTNAME"  + ", testDESCRIPTION"  + ", testOBJECTTYPE"  + ", testQUALIFICATION"  + ", testAGGREGATION"  + ", testOBJSELECT"  + ", testOBJWHERE"  + ", testPROMPTHIERARCHY"  + ", 1"  + ", 1"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniversecomputedobjectConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Universecomputedobject(null ,  "testVERSIONID",  "testCLASSNAME",  "testUNIVERSEEXTENSION",  "testOBJECTNAME");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Universecomputedobject constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testUniversecomputedobjectConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Universecomputedobject whereObject = new Universecomputedobject(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Universecomputedobject(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  VERSIONID.get(objUnderTest)  + ", " + CLASSNAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + QUALIFICATION.get(objUnderTest)  + ", " + AGGREGATION.get(objUnderTest)  + ", " + OBJSELECT.get(objUnderTest)  + ", " + OBJWHERE.get(objUnderTest)  + ", " + PROMPTHIERARCHY.get(objUnderTest)  + ", " + OBJ_BH_REL.get(objUnderTest)  + ", " + ELEM_BH_REL.get(objUnderTest)  + ", " + INHERITANCE.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  "testVERSIONID"  + ", testCLASSNAME"  + ", testUNIVERSEEXTENSION"  + ", testOBJECTNAME"  + ", testDESCRIPTION"  + ", testOBJECTTYPE"  + ", testQUALIFICATION"  + ", testAGGREGATION"  + ", testOBJSELECT"  + ", testOBJWHERE"  + ", testPROMPTHIERARCHY"  + ", 1"  + ", 1"  + ", 1"  + ", 1" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testUniversecomputedobjectConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Universecomputedobject whereObject = new Universecomputedobject(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Universecomputedobject(null, whereObject);
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
    assertEquals("Universecomputedobject", objUnderTest.getTableName());
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
    Universecomputedobject whereObject = new Universecomputedobject(rockFactory);

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
    Universecomputedobject whereObject = new Universecomputedobject(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universecomputedobject");
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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Universecomputedobject");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the VERSIONID column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT VERSIONID FROM Universecomputedobject");
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
    
    String expected = "<Universecomputedobject VERSIONID=\"'testVERSIONID'\" CLASSNAME=\"'testCLASSNAME'\" UNIVERSEEXTENSION=\"'testUNIVERSEEXTENSION'\" OBJECTNAME=\"'testOBJECTNAME'\" DESCRIPTION=\"'testDESCRIPTION'\" OBJECTTYPE=\"'testOBJECTTYPE'\" QUALIFICATION=\"'testQUALIFICATION'\" AGGREGATION=\"'testAGGREGATION'\" OBJSELECT=\"'testOBJSELECT'\" OBJWHERE=\"'testOBJWHERE'\" PROMPTHIERARCHY=\"'testPROMPTHIERARCHY'\" OBJ_BH_REL=\"1\" ELEM_BH_REL=\"1\" INHERITANCE=\"1\" ORDERNRO=\"1\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Universecomputedobject VERSIONID=\"'testVERSIONID'\" CLASSNAME=\"'testCLASSNAME'\" UNIVERSEEXTENSION=\"'testUNIVERSEEXTENSION'\" OBJECTNAME=\"'testOBJECTNAME'\" DESCRIPTION=\"'testDESCRIPTION'\" OBJECTTYPE=\"'testOBJECTTYPE'\" QUALIFICATION=\"'testQUALIFICATION'\" AGGREGATION=\"'testAGGREGATION'\" OBJSELECT=\"'testOBJSELECT'\" OBJWHERE=\"'testOBJWHERE'\" PROMPTHIERARCHY=\"'testPROMPTHIERARCHY'\" OBJ_BH_REL=\"1\" ELEM_BH_REL=\"1\" INHERITANCE=\"1\" ORDERNRO=\"1\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Universecomputedobject>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Universecomputedobject ( VERSIONID, CLASSNAME, UNIVERSEEXTENSION, OBJECTNAME, DESCRIPTION, OBJECTTYPE, QUALIFICATION, AGGREGATION, OBJSELECT, OBJWHERE, PROMPTHIERARCHY, OBJ_BH_REL, ELEM_BH_REL, INHERITANCE, ORDERNRO ) values "
      + "( 'testVERSIONID', 'testCLASSNAME', 'testUNIVERSEEXTENSION', 'testOBJECTNAME', 'testDESCRIPTION', 'testOBJECTTYPE', 'testQUALIFICATION', 'testAGGREGATION', 'testOBJSELECT', 'testOBJWHERE', 'testPROMPTHIERARCHY', 1, 1, 1, 1 );\n";
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
    objUnderTest.setVersionid(UniversecomputedobjectTest.testStringGenerator("anotherVERSIONID", 128));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherVERSIONID", 128), VERSIONID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetClassname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setClassname(UniversecomputedobjectTest.testStringGenerator("anotherCLASSNAME", 128));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherCLASSNAME", 128), CLASSNAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetUniverseextension() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setUniverseextension(UniversecomputedobjectTest.testStringGenerator("anotherUNIVERSEEXTENSION", 12));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherUNIVERSEEXTENSION", 12), UNIVERSEEXTENSION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjectname() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjectname(UniversecomputedobjectTest.testStringGenerator("anotherOBJECTNAME", 128));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherOBJECTNAME", 128), OBJECTNAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDescription() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDescription(UniversecomputedobjectTest.testStringGenerator("anotherDESCRIPTION", 32000));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherDESCRIPTION", 32000), DESCRIPTION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjecttype() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjecttype(UniversecomputedobjectTest.testStringGenerator("anotherOBJECTTYPE", 16));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherOBJECTTYPE", 16), OBJECTTYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetQualification() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setQualification(UniversecomputedobjectTest.testStringGenerator("anotherQUALIFICATION", 16));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherQUALIFICATION", 16), QUALIFICATION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetAggregation() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setAggregation(UniversecomputedobjectTest.testStringGenerator("anotherAGGREGATION", 16));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherAGGREGATION", 16), AGGREGATION.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjselect() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjselect(UniversecomputedobjectTest.testStringGenerator("anotherOBJSELECT", 32000));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherOBJSELECT", 32000), OBJSELECT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObjwhere() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObjwhere(UniversecomputedobjectTest.testStringGenerator("anotherOBJWHERE", 32000));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherOBJWHERE", 32000), OBJWHERE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPrompthierarchy() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPrompthierarchy(UniversecomputedobjectTest.testStringGenerator("anotherPROMPTHIERARCHY", 255));
    assertEquals(UniversecomputedobjectTest.testStringGenerator("anotherPROMPTHIERARCHY", 255), PROMPTHIERARCHY.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetObj_bh_rel() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setObj_bh_rel(555);
    assertEquals(555, OBJ_BH_REL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetElem_bh_rel() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setElem_bh_rel(555);
    assertEquals(555, ELEM_BH_REL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetInheritance() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setInheritance(555);
    assertEquals(555, INHERITANCE.get(objUnderTest));
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
    String[] expectedKeys = { "VERSIONID","CLASSNAME","UNIVERSEEXTENSION","OBJECTNAME"};

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
    objUnderTest = new Universecomputedobject(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  VERSIONID.get(objUnderTest)  + ", " + CLASSNAME.get(objUnderTest)  + ", " + UNIVERSEEXTENSION.get(objUnderTest)  + ", " + OBJECTNAME.get(objUnderTest)  + ", " + DESCRIPTION.get(objUnderTest)  + ", " + OBJECTTYPE.get(objUnderTest)  + ", " + QUALIFICATION.get(objUnderTest)  + ", " + AGGREGATION.get(objUnderTest)  + ", " + OBJSELECT.get(objUnderTest)  + ", " + OBJWHERE.get(objUnderTest)  + ", " + PROMPTHIERARCHY.get(objUnderTest)  + ", " + OBJ_BH_REL.get(objUnderTest)  + ", " + ELEM_BH_REL.get(objUnderTest)  + ", " + INHERITANCE.get(objUnderTest)  + ", " + ORDERNRO.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", 0"  + ", 0" ;
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
    Universecomputedobject compareObj = new Universecomputedobject(rockFactory ,  "testVERSIONID",  "testCLASSNAME",  "testUNIVERSEEXTENSION",  "testOBJECTNAME");

    /* Testing first with null primary key value */
    VERSIONID.set(objUnderTest, null);
  	CLASSNAME.set(objUnderTest, null);
  	UNIVERSEEXTENSION.set(objUnderTest, null);
  	OBJECTNAME.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    VERSIONID.set(objUnderTest,  "different");
  	CLASSNAME.set(objUnderTest,  "different");
  	UNIVERSEEXTENSION.set(objUnderTest,  "different");
  	OBJECTNAME.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    VERSIONID.set(objUnderTest,  "testVERSIONID");
  	CLASSNAME.set(objUnderTest,  "testCLASSNAME");
  	UNIVERSEEXTENSION.set(objUnderTest,  "testUNIVERSEEXTENSION");
  	OBJECTNAME.set(objUnderTest,  "testOBJECTNAME");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Universecomputedobject with our current one. If the two
   * Universecomputedobjects are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnUniversecomputedobject() throws Exception {

    /* Creating another Universecomputedobject which will be compared to the tested one */
    Universecomputedobject comparedObj = new Universecomputedobject(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universecomputedobject with our current one. If the two
   * Universecomputedobjects are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentUniversecomputedobject() throws Exception {

    /* Creating another Universecomputedobject which will be compared to the tested one */
    Universecomputedobject comparedObj = new Universecomputedobject(rockFactory ,  "testVERSIONID",  "testCLASSNAME",  "testUNIVERSEEXTENSION",  "testOBJECTNAME");
    comparedObj.setVersionid( "DifferentVERSIONID");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universecomputedobject with our current one. If the two
   * Universecomputedobjects are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameUniversecomputedobject() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Universecomputedobject comparedObj = new Universecomputedobject(rockFactory ,  "testVERSIONID",  "testCLASSNAME",  "testUNIVERSEEXTENSION",  "testOBJECTNAME");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Universecomputedobject with our current one using null value.
   */
  @Test
  public void testEqualsWithNullUniversecomputedobject() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Universecomputedobject comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Universecomputedobject was null \n");
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
    assertEquals(Universecomputedobject.class, actualObject.getClass());
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
    Universecomputedobject testAgg = new Universecomputedobject(rockFactory ,  "testVERSIONID",  "testCLASSNAME",  "testUNIVERSEEXTENSION",  "testOBJECTNAME");
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
   * Testing columnsize retrieving for Classname.
   */
  @Test
  public void testGetClassnameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getClassnameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Classname.
  */
  @Test
  public void testGetClassnameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getClassnameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Classname.
  */
  @Test
  public void testGetClassnameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getClassnameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Universeextension.
   */
  @Test
  public void testGetUniverseextensionColumnSize() throws Exception {
    
     assertEquals(12, objUnderTest.getUniverseextensionColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Universeextension.
  */
  @Test
  public void testGetUniverseextensionDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getUniverseextensionDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Universeextension.
  */
  @Test
  public void testGetUniverseextensionSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getUniverseextensionSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objectname.
   */
  @Test
  public void testGetObjectnameColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getObjectnameColumnSize());   
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
   * Testing columnsize retrieving for Objselect.
   */
  @Test
  public void testGetObjselectColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getObjselectColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objselect.
  */
  @Test
  public void testGetObjselectDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjselectDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objselect.
  */
  @Test
  public void testGetObjselectSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjselectSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Objwhere.
   */
  @Test
  public void testGetObjwhereColumnSize() throws Exception {
    
     assertEquals(32000, objUnderTest.getObjwhereColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Objwhere.
  */
  @Test
  public void testGetObjwhereDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObjwhereDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Objwhere.
  */
  @Test
  public void testGetObjwhereSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getObjwhereSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Prompthierarchy.
   */
  @Test
  public void testGetPrompthierarchyColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getPrompthierarchyColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Prompthierarchy.
  */
  @Test
  public void testGetPrompthierarchyDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPrompthierarchyDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Prompthierarchy.
  */
  @Test
  public void testGetPrompthierarchySQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPrompthierarchySQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Obj_bh_rel.
   */
  @Test
  public void testGetObj_bh_relColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getObj_bh_relColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Obj_bh_rel.
  */
  @Test
  public void testGetObj_bh_relDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getObj_bh_relDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Obj_bh_rel.
  */
  @Test
  public void testGetObj_bh_relSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getObj_bh_relSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Elem_bh_rel.
   */
  @Test
  public void testGetElem_bh_relColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getElem_bh_relColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Elem_bh_rel.
  */
  @Test
  public void testGetElem_bh_relDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getElem_bh_relDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Elem_bh_rel.
  */
  @Test
  public void testGetElem_bh_relSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getElem_bh_relSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Inheritance.
   */
  @Test
  public void testGetInheritanceColumnSize() throws Exception {
    
     assertEquals(10, objUnderTest.getInheritanceColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Inheritance.
  */
  @Test
  public void testGetInheritanceDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getInheritanceDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Inheritance.
  */
  @Test
  public void testGetInheritanceSQLType() throws Exception {
    
    assertEquals(4, objUnderTest.getInheritanceSQLType());    
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
   * Testing original Universecomputedobject object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Universecomputedobject(rockFactory, false);
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
    Universecomputedobject changedOriginal = new Universecomputedobject(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Universecomputedobject(rockFactory, false);
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
    Universecomputedobject changedOriginal = new Universecomputedobject(rockFactory, false);
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