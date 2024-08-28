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
 * Test class for Dwhtype. Changes to Dwhtype table are made via
 * this class.
 */
public class DwhtypeTest {

  private static Dwhtype objUnderTest;

  private static RockFactory rockFactory;

  private static Connection con = null;

  private static Statement stmt;
  
  private static Field newItem;
  
  private static Field TECHPACK_NAME;
  
  private static Field TYPENAME;
  
  private static Field TABLELEVEL;
  
  private static Field STORAGEID;
  
  private static Field PARTITIONSIZE;
  
  private static Field PARTITIONCOUNT;
  
  private static Field STATUS;
  
  private static Field TYPE;
  
  private static Field OWNER;
  
  private static Field VIEWTEMPLATE;
  
  private static Field CREATETEMPLATE;
  
  private static Field NEXTPARTITIONTIME;
  
  private static Field BASETABLENAME;
  
  private static Field DATADATECOLUMN;
  
  private static Field PUBLICVIEWTEMPLATE;
  
  private static Field PARTITIONPLAN;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  
    /* Reflecting the private fields */
    newItem = Dwhtype.class.getDeclaredField("newItem");
		TECHPACK_NAME = Dwhtype.class.getDeclaredField("TECHPACK_NAME");
		TYPENAME = Dwhtype.class.getDeclaredField("TYPENAME");
		TABLELEVEL = Dwhtype.class.getDeclaredField("TABLELEVEL");
		STORAGEID = Dwhtype.class.getDeclaredField("STORAGEID");
		PARTITIONSIZE = Dwhtype.class.getDeclaredField("PARTITIONSIZE");
		PARTITIONCOUNT = Dwhtype.class.getDeclaredField("PARTITIONCOUNT");
		STATUS = Dwhtype.class.getDeclaredField("STATUS");
		TYPE = Dwhtype.class.getDeclaredField("TYPE");
		OWNER = Dwhtype.class.getDeclaredField("OWNER");
		VIEWTEMPLATE = Dwhtype.class.getDeclaredField("VIEWTEMPLATE");
		CREATETEMPLATE = Dwhtype.class.getDeclaredField("CREATETEMPLATE");
		NEXTPARTITIONTIME = Dwhtype.class.getDeclaredField("NEXTPARTITIONTIME");
		BASETABLENAME = Dwhtype.class.getDeclaredField("BASETABLENAME");
		DATADATECOLUMN = Dwhtype.class.getDeclaredField("DATADATECOLUMN");
		PUBLICVIEWTEMPLATE = Dwhtype.class.getDeclaredField("PUBLICVIEWTEMPLATE");
		PARTITIONPLAN = Dwhtype.class.getDeclaredField("PARTITIONPLAN");
		newItem.setAccessible(true);
		TECHPACK_NAME.setAccessible(true);
		TYPENAME.setAccessible(true);
		TABLELEVEL.setAccessible(true);
		STORAGEID.setAccessible(true);
		PARTITIONSIZE.setAccessible(true);
		PARTITIONCOUNT.setAccessible(true);
		STATUS.setAccessible(true);
		TYPE.setAccessible(true);
		OWNER.setAccessible(true);
		VIEWTEMPLATE.setAccessible(true);
		CREATETEMPLATE.setAccessible(true);
		NEXTPARTITIONTIME.setAccessible(true);
		BASETABLENAME.setAccessible(true);
		DATADATECOLUMN.setAccessible(true);
		PUBLICVIEWTEMPLATE.setAccessible(true);
		PARTITIONPLAN.setAccessible(true);
	  
    /* Creating connection for rockfactory */
    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    stmt = con.createStatement();
    stmt.execute("CREATE TABLE Dwhtype ( TECHPACK_NAME VARCHAR(31)  ,TYPENAME VARCHAR(31) ,TABLELEVEL VARCHAR(31) ,STORAGEID VARCHAR(31) ,PARTITIONSIZE BIGINT  ,PARTITIONCOUNT BIGINT  ,STATUS VARCHAR(31) ,TYPE VARCHAR(31) ,OWNER VARCHAR(31) ,VIEWTEMPLATE VARCHAR(31) ,CREATETEMPLATE VARCHAR(31) ,NEXTPARTITIONTIME TIMESTAMP  ,BASETABLENAME VARCHAR(31) ,DATADATECOLUMN VARCHAR(31) ,PUBLICVIEWTEMPLATE VARCHAR(31) ,PARTITIONPLAN VARCHAR(31))");

    /* Initializing rockfactory */
    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    /* Cleaning up after test */
    stmt.execute("DROP TABLE Dwhtype");
    con = null;
    objUnderTest = null;
  }
  
  @Before
  public void setUp() throws Exception {

    /* Adding example data to table */
    stmt.executeUpdate("INSERT INTO Dwhtype VALUES( 'testTECHPACK_NAME'  ,'testTYPENAME'  ,'testTABLELEVEL'  ,'testSTORAGEID'  ,1  ,1  ,'testSTATUS'  ,'testTYPE'  ,'testOWNER'  ,'testVIEWTEMPLATE'  ,'testCREATETEMPLATE'  ,'2000-01-01 00:00:00.0'  ,'testBASETABLENAME'  ,'testDATADATECOLUMN'  ,'testPUBLICVIEWTEMPLATE'  ,'testPARTITIONPLAN' )");

    /* Initializing tested object where primary key is defined */
    objUnderTest = new Dwhtype(rockFactory ,  "testSTORAGEID");
  }
  
  @After
  public void tearDown() throws Exception {

    /* Cleaning up after each test */
    stmt.executeUpdate("DELETE FROM Dwhtype");
    objUnderTest = null;
  }
  
  /**
   * Testing Dwhtype constructor variable initialization with null values.
   */
  @Test
  public void testDwhtypeConstructorWithNullValues() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Dwhtype(rockFactory, true);

    /* Asserting that variables are null initialized */
    String actual =  TECHPACK_NAME.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + TABLELEVEL.get(objUnderTest)  + ", " + STORAGEID.get(objUnderTest)  + ", " + PARTITIONSIZE.get(objUnderTest)  + ", " + PARTITIONCOUNT.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + OWNER.get(objUnderTest)  + ", " + VIEWTEMPLATE.get(objUnderTest)  + ", " + CREATETEMPLATE.get(objUnderTest)  + ", " + NEXTPARTITIONTIME.get(objUnderTest)  + ", " + BASETABLENAME.get(objUnderTest)  + ", " + DATADATECOLUMN.get(objUnderTest)  + ", " + PUBLICVIEWTEMPLATE.get(objUnderTest)  + ", " + PARTITIONPLAN.get(objUnderTest) ;
    String expected =  null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null  + ", " + null ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing Dwhtype constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDwhtypeConstructorWithPrimaryKeyDefined() throws Exception {

    /* Calling the tested constructor */
    objUnderTest = new Dwhtype(rockFactory ,  "testSTORAGEID");

    /* Asserting that variables are initialized */
    String actual =  TECHPACK_NAME.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + TABLELEVEL.get(objUnderTest)  + ", " + STORAGEID.get(objUnderTest)  + ", " + PARTITIONSIZE.get(objUnderTest)  + ", " + PARTITIONCOUNT.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + OWNER.get(objUnderTest)  + ", " + VIEWTEMPLATE.get(objUnderTest)  + ", " + CREATETEMPLATE.get(objUnderTest)  + ", " + NEXTPARTITIONTIME.get(objUnderTest)  + ", " + BASETABLENAME.get(objUnderTest)  + ", " + DATADATECOLUMN.get(objUnderTest)  + ", " + PUBLICVIEWTEMPLATE.get(objUnderTest)  + ", " + PARTITIONPLAN.get(objUnderTest) ;
    String expected =  "testTECHPACK_NAME"  + ", testTYPENAME"  + ", testTABLELEVEL"  + ", testSTORAGEID"  + ", 1"  + ", 1"  + ", testSTATUS"  + ", testTYPE"  + ", testOWNER"  + ", testVIEWTEMPLATE"  + ", testCREATETEMPLATE"  + ", 2000-01-01 00:00:00.0"  + ", testBASETABLENAME"  + ", testDATADATECOLUMN"  + ", testPUBLICVIEWTEMPLATE"  + ", testPARTITIONPLAN" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDwhtypeConstructorWithPrimaryKeyDefinedNullRockfactory() throws Exception {

    /* Checking that null pointer exception is thrown */
    try {
      objUnderTest = new Dwhtype(null ,  "testSTORAGEID");
      fail("Test failed - NullPointerException was expected as rockfactory was initialized as null!");
    } catch (NullPointerException npe) {
      // test passed
    } catch (Exception e) {
      fail("Test failed - Unexpected exception occurred!\n" + e);
    }
  }
  
  /**
   * Testing Dwhtype constructor variable initialization with values taken
   * from database.
   */
  @Test
  public void testDwhtypeConstructorWithPrimaryKeyUndefined() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Dwhtype whereObject = new Dwhtype(rockFactory);

    /* Calling the tested constructor */
    objUnderTest = new Dwhtype(rockFactory, whereObject);

    /* Asserting that variables are initialized */
    String actual =  TECHPACK_NAME.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + TABLELEVEL.get(objUnderTest)  + ", " + STORAGEID.get(objUnderTest)  + ", " + PARTITIONSIZE.get(objUnderTest)  + ", " + PARTITIONCOUNT.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + OWNER.get(objUnderTest)  + ", " + VIEWTEMPLATE.get(objUnderTest)  + ", " + CREATETEMPLATE.get(objUnderTest)  + ", " + NEXTPARTITIONTIME.get(objUnderTest)  + ", " + BASETABLENAME.get(objUnderTest)  + ", " + DATADATECOLUMN.get(objUnderTest)  + ", " + PUBLICVIEWTEMPLATE.get(objUnderTest)  + ", " + PARTITIONPLAN.get(objUnderTest) ;
    String expected =  "testTECHPACK_NAME"  + ", testTYPENAME"  + ", testTABLELEVEL"  + ", testSTORAGEID"  + ", 1"  + ", 1"  + ", testSTATUS"  + ", testTYPE"  + ", testOWNER"  + ", testVIEWTEMPLATE"  + ", testCREATETEMPLATE"  + ", 2000-01-01 00:00:00.0"  + ", testBASETABLENAME"  + ", testDATADATECOLUMN"  + ", testPUBLICVIEWTEMPLATE"  + ", testPARTITIONPLAN" ;
    assertEquals(expected, actual);
  }
  
  /**
   * Testing constructor with negative case where rockfactory object is null.
   */
  @Test
  public void testDwhtypeConstructorWithPrimaryKeyUndefinedNullRockfactory() throws Exception {

    /* Creating where object which tells what sort of query is to be done */
    Dwhtype whereObject = new Dwhtype(rockFactory);

    /* Asserting that variables are initialized */
    try {
      objUnderTest = new Dwhtype(null, whereObject);
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
    assertEquals("Dwhtype", objUnderTest.getTableName());
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
    Dwhtype whereObject = new Dwhtype(rockFactory);

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
    Dwhtype whereObject = new Dwhtype(rockFactory);

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
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dwhtype");
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
    TECHPACK_NAME.set(objUnderTest, "changed");
    HashSet testSet = new HashSet();
    testSet.add("TECHPACK_NAME");
    objUnderTest.setModifiedColumns(testSet);
    objUnderTest.saveToDB();

    /* Getting row count */
    int rows = 0;
    ResultSet res = stmt.executeQuery("SELECT COUNT(*) FROM Dwhtype");
    while (res.next()) {
      rows = res.getInt(1);
    }

    /* Getting the TECHPACK_NAME column and see if it has changed */
    String queryResult = "";
    res = stmt.executeQuery("SELECT TECHPACK_NAME FROM Dwhtype");
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
    
    String expected = "<Dwhtype TECHPACK_NAME=\"'testTECHPACK_NAME'\" TYPENAME=\"'testTYPENAME'\" TABLELEVEL=\"'testTABLELEVEL'\" STORAGEID=\"'testSTORAGEID'\" PARTITIONSIZE=\"1\" PARTITIONCOUNT=\"1\" STATUS=\"'testSTATUS'\" TYPE=\"'testTYPE'\" OWNER=\"'testOWNER'\" VIEWTEMPLATE=\"'testVIEWTEMPLATE'\" CREATETEMPLATE=\"'testCREATETEMPLATE'\" NEXTPARTITIONTIME=\"'2000-01-01 00:00:00.0'\" BASETABLENAME=\"'testBASETABLENAME'\" DATADATECOLUMN=\"'testDATADATECOLUMN'\" PUBLICVIEWTEMPLATE=\"'testPUBLICVIEWTEMPLATE'\" PARTITIONPLAN=\"'testPARTITIONPLAN'\" DiffStatus=\"\" />\n";
    assertEquals(expected, objUnderTest.toXML_tag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_startTag() throws Exception {
    
    String expected = "<Dwhtype TECHPACK_NAME=\"'testTECHPACK_NAME'\" TYPENAME=\"'testTYPENAME'\" TABLELEVEL=\"'testTABLELEVEL'\" STORAGEID=\"'testSTORAGEID'\" PARTITIONSIZE=\"1\" PARTITIONCOUNT=\"1\" STATUS=\"'testSTATUS'\" TYPE=\"'testTYPE'\" OWNER=\"'testOWNER'\" VIEWTEMPLATE=\"'testVIEWTEMPLATE'\" CREATETEMPLATE=\"'testCREATETEMPLATE'\" NEXTPARTITIONTIME=\"'2000-01-01 00:00:00.0'\" BASETABLENAME=\"'testBASETABLENAME'\" DATADATECOLUMN=\"'testDATADATECOLUMN'\" PUBLICVIEWTEMPLATE=\"'testPUBLICVIEWTEMPLATE'\" PARTITIONPLAN=\"'testPARTITIONPLAN'\" DiffStatus=\"\" >\n";
    assertEquals(expected, objUnderTest.toXML_startTag());
  }
  
  /**
   * Testing column data formatting into xml.
   */
  @Test
  public void testToXML_endTag() throws Exception {
    
    assertEquals("</Dwhtype>\n", objUnderTest.toXML_endTag());
  }
  
  /**
   * Testing column data formatting into sql insert clause.
   */
  @Test
  public void testToSQLInsert() throws Exception {
    
    String expected = "insert into Dwhtype ( TECHPACK_NAME, TYPENAME, TABLELEVEL, STORAGEID, PARTITIONSIZE, PARTITIONCOUNT, STATUS, TYPE, OWNER, VIEWTEMPLATE, CREATETEMPLATE, NEXTPARTITIONTIME, BASETABLENAME, DATADATECOLUMN, PUBLICVIEWTEMPLATE, PARTITIONPLAN ) values "
      + "( 'testTECHPACK_NAME', 'testTYPENAME', 'testTABLELEVEL', 'testSTORAGEID', 1, 1, 'testSTATUS', 'testTYPE', 'testOWNER', 'testVIEWTEMPLATE', 'testCREATETEMPLATE', '2000-01-01 00:00:00.0', 'testBASETABLENAME', 'testDATADATECOLUMN', 'testPUBLICVIEWTEMPLATE', 'testPARTITIONPLAN' );\n";
    assertEquals(expected, objUnderTest.toSQLInsert());
  }
  
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTechpack_name() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTechpack_name(DwhtypeTest.testStringGenerator("anotherTECHPACK_NAME", 30));
    assertEquals(DwhtypeTest.testStringGenerator("anotherTECHPACK_NAME", 30), TECHPACK_NAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTypename() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTypename(DwhtypeTest.testStringGenerator("anotherTYPENAME", 255));
    assertEquals(DwhtypeTest.testStringGenerator("anotherTYPENAME", 255), TYPENAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetTablelevel() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setTablelevel(DwhtypeTest.testStringGenerator("anotherTABLELEVEL", 50));
    assertEquals(DwhtypeTest.testStringGenerator("anotherTABLELEVEL", 50), TABLELEVEL.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStorageid() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStorageid(DwhtypeTest.testStringGenerator("anotherSTORAGEID", 255));
    assertEquals(DwhtypeTest.testStringGenerator("anotherSTORAGEID", 255), STORAGEID.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPartitionsize() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPartitionsize(555L);
    assertEquals(555L, PARTITIONSIZE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPartitioncount() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPartitioncount(555L);
    assertEquals(555L, PARTITIONCOUNT.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetStatus() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setStatus(DwhtypeTest.testStringGenerator("anotherSTATUS", 50));
    assertEquals(DwhtypeTest.testStringGenerator("anotherSTATUS", 50), STATUS.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetType() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setType(DwhtypeTest.testStringGenerator("anotherTYPE", 50));
    assertEquals(DwhtypeTest.testStringGenerator("anotherTYPE", 50), TYPE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetOwner() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setOwner(DwhtypeTest.testStringGenerator("anotherOWNER", 50));
    assertEquals(DwhtypeTest.testStringGenerator("anotherOWNER", 50), OWNER.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetViewtemplate() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setViewtemplate(DwhtypeTest.testStringGenerator("anotherVIEWTEMPLATE", 255));
    assertEquals(DwhtypeTest.testStringGenerator("anotherVIEWTEMPLATE", 255), VIEWTEMPLATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetCreatetemplate() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setCreatetemplate(DwhtypeTest.testStringGenerator("anotherCREATETEMPLATE", 255));
    assertEquals(DwhtypeTest.testStringGenerator("anotherCREATETEMPLATE", 255), CREATETEMPLATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetNextpartitiontime() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setNextpartitiontime(new Timestamp(946677600000L));
    assertEquals(new Timestamp(946677600000L), NEXTPARTITIONTIME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetBasetablename() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setBasetablename(DwhtypeTest.testStringGenerator("anotherBASETABLENAME", 125));
    assertEquals(DwhtypeTest.testStringGenerator("anotherBASETABLENAME", 125), BASETABLENAME.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetDatadatecolumn() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setDatadatecolumn(DwhtypeTest.testStringGenerator("anotherDATADATECOLUMN", 128));
    assertEquals(DwhtypeTest.testStringGenerator("anotherDATADATECOLUMN", 128), DATADATECOLUMN.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPublicviewtemplate() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPublicviewtemplate(DwhtypeTest.testStringGenerator("anotherPUBLICVIEWTEMPLATE", 255));
    assertEquals(DwhtypeTest.testStringGenerator("anotherPUBLICVIEWTEMPLATE", 255), PUBLICVIEWTEMPLATE.get(objUnderTest));
  }
    /**
   * Testing column data setting and retrieving via get and set methods.
   */
  @Test
  public void testSetAndGetPartitionplan() throws Exception {

    /* Data validating on */
    objUnderTest.setValidateData(true);

    /* Setting column values and asserting correct value is returned */
    objUnderTest.setPartitionplan(DwhtypeTest.testStringGenerator("anotherPARTITIONPLAN", 128));
    assertEquals(DwhtypeTest.testStringGenerator("anotherPARTITIONPLAN", 128), PARTITIONPLAN.get(objUnderTest));
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
    String[] expectedKeys = { "STORAGEID"};

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
    objUnderTest = new Dwhtype(rockFactory, true);

    /* Calling the tested method and asserting nulls are removed */
    objUnderTest.removeNulls();
    String actual =  TECHPACK_NAME.get(objUnderTest)  + ", " + TYPENAME.get(objUnderTest)  + ", " + TABLELEVEL.get(objUnderTest)  + ", " + STORAGEID.get(objUnderTest)  + ", " + PARTITIONSIZE.get(objUnderTest)  + ", " + PARTITIONCOUNT.get(objUnderTest)  + ", " + STATUS.get(objUnderTest)  + ", " + TYPE.get(objUnderTest)  + ", " + OWNER.get(objUnderTest)  + ", " + VIEWTEMPLATE.get(objUnderTest)  + ", " + CREATETEMPLATE.get(objUnderTest)  + ", " + NEXTPARTITIONTIME.get(objUnderTest)  + ", " + BASETABLENAME.get(objUnderTest)  + ", " + DATADATECOLUMN.get(objUnderTest)  + ", " + PUBLICVIEWTEMPLATE.get(objUnderTest)  + ", " + PARTITIONPLAN.get(objUnderTest) ;
    String expected =  ""  + ", "  + ", "  + ", "  + ", 0"  + ", 0"  + ", "  + ", "  + ", "  + ", "  + ", "  + ", " + new Timestamp(0)  + ", "  + ", "  + ", "  + ", " ;
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
    Dwhtype compareObj = new Dwhtype(rockFactory ,  "testSTORAGEID");

    /* Testing first with null primary key value */
    STORAGEID.set(objUnderTest, null);
  	String actual = objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Testing with different key value */
    STORAGEID.set(objUnderTest,  "different");
  	actual += objUnderTest.dbEquals(compareObj) + ", ";
    
    /* Finally test with same value and test assertion */
    STORAGEID.set(objUnderTest,  "testSTORAGEID");
  	actual += objUnderTest.dbEquals(compareObj);
    assertEquals(false + ", " + false + ", " + true, actual);
  }
  
  /**
   * Testing comparing another Dwhtype with our current one. If the two
   * Dwhtypes are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithNullColumnDwhtype() throws Exception {

    /* Creating another Dwhtype which will be compared to the tested one */
    Dwhtype comparedObj = new Dwhtype(rockFactory, true);

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dwhtype with our current one. If the two
   * Dwhtypes are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithDifferentDwhtype() throws Exception {

    /* Creating another Dwhtype which will be compared to the tested one */
    Dwhtype comparedObj = new Dwhtype(rockFactory ,  "testSTORAGEID");
    comparedObj.setTechpack_name( "DifferentTECHPACK_NAME");

    /* Asserting that false is returned */
    assertEquals(false, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dwhtype with our current one. If the two
   * Dwhtypes are the same true is returned, otherwise false.
   */
  @Test
  public void testEqualsWithSameDwhtype() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Dwhtype comparedObj = new Dwhtype(rockFactory ,  "testSTORAGEID");

    /* Asserting that true is returned */
    assertEquals(true, objUnderTest.equals(comparedObj));
  }
  
  /**
   * Testing comparing another Dwhtype with our current one using null value.
   */
  @Test
  public void testEqualsWithNullDwhtype() throws Exception {

    /* Creating another aggregation which will be compared to the tested one */
    Dwhtype comparedObj = null;

    /* Asserting that exception is thrown */
    try {
      objUnderTest.equals(comparedObj);
      fail("Test Failed - Unexpected NullPointerException expected as compared Dwhtype was null \n");
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
    assertEquals(Dwhtype.class, actualObject.getClass());
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
    Dwhtype testAgg = new Dwhtype(rockFactory ,  "testSTORAGEID");
    TECHPACK_NAME.set(objUnderTest, "changed");

    actual += objUnderTest.existsDB();
    assertEquals(true + ", " + false, actual);
  }
  
    /**
   * Testing columnsize retrieving for Techpack_name.
   */
  @Test
  public void testGetTechpack_nameColumnSize() throws Exception {
    
     assertEquals(30, objUnderTest.getTechpack_nameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Techpack_name.
  */
  @Test
  public void testGetTechpack_nameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTechpack_nameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Techpack_name.
  */
  @Test
  public void testGetTechpack_nameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTechpack_nameSQLType());    
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
   * Testing columnsize retrieving for Tablelevel.
   */
  @Test
  public void testGetTablelevelColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getTablelevelColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Tablelevel.
  */
  @Test
  public void testGetTablelevelDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getTablelevelDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Tablelevel.
  */
  @Test
  public void testGetTablelevelSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getTablelevelSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Storageid.
   */
  @Test
  public void testGetStorageidColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getStorageidColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Storageid.
  */
  @Test
  public void testGetStorageidDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getStorageidDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Storageid.
  */
  @Test
  public void testGetStorageidSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getStorageidSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Partitionsize.
   */
  @Test
  public void testGetPartitionsizeColumnSize() throws Exception {
    
     assertEquals(9, objUnderTest.getPartitionsizeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Partitionsize.
  */
  @Test
  public void testGetPartitionsizeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPartitionsizeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Partitionsize.
  */
  @Test
  public void testGetPartitionsizeSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getPartitionsizeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Partitioncount.
   */
  @Test
  public void testGetPartitioncountColumnSize() throws Exception {
    
     assertEquals(9, objUnderTest.getPartitioncountColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Partitioncount.
  */
  @Test
  public void testGetPartitioncountDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPartitioncountDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Partitioncount.
  */
  @Test
  public void testGetPartitioncountSQLType() throws Exception {
    
    assertEquals(2, objUnderTest.getPartitioncountSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Status.
   */
  @Test
  public void testGetStatusColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getStatusColumnSize());   
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
    
    assertEquals(12, objUnderTest.getStatusSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Type.
   */
  @Test
  public void testGetTypeColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getTypeColumnSize());   
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
   * Testing columnsize retrieving for Owner.
   */
  @Test
  public void testGetOwnerColumnSize() throws Exception {
    
     assertEquals(50, objUnderTest.getOwnerColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Owner.
  */
  @Test
  public void testGetOwnerDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getOwnerDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Owner.
  */
  @Test
  public void testGetOwnerSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getOwnerSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Viewtemplate.
   */
  @Test
  public void testGetViewtemplateColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getViewtemplateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Viewtemplate.
  */
  @Test
  public void testGetViewtemplateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getViewtemplateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Viewtemplate.
  */
  @Test
  public void testGetViewtemplateSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getViewtemplateSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Createtemplate.
   */
  @Test
  public void testGetCreatetemplateColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getCreatetemplateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Createtemplate.
  */
  @Test
  public void testGetCreatetemplateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getCreatetemplateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Createtemplate.
  */
  @Test
  public void testGetCreatetemplateSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getCreatetemplateSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Nextpartitiontime.
   */
  @Test
  public void testGetNextpartitiontimeColumnSize() throws Exception {
    
     assertEquals(23, objUnderTest.getNextpartitiontimeColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Nextpartitiontime.
  */
  @Test
  public void testGetNextpartitiontimeDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getNextpartitiontimeDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Nextpartitiontime.
  */
  @Test
  public void testGetNextpartitiontimeSQLType() throws Exception {
    
    assertEquals(93, objUnderTest.getNextpartitiontimeSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Basetablename.
   */
  @Test
  public void testGetBasetablenameColumnSize() throws Exception {
    
     assertEquals(125, objUnderTest.getBasetablenameColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Basetablename.
  */
  @Test
  public void testGetBasetablenameDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getBasetablenameDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Basetablename.
  */
  @Test
  public void testGetBasetablenameSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getBasetablenameSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Datadatecolumn.
   */
  @Test
  public void testGetDatadatecolumnColumnSize() throws Exception {
    
     assertEquals(128, objUnderTest.getDatadatecolumnColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Datadatecolumn.
  */
  @Test
  public void testGetDatadatecolumnDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getDatadatecolumnDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Datadatecolumn.
  */
  @Test
  public void testGetDatadatecolumnSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getDatadatecolumnSQLType());    
  }
  
    /**
   * Testing columnsize retrieving for Publicviewtemplate.
   */
  @Test
  public void testGetPublicviewtemplateColumnSize() throws Exception {
    
     assertEquals(255, objUnderTest.getPublicviewtemplateColumnSize());   
  }

 /**
  * Testing decimal digits retrieving for Publicviewtemplate.
  */
  @Test
  public void testGetPublicviewtemplateDecimalDigits() throws Exception {
    
     assertEquals(0, objUnderTest.getPublicviewtemplateDecimalDigits());     
  }
  
 /**
  * Testing columnsize retrieving for Publicviewtemplate.
  */
  @Test
  public void testGetPublicviewtemplateSQLType() throws Exception {
    
    assertEquals(12, objUnderTest.getPublicviewtemplateSQLType());    
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
   * Testing flag variable setting and retrieving.
   */
  @Test
  public void testSetAndGetflagVariables() throws Exception {

    objUnderTest.setNewItem(true);
    objUnderTest.setValidateData(true);
    assertEquals(true + ", " + true, objUnderTest.isNewItem() + ", " + objUnderTest.isValidateData());
  }

  /**
   * Testing original Dwhtype object setting and retrieving.
   */
  @Test
  public void testSetAndGetOriginal() throws Exception {

    objUnderTest = new Dwhtype(rockFactory, false);
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
    Dwhtype changedOriginal = new Dwhtype(rockFactory, false);
    objUnderTest.setOriginal(changedOriginal);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object has been updated */
    modifiedColumns.add("updatedValue");
    objUnderTest.setModifiedColumns(modifiedColumns);
    actual += objUnderTest.isUpdated() + ", ";

    /* Tested object is null initialized */
    objUnderTest = new Dwhtype(rockFactory, false);
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
    Dwhtype changedOriginal = new Dwhtype(rockFactory, false);
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