package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_jointsFactory;
import com.distocraft.dc5000.etl.rock.Meta_source_tables;
import com.distocraft.dc5000.etl.rock.Meta_tables;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
import java.lang.reflect.Method;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.sql.Statement;
import java.util.Vector;

import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

public class SQLSourceTest extends UnitDatabaseTestCase {

	static Meta_transfer_actions metaTransferActions;
	static Meta_versions metaVersions;
	static Meta_collections collection;
	static Meta_columns objMetaCol;
	static Meta_tables metaTables;
	static ConnectionPool  connectionPool;
	
	static TransferActionBase transferActionBase;
	
    Long collectionSetId = 1L;
    Long transferActionId = 1L;
    Long transferBatchId = 1L;
    Long connectId = 1L;
    
    static RockFactory etlrep ;
    static RockFactory dwhrep ;
    
    static String batchColumnName="batchColumnName";
	
  @BeforeClass
  public static void setUp() throws Exception{
	  
	setup(TestType.unit);
	
    loadDefaultTechpack(TechPack.stats, "v1");
    loadDefaultTechpack(TechPack.stats, "v2");
    
    etlrep = getRockFactory(Schema.etlrep);
    dwhrep = getRockFactory(Schema.dwhrep);
    Statement stmt = etlrep.getConnection().createStatement();
    
    try {
            stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'set_name', 'description', '((1))', 'Y', 'type')");
            
            stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'COLLECTION_SET_ID'  ,'COLLECTION_SET_ID'  ,'COL_TYPE'  ,1  ,'Y'  ,'((1))'  ,1  ,1 )");
            
            stmt.executeUpdate("INSERT INTO Meta_source_tables VALUES( '2000-01-01 00:00:00.0'  ,1  ,1  ,'Y', "
            		+ "1  ,1  ,1  ,'Y'  ,'testAS_SELECT_OPTIONS'  ,'((1))'  ,'((1))'  ,1 )");
            
            stmt.executeUpdate("INSERT INTO Meta_tables VALUES( 1  ,'testTABLE_NAME'  ,'((1))'  ,'Y'  ,'testJOIN_CLAUSE', "
            		+ "'testTABLES_AND_ALIASES'  ,1 )");
         
            stmt.executeUpdate("INSERT INTO Meta_joints VALUES( 1  ,'Y'  ,'Y'  ,'Y', "
    				+ "1  ,1  ,'testPLUGIN_METHOD_NAME1'  ,'((1))'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1, "
    				+ "'testPAR_NAME'  ,1  ,1  ,'testFREE_FORMAT_TRANSFORMAT1'  ,'testMETHOD_PARAMETER1' )");
            
            stmt.executeUpdate("INSERT INTO Meta_parameter_tables VALUES( 'testPAR_NAME'  ,'testPAR_VALUE'  ,'((1))' )");
            
            stmt.executeUpdate("INSERT INTO Meta_transformation_rules VALUES( 1  ,'testTRASF'  ,'testCODE', "
            		+ "'testDESCRIPTION'  ,'((1))' )");
            
            stmt.executeUpdate("INSERT INTO Meta_versions VALUES( '((1))'  ,'testDESCRIPTION'  ,'Y'  ,'Y'  ,'testENGINE_SERVER', "
            		+ "'testMAIL_SERVER'  ,'testSCHEDULER_SERVER'  ,1 )");
            
            stmt.executeUpdate("INSERT INTO Meta_transformation_tables VALUES( 1  ,'testTABLE_NAME'  ,'testDESCRIPTION', "
            		+ "'((1))'  ,'Y'  ,1  ,1  ,1  ,1 )");
            
            stmt.executeUpdate("INSERT INTO Meta_transformation_tables VALUES( 2  ,'testTABLE_NAME'  ,'testDESCRIPTION', "
            		+ "'((1))'  ,'N'  ,1  ,1  ,1  ,1 )");
            
            Statement dcStmt = null;
		    Connection dcConn = DriverManager.getConnection("jdbc:hsqldb:mem:repdb", "dc", "dc");
		    dcStmt=dcConn.createStatement();
		      
			dcStmt.execute("CREATE TABLE public.Example_table (TESTCOLUMN_NAME VARCHAR(31))");
			dcStmt.executeUpdate("INSERT INTO public.Example_table VALUES ('1')");
      }
    catch(SQLException sqlE) {
    	System.out.println("SQLException :" + sqlE);
    }
    
    metaTransferActions = new Meta_transfer_actions(etlrep);
    metaTransferActions.setAction_contents_01("DC_E_RAN_RNC_RAW_01");
    metaVersions = new Meta_versions(etlrep);
    metaTables = new Meta_tables(etlrep);
    collection = new Meta_collections(etlrep);
    objMetaCol = new Meta_columns(etlrep ,  1L ,  "((1))",  1L ,  1L );
    connectionPool = new ConnectionPool(etlrep);
  }
  
  @Test
  public void testConstructor() throws EngineMetaDataException {
	try {  
		  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in Constructor: " + e);
	  }
  }
  
  @Test
  public void testbuildSourceFromJoin() throws Exception {
	  final Method m = SQLSource.class.getDeclaredMethod("buildSourceFromJoin", Meta_tables.class);
	  m.setAccessible(true);
	  String actual = m.invoke(new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName),metaTables).toString();
	  String expected = "(SELECT COLLECTION_SET_ID FROM null)";
	  assertEquals(expected, actual);
  }
  
  @Test
  public void testgetWhereClause() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  String actual = scs.getWhereClause();
	  String expected = "COLLECTION_SET_ID > TO_DATE('2000-01-01 00:00:00.0','yyyy-mm-dd hh24:mi:ss') AND  SRC.COLLECTION_SET_ID = out_tables.COLLECTION_SET_ID ";
	  assertEquals(expected, actual);
  }
  
  @Test
  public void testgetGroupByClause() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  String actual = scs.getGroupByClause();
	  String expected = "SUM('testPAR_VALUE')";
	  assertEquals(expected, actual);
  }
  
  @Test
  public void testgetSelectClause() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  String actual = scs.getSelectClause(true);
	  String expected = "SELECT DISTINCT SUM('testPAR_VALUE'),1 FROM (SELECT COLLECTION_SET_ID FROM testTABLES_AND_ALIASES WHERE testJOIN_CLAUSE) " +
	  		"SRC,testTABLE_NAME out_tables";
	  assertEquals(expected, actual);
  }
  
  @Test
  public void testgetSelectClause1() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  String actual = scs.getSelectClause(true,true);
	  String expected = "SELECT DISTINCT SUM('testPAR_VALUE'),1 FROM (SELECT COLLECTION_SET_ID FROM testTABLES_AND_ALIASES WHERE testJOIN_CLAUSE) " +
	  		"SRC,testTABLE_NAME out_tables";
	  assertEquals(expected, actual);
  }
  
  @Test
  public void testgetSelectClause2() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  String actual = scs.getSelectClause(true,true,true);
	  String expected = "SELECT DISTINCT SUM('testPAR_VALUE'),1 FROM (SELECT COLLECTION_SET_ID FROM testTABLES_AND_ALIASES WHERE testJOIN_CLAUSE) " +
	  		"SRC,testTABLE_NAME out_tables";
	  assertEquals(expected, actual);
  }
  
  @Test
  public void testsetLastTransferDate() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  scs.setLastTransferDate();
  }
  
  @Test
  public void testgetIsDistinct() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  boolean testFlag=false;
	  testFlag=scs.getIsDistinct();
	  assertTrue(testFlag);
  }
  
  @Test
  public void testgetConnectionId() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  Long result = null;
	  boolean testFlag=false;
	  result=scs.getConnectionId();
	  if (result!=null){
		  testFlag=true;
	  }
	  assertTrue(testFlag);
  }
  
  
  @Test
  public void testgetConnection() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  RockFactory result = null;
	  boolean testFlag=false;
	  result=scs.getConnection();
	  if (result!=null){
		  testFlag=true;
	  }
	  assertTrue(testFlag);
  }
  
  
  @Test
  public void testgetJoinedColumns() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  Meta_jointsFactory result = null;
	  boolean testFlag=false;
	  result=scs.getJoinedColumns();
	  if (result!=null){
		  testFlag=true;
	  }
	  assertTrue(testFlag);
  }
  
  
  @Test
  public void testgetColumns() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  Vector<Meta_columns> result = null;
	  boolean testFlag=false;
	  result=scs.getColumns();
	  if (result!=null){
		  testFlag=true;
	  }
	  assertTrue(testFlag);
  }
  
  
  @Test
  public void testgetParameters() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  Vector<String> result = null;
	  boolean testFlag=false;
	  result=scs.getParameters();
	  if (result!=null){
		  testFlag=true;
	  }
	  assertTrue(testFlag);
  }
  
  
  @Test
  public void testgetTransformationRules() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  Vector result = null;
	  boolean testFlag=false;
	  result=scs.getTransformationRules();
	  if (result!=null){
		  testFlag=true;
	  }
	  assertTrue(testFlag);
  }
  
  
  @Test
  public void testgetTable() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  Meta_source_tables result = null;
	  boolean testFlag=false;
	  result=scs.getTable();
	  if (result!=null){
		  testFlag=true;
	  }
	  assertTrue(testFlag);
  }

  
  @Test
  public void testgetTableName() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  String actual = scs.getTableName();
	  String expected = "(SELECT COLLECTION_SET_ID FROM testTABLES_AND_ALIASES WHERE testJOIN_CLAUSE)";
	  assertEquals(expected, actual);
  }

@Test
public void testgetSelectObjVec() throws Exception {
	  SQLSource scs = new SQLSource(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  Vector<Object> result = null;
	  final RockResultSet results = scs.getConnection().setSelectSQL("SELECT * FROM public.Example_table");
	  boolean testFlag=false;
	  result=scs.getSelectObjVec(results, true);
	  if (result!=null){
		  testFlag=true;
	  }
	  assertTrue(testFlag);
}
}