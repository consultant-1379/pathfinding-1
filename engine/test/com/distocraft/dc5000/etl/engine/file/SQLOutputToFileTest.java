package com.distocraft.dc5000.etl.engine.file;

import static org.junit.Assert.*;

import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_tables;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

public class SQLOutputToFileTest extends UnitDatabaseTestCase {

	static Meta_transfer_actions metaTransferActions;
	static Meta_versions metaVersions;
	static Meta_collections collection;
	static Meta_columns objMetaCol;
	static Meta_tables metaTables;
	static ConnectionPool  connectionPool;
	
    Long collectionSetId = 1L;
    Long transferActionId = 1L;
    Long transferBatchId = 1L;
    Long connectId = 1L;
    
    static RockFactory etlrep ;
    static RockFactory dwhrep ;
    
    static String batchColumnName;
	
  @BeforeClass
  public static void setUp() throws Exception{
	  
	setup(TestType.unit);
	
    loadDefaultTechpack(TechPack.stats, "v1");
    
    etlrep = getRockFactory(Schema.etlrep);
   
    Statement stmt = etlrep.getConnection().createStatement();
    
    try {
    	
    	stmt.executeUpdate("INSERT INTO Meta_files VALUES( 1  ,'testF_NAME'  ,'testF_C_T'  ,'\n', "
    			+ "'\t'  ,1  ,1  ,1  ,'Y'  ,'((1))'  ,1 )");
    	
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
        
    }
    catch(SQLException sqlE) {
    	System.out.println("SQLException :" + sqlE);
    }
    
    metaTransferActions = new Meta_transfer_actions(etlrep);
    metaVersions = new Meta_versions(etlrep);
    metaTables = new Meta_tables(etlrep);
    collection = new Meta_collections(etlrep);
    connectionPool = new ConnectionPool(etlrep);
  }
   
  @Test
  public void testConstructor() throws Exception {
	  try {	  
		  SQLOutputToFile sotf = new SQLOutputToFile(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in getFile: " + e);
	  }
  }
  
  @Test
  public void testInsertToFile() throws Exception {
	  try {	  
		  
		  RockResultSet rrs = new RockResultSet();
		  
		  final String query = "Select * from Meta_columns";
		  
		  Statement statement = null;
		  ResultSet resultSet = null;
			
          statement = etlrep.getConnection().createStatement();
		  resultSet = statement.executeQuery(query);
		  rrs.setResultSet(resultSet, statement); 
		  
		  final Method m = SQLOutputToFile.class.getDeclaredMethod("InsertToFile", RockResultSet.class);
		  m.setAccessible(true);
		  m.invoke(new SQLOutputToFile(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName),rrs);
	  } 
	  catch(Exception e) {
		 System.out.println("Exception in InsertToFile: " + e);
	  }
  }
  
  @Test
  public void testgetBatchColumnName() throws Exception {
	  try {	  
		  SQLOutputToFile sotf = new SQLOutputToFile(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
		  String actual = sotf.getBatchColumnName();
		  assertEquals(null, actual);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in getFile: " + e);
	  }
  }
  
}
