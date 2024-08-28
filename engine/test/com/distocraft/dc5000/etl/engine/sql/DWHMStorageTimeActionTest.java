package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_tables;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.ActivationCache;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

public class DWHMStorageTimeActionTest  extends UnitDatabaseTestCase {

	  private final Logger log = Logger.getLogger("DWHMStorageTimeAction");
	  
		private static DWHMStorageTimeAction objUnderTest=null;
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
	    
	    static String batchColumnName;
	  
	  
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
	            
	    	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', 1, 'dwhrep', "
				        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
			    
			    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '2', 'DBA', 1, 'dwh', "
				        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
			    
			    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '3', 'USER', 1, 'dwh', "
				        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");

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
	    
	    StaticProperties.giveProperties(new Properties());
	  }
	  
	  
	  @Test
	  public void testConstructor() throws EngineMetaDataException {
		try {  
			objUnderTest = new DWHMStorageTimeAction(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId, 
		    		  etlrep, connectionPool, metaTransferActions,log);
	 		assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in Constructor: " + e);
		  }
	  }
	  @Test
	  public void testExecute() throws Exception{
		 try{
			 	ActivationCache.initialize(etlrep);
       objUnderTest = new DWHMStorageTimeAction(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
		    		  etlrep, connectionPool, metaTransferActions,log);
			 	objUnderTest.execute();
		 		assertNotNull(objUnderTest);
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("DWHMStorageTimeAction() failed");
				}
	  } 

	  @AfterClass
	  public static void tearDownAfterClass() throws Exception {
	    try {
		objUnderTest = null;	    	
	    }
	    catch(Exception e) {}
	  }

}
