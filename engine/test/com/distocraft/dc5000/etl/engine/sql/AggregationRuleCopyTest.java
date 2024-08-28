package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.AggregationRuleCache;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

/**
 * @author ejamves
 *
 */
public class AggregationRuleCopyTest  extends UnitDatabaseTestCase{
	
	  private final Logger log = Logger.getLogger("AggregationRuleCopyTest");
	  
	  private static AggregationRuleCopy objUnderTest=null;
	  
	  private static ConnectionPool connectionPool;
	  
	  private static ResultSet resultSet;
	  
	  private static Meta_versions metaVersions=null;
	  private static Meta_transfer_actions metaTransferActions=null;
	  private static Meta_collections collection=null;
	  private static Long connectId = 1L;
	  private static String connectionName="connectionname";
	  private static String userType="typenames";  
	  private static boolean resultsetClosed=false;
	  
	    Long collectionSetId = 1L;
	    Long transferActionId = 1L;
	    Long transferBatchId = 1L;
	  
	  static RockFactory etlrep ;
	  static RockFactory dwhrep ;
	  static RockFactory sa ;
	  
	  @BeforeClass
	  public static void setUp() throws Exception{
		  
			setup(TestType.unit);
			
		    loadDefaultTechpack(TechPack.stats, "v1");
		    loadDefaultTechpack(TechPack.stats, "v2");
		    
		    etlrep = getRockFactory(Schema.etlrep);
		    dwhrep = getRockFactory(Schema.dwhrep);
		    sa = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver",
					"SA", true);
		    
		    Statement stmt = etlrep.getConnection().createStatement();
		    Statement stmt1 = sa.getConnection().createStatement();
		    
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
		            
		    	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'typenames', 1, 'connectionname', "
		    		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
		          	stmt.executeUpdate("INSERT INTO Meta_databases VALUES('dwhrep', '1', '', 2, 'dwhrep', "
		          	    		        + "'jdbc:hsqldb:mem:repdb', 'dwhrep', 'description', 'org.hsqldb.jdbcDriver', '')");
		      	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', 3, 'dwh', "
		      		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
		      	    
		      	  
		      	    stmt1.execute("CREATE TABLE LOG_AGGREGATIONRULES (AGGREGATION varchar(255), VERSIONID varchar(255), RULEID bigint,TARGET_TYPE varchar(255)," +
		      	    		"TARGET_LEVEL varchar(255),TARGET_TABLE varchar(255), TARGET_MTABLEID varchar(255),SOURCE_TYPE varchar(255),SOURCE_LEVEL varchar(255)," +
		      	    		"SOURCE_TABLE varchar(255),SOURCE_MTABLEID varchar(255),RULETYPE varchar(255),AGGREGATIONSCOPE varchar(255),BHTYPE varchar(255)," +
		      	    		"ENABLE bigint)");
		      	  
		      	    stmt1.executeUpdate("INSERT INTO LOG_AGGREGATIONRULES VALUES('AGGREGATION', 'VERSIONID', 1, 'TARGET_TYPE', 'TARGET_LEVEL', "
		      		        + "'TARGET_TABLE', 'TARGET_MTABLEID', 'SOURCE_TYPE', 'SOURCE_LEVEL', 'SOURCE_TABLE','SOURCE_MTABLEID','RULETYPE','AGGREGATIONSCOPE'," +
		      		        		"'BHTYPE',1)");

		      }
		    catch(SQLException sqlE) {
		    	System.out.println("SQLException :" + sqlE);
		    }
		    
		    metaTransferActions = new Meta_transfer_actions(etlrep);
		    metaTransferActions.setAction_contents_01("DC_E_RAN_RNC_RAW_01");
		    metaVersions = new Meta_versions(etlrep);
		    collection = new Meta_collections(etlrep);
		    connectionPool = new ConnectionPool(etlrep);
		  }
		  
	  @Test
	  public void testConstructor() throws EngineMetaDataException {
		try {  
			objUnderTest = new AggregationRuleCopy(metaVersions, collectionSetId, collection, transferActionId, transferBatchId, connectId, etlrep, metaTransferActions, log);
	 		assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in Constructor: " + e);
		  }
	  }
	  
		@Test
		 public void testExecute() throws Exception {
			 try{
				 AggregationRuleCache.initialize(etlrep);
         objUnderTest = new AggregationRuleCopy(metaVersions, collectionSetId, collection, transferActionId, transferBatchId, connectId, etlrep, metaTransferActions, log);
				 objUnderTest.execute();
			    } catch (Exception e) {
			        e.printStackTrace();
			        assertEquals("",e.getMessage());
			      }
		 }

	  
	  @AfterClass
	  public static void tearDownAfterClass() throws Exception {
	    try {
	    	objUnderTest = null;
	    }
	    catch(Exception e) {
	        e.printStackTrace();	    	
	    }
	  }
}
