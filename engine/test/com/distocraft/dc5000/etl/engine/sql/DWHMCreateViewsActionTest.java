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
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

/**
 * @author ejamves
 *
 */
public class DWHMCreateViewsActionTest  extends UnitDatabaseTestCase{
	
	  private final Logger log = Logger.getLogger("DWHMCreateViewsActionTest");
	  
	  private static DWHMCreateViewsAction objUnderTest=null;
	  
	  private static ConnectionPool connectionPool;
	  
	  private static Meta_versions metaVersions=null;
	  private static Meta_transfer_actions metaTransferActions=null;
	  private static Meta_collections collection=null;
	  private static Long connectId = 1L;

	  
	    Long collectionSetId = 1L;
	    Long transferActionId = 1L;
	    Long transferBatchId = 1L;
	  
	  static RockFactory etlrep ;
	  static RockFactory dwhrep ;
	  
	  @BeforeClass
	  public static void setUp() throws Exception{
		  
			setup(TestType.unit);
			
		    loadDefaultTechpack(TechPack.stats, "v1");
		    loadDefaultTechpack(TechPack.stats, "v2");
		    
		    etlrep = getRockFactory(Schema.etlrep);
		    dwhrep = getRockFactory(Schema.dwhrep);
		    Statement stmt = etlrep.getConnection().createStatement();
		    Statement stmt1 = dwhrep.getConnection().createStatement();
		    
		    try {
		            stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'STATS', 'description', '((1))', 'Y', 'type')");
		            
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

		          	stmt.executeUpdate("INSERT INTO Meta_databases VALUES('dwhrep', '1', 'USER', 2, 'dwhrep', "
      	    		        + "'jdbc:hsqldb:mem:repdb', 'dwhrep', 'description', 'org.hsqldb.jdbcDriver', '')");
				    
				    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '2', 'DBA', 1, 'dwh', "
					        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
				    
				    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '3', 'USER', 1, 'dwh', "
					        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");

				    
		      	    stmt1.executeUpdate("INSERT INTO DWHTECHPACKS (TECHPACK_NAME, VERSIONID, CREATIONDATE) values ('STATS', '((1))', '2011-04-05 22:41:55.0')");

		    	   
		      	    stmt1.executeUpdate("INSERT INTO DWHType (TECHPACK_NAME,TYPENAME,TABLELEVEL,STORAGEID,PARTITIONSIZE,PARTITIONCOUNT,STATUS,TYPE," +
		      	    		"OWNER,VIEWTEMPLATE,CREATETEMPLATE,BASETABLENAME,DATADATECOLUMN,PUBLICVIEWTEMPLATE,PARTITIONPLAN) " +
		      	    		"VALUES('STATS', 'typename', 'tablelevel', 'storageid', 1, 1, 'status', 'type', 'owner', '', " +
		      	    		"'', 'Example_table', 'datadatecolumn', '', 'partitionplan')");
		      	    
		      	    stmt1.executeUpdate("INSERT INTO Dwhcolumn (STORAGEID,DATANAME,COLNUMBER,DATATYPE,DATASIZE,DATASCALE," +
		      	    		"UNIQUEVALUE,NULLABLE,INDEXES,UNIQUEKEY,STATUS,INCLUDESQL) " +
		      	    		"VALUES ('storageid','DATANAME',1,'DATATYPE',1,1,1,1,'INDEXES',1,'STATUS',1)");
		      	    

		      }
		    catch(SQLException sqlE) {
		    	System.out.println("SQLException :" + sqlE);
		    }
		    
		    metaTransferActions = new Meta_transfer_actions(etlrep);
		    metaTransferActions.setAction_contents_01("DC_E_RAN_RNC_RAW_01");
		    metaVersions = new Meta_versions(etlrep);
		    collection = new Meta_collections(etlrep);
		    connectionPool = new ConnectionPool(etlrep);
		    setupProperties();
		  }
		  
	  @Test
	  public void testConstructor() throws EngineMetaDataException {
		try {  
			Long techPackId=1l;
			objUnderTest = new DWHMCreateViewsAction(metaVersions, techPackId, collection, transferActionId, transferBatchId, connectId, etlrep, connectionPool, metaTransferActions, log);
	 		assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in Constructor: " + e);
		  }
	  }
	  
	  @Test
	  public void testExecute() throws EngineMetaDataException {
		try {
      Long techPackId=1l;
      objUnderTest = new DWHMCreateViewsAction(metaVersions, techPackId, collection, transferActionId, transferBatchId, connectId, etlrep, connectionPool, metaTransferActions, log);
			objUnderTest.execute();
			assertNotNull(objUnderTest);
		} catch (Exception e) {
		    e.printStackTrace();
		    fail("testExecute() failed");
			}
	  }
	  
	  
	  private static void setupProperties() throws Exception {
		    Properties props = new Properties();
		    props.setProperty("DWHManager.viewCreateRetries", "2");
		    props.setProperty("DWHManager.viewCreateRetryPeriod", "1");
		    props.setProperty("DWHManager.viewCreateRetryRandom", "1");

		    StaticProperties.giveProperties(props);
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
