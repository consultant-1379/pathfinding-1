package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

public class SQLActionExecuteTest extends UnitDatabaseTestCase{

	  	private static Meta_transfer_actions metaTransferActions;
	  	private static Meta_versions metaVersions;
	  	private static Meta_collections collection;
	  	private static ConnectionPool  connectionPool;
	    
		private Method m;    
		private static String METHOD_NAME = "getLoggerName";    
		private Class[] parameterTypes;    
		private Object[] parameters;    

		
	    Long collectionSetId = 1L;
	    Long transferActionId = 1L;
	    Long transferBatchId = 1L;
	    Long connectId = 1L;
	    String batchColumnName;
	    
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
			    	    
			            stmt.execute("create table public.EXAMPLE (name varchar(255))");
			            stmt.executeUpdate("INSERT INTO public.EXAMPLE VALUES ('${1')");
			            stmt.executeUpdate("INSERT INTO public.EXAMPLE VALUES ('${2')");
			            
			      }
			    catch(SQLException sqlE) {
			    	System.out.println("SQLException :" + sqlE);
			    }
			    
			    metaTransferActions = new Meta_transfer_actions(etlrep);
			    metaTransferActions.setAction_contents_01("update public.EXAMPLE SET NAME = '${10}' WHERE NAME = '${1'");
			    metaTransferActions.setWhere_clause("${");
			    metaVersions = new Meta_versions(etlrep);
			    collection = new Meta_collections(etlrep);
			    connectionPool = new ConnectionPool(etlrep);
			  }

		  @Test
		  public void testConstructorSQLActionExecute() throws EngineMetaDataException {
			try {  
				final SQLActionExecute objUnderTest = new SQLActionExecute();
		 		assertNotNull(objUnderTest);
			  }
			  catch(Exception e) {
				 System.out.println("Exception in Constructor: " + e);
			  }
		  }
		  
		  @Test
		  public void testConstructor() throws EngineMetaDataException {
			try {
			    final SQLActionExecute objUnderTest = new SQLActionExecute(metaVersions, collectionSetId, collection,
            transferActionId, transferBatchId, connectId, etlrep, connectionPool, metaTransferActions);
		 		assertNotNull(objUnderTest);
			  }
			  catch(Exception e) {
				 System.out.println("Exception in Constructor: " + e);
			  }
		  }
		  

	  @Test
	  public void testExecute() throws Exception{
		 try{
			 boolean testSQLActionExecute=false;
       final SQLActionExecute objUnderTest = new SQLActionExecute(metaVersions, collectionSetId, collection,
            transferActionId, transferBatchId, connectId, etlrep, connectionPool, metaTransferActions);
				try{
			 	objUnderTest.execute();
				} catch (Exception e) {
					e.printStackTrace();
					fail("testSQLActionExecute_testExecute() failed");
				}
				testSQLActionExecute=true;
				assertTrue(testSQLActionExecute);
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("testSQLActionExecute_testExecute() failed");
				}
	  } 
	  
	  @Test
	  public void testExecuteSQLUpdate() throws Exception{
		 try{
       final SQLActionExecute objUnderTest = new SQLActionExecute(metaVersions, collectionSetId, collection,
            transferActionId, transferBatchId, connectId, etlrep, connectionPool, metaTransferActions);
			 String sqlClause = objUnderTest.getTrActions().getAction_contents();
			 int testSQLActionExecute=objUnderTest.executeSQLUpdate(sqlClause);
				assertNotNull(testSQLActionExecute);  
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("testSQLActionExecute_testExecuteSQLUpdate() failed");
				}
	  } 
	  
	  @Test
	  public void testgetLoggerName() throws Exception{
      final SQLActionExecute objUnderTest = new SQLActionExecute(metaVersions, collectionSetId, collection,
            transferActionId, transferBatchId, connectId, etlrep, connectionPool, metaTransferActions);
		 try{

				parameterTypes = new Class[3];        
				parameterTypes[0] = java.lang.Long.class;
				parameterTypes[1] = RockFactory.class;
				parameterTypes[2] = java.lang.String.class;
				m = objUnderTest.getClass().getDeclaredMethod(METHOD_NAME, parameterTypes);        
				m.setAccessible(true);        
				parameters = new Object[3];  
				parameters[0] = objUnderTest.getCollectionSetId();
				parameters[1] = objUnderTest.getRockFact(); 
				parameters[2] = objUnderTest.getTrActions().getTransfer_action_name();
				String result = (String) m.invoke(objUnderTest, parameters);     
				assertNotNull(result);   
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("testSQLActionExecute_testgetLoggerName() failed");
				}
	  } 

}
