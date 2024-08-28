package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_joints;
import com.distocraft.dc5000.etl.rock.Meta_jointsFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase.Schema;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase.TechPack;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase.TestType;

public class SQLUpdateTest extends UnitDatabaseTestCase{

	  	private final Logger log = Logger.getLogger("SQLUpdateTest");
	  	private static Meta_transfer_actions metaTransferActions;
	  	private static Meta_versions metaVersions;
	  	private static Meta_collections collection;
	  	private static ConnectionPool  connectionPool;
	  	private static Meta_joints metaJoints;
	  	private static Meta_jointsFactory metaJointsFactory;
		
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
					    stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'set_name', 'description', '1', 'Y', 'type')");
					    
						stmt.executeUpdate("INSERT INTO Meta_Target_Tables VALUES('((89))', 1, 1, 1, 1, 1)");
						
						stmt.executeUpdate("INSERT INTO Meta_Tables VALUES(1, 'EXAMPLE_TABLE', '((89))', 'N', '', 'EXAMPLE_TABLE', 1)");
						
						stmt.executeUpdate("INSERT INTO Meta_joints VALUES( 1  ,'N'  ,'N'  ,'Y', 1  ,1  ,'testPLUGIN_METHOD_NAME1'  ,'testVERSION_NUMBER1'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1, "
								+ "'testPAR_NAME1'  ,1  ,1  ,'testFREE_FORMAT_TRANSFORMAT1'  ,'testMETHOD_PARAMETER1' )");
						 /**
						stmt.executeUpdate("INSERT INTO Meta_fk_table_joints VALUES( '((89))'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1 )");
			   				**/		    
					    stmt.executeUpdate("INSERT INTO Meta_Source_Tables (TRANSFER_ACTION_ID, TABLE_ID,USE_TR_DATE_IN_WHERE_FLAG , COLLECTION_SET_ID , COLLECTION_ID , CONNECTION_ID, "
						        + "DISTINCT_FLAG, AS_SELECT_OPTIONS, AS_SELECT_TABLESPACE, VERSION_NUMBER , TIMESTAMP_COLUMN_ID)" +
						        		" VALUES(1, 1, 'N', 1, 1, 1, 'Y', 'AS_SELECT_OPTIONS', 'AS_SELECT_TABLESPACE', '1', 1)");

						stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'testCOLUMN_NAME'  ,'testCOLUMN_ALIAS_NAME'  ,'testCOLUMN_TYPE', "
								+ "1  ,'N'  ,'((89))'  ,1  ,1 )");
						
					    stmt.executeUpdate("INSERT INTO Meta_Parameter_tables VALUES('testPAR_NAME1', '1', 'testVERSION_NUMBER1')");
			            
					    stmt.executeUpdate("INSERT INTO META_TRANSFORMATION_RULES VALUES(1 ,'TRANS_NAME' ,'CODE', 'DESCRIPTION' ,'testVERSION_NUMBER1')");	 
					    
			            stmt.executeUpdate("INSERT INTO Meta_versions VALUES( '((1))'  ,'testDESCRIPTION'  ,'Y'  ,'Y'  ,'testENGINE_SERVER', "
			            		+ "'testMAIL_SERVER'  ,'testSCHEDULER_SERVER'  ,1 )");
			            
					    stmt.executeUpdate("INSERT INTO META_TRANSFORMATION_TABLES VALUES(1 ,'TRANSF_TABLE_NAME' ,'DESCRIPTION', 'testVERSION_NUMBER1' ,''," +
			    		"1,1,1,1)");	 
			    	    
					    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', 1, 'dwhrep', "
						        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
					    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '2', 'DBA', 1, 'dwh', "
						        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");

					      
            stmt.execute("CREATE TABLE Example_table (TESTCOLUMN_NAME VARCHAR(31))");
						    
            stmt.executeUpdate("INSERT INTO Example_table VALUES ('1')");
						
			      }
			    catch(SQLException sqlE) {
			    	System.out.println("SQLException :" + sqlE);
			    }
			    
			    metaTransferActions = new Meta_transfer_actions(etlrep);
			    metaTransferActions.setAction_contents_01("DC_E_RAN_RNC_RAW_01");
				//metaTransferActions.setWhere_clause("TESTCOLUMN_NAME = '1'");
			    metaVersions = new Meta_versions(etlrep);
			    collection = new Meta_collections(etlrep);
			    connectionPool = new ConnectionPool(etlrep);
			    metaJoints=new Meta_joints(etlrep);
			    metaJointsFactory=new Meta_jointsFactory(etlrep, metaJoints);
			  }

		  
		  @Test
		  public void testConstructor() throws EngineMetaDataException {
			try {
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
		 		assertNotNull(objUnderTest);
			  }
			  catch(Exception e) {
				 System.out.println("Exception in Constructor: " + e);
			  }
		  }
	@Test
	@Ignore
	  public void testExecute() throws Exception{
		 try{
			 boolean testSQLUpdateTest=false;
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
			 	objUnderTest.execute();
				testSQLUpdateTest=true;
				assertTrue(testSQLUpdateTest);
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("testSQLUpdateTest_testExecute() failed");
				}
	  } 
	  
	  @Test
	  public void testExecuteSQLThroughJava() throws Exception{
		 try{
			 	boolean testSQLUpdateTest=false;
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
				objUnderTest.executeThroughJava();
				testSQLUpdateTest=true;
				assertTrue(testSQLUpdateTest);  
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("testSQLUpdateTest_testExecuteSQLUpdate() failed");
				}
	  } 

		  @Test
		  public void testExecuteFkCheck() throws Exception{
			 try{
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
				 	int testSQLUpdateTest=objUnderTest.executeFkCheck();
					assertNotNull(testSQLUpdateTest);  
				} catch (Exception e) {
				    e.printStackTrace();
				    fail("testSQLUpdateTest_testExecuteFkCheck failed");
					}
		  } 

		  
		  @Test
		  public void testRemoveDataFromTarget() throws Exception{
			 try{			 	
				boolean testSQLUpdateTest=false;
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
				objUnderTest.removeDataFromTarget();
				testSQLUpdateTest=true;
				assertTrue(testSQLUpdateTest);    
				} catch (Exception e) {
				    e.printStackTrace();
				    fail("testSQLUpdateTest_testRemoveDataFromTarget failed");
					}
		  } 	  
		  
		  @Test
		  public void testgetUpdateClause(){
			 try{
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
				String testgetUpdateClause= objUnderTest.getUpdateClause(); 
				String expected = "UPDATE EXAMPLE_TABLE TRG SET TRG.testCOLUMN_NAME='1'";
				assertEquals(expected,testgetUpdateClause);
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("testgetUpdateClause() failed");
				}
		  }
		  
		  @Test
		  public void testgetUpdateSelectClause(){
			 try{
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
				String testgetUpdateSelectClause= objUnderTest.getUpdateSelectClause(); 
				String expected = "SELECT DISTINCT '1' FROM EXAMPLE_TABLE SRC";
				assertEquals(expected,testgetUpdateSelectClause);
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("testgetUpdateSelectClause() failed");
				}
		  }
		  
		  @Test
		  public void testgetSource(){
			 try{
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
				SQLSource testgetSource= objUnderTest.getSource(); 
				assertNotNull(testgetSource);
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("testgetSource() failed");
				}
		  }
		  
		  @Test
		  public void testgetTarget(){
			 try{
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
				SQLTarget testgetTarget= objUnderTest.getTarget(); 
				assertNotNull(testgetTarget);
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("testgetTarget() failed");
				}
		  }
		  
		  @Test
		  public void testgetPreparedUpdateClause(){
			 try{
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
				String testgetPreparedUpdateClause= objUnderTest.getPreparedUpdateClause(true); 
				String expected = "UPDATE EXAMPLE_TABLE TRG SET testCOLUMN_NAME = ? ";
				assertEquals(expected,testgetPreparedUpdateClause);
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("testgetPreparedUpdateClause() failed");
				}
		  }
		  
			 @Test
			  public void testsetSybaseUpdateClause() throws Exception{
				 try{
					 boolean testsetSybaseUpdateClause=false;
        final SQLUpdate objUnderTest = new SQLUpdate(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId,
			    		  etlrep, connectionPool, metaTransferActions,"batchColumnName");
					   Method privateMethod = SQLUpdate.class.getDeclaredMethod("setSybaseUpdateClause", new Class[] {});
					    privateMethod.setAccessible(true);
					 	privateMethod.invoke(objUnderTest, null);
					 	testsetSybaseUpdateClause=true;
						assertTrue(testsetSybaseUpdateClause);
					} catch (Exception e) {
					    e.printStackTrace();
					    fail("testsetSybaseUpdateClause() failed");
						}
			  } 


}
