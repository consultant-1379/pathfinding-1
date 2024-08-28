package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache.PTableEntry;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

/**
 * @author ejamves
 *
 */
public class PartitionedSQLExecuteTest  extends UnitDatabaseTestCase{
	
	  private final Logger log = Logger.getLogger("PartitionedSQLExecuteTest");
	  private final static SetContext sctx = new SetContext();	  
	  
	  private static PartitionedSQLExecute objUnderTest=null;
	  
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
		            stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'DC_E_MGW', 'description', '((1))', 'Y', 'type')");
		            
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
		          	stmt.executeUpdate("INSERT INTO Meta_databases VALUES('dwhrep', '1', 'USER', 2, 'dwhrep', "
		          	    		        + "'jdbc:hsqldb:mem:repdb', 'dwhrep', 'description', 'org.hsqldb.jdbcDriver', '')");
		      	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', 3, 'dwh', "
		      		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");

		      	    stmt1.executeUpdate("INSERT INTO DWHType (TECHPACK_NAME,TYPENAME,TABLELEVEL,STORAGEID,PARTITIONSIZE,PARTITIONCOUNT,STATUS,TYPE," +
		      	    		"OWNER,VIEWTEMPLATE,CREATETEMPLATE,BASETABLENAME,DATADATECOLUMN,PUBLICVIEWTEMPLATE,PARTITIONPLAN) " +
		      	    		"VALUES('DC_E_MGW', 'typename', 'tablelevel', 'storageid', 1, 1, 'ACTIVE', 'type', 'owner', 'viewtemplate', " +
		      	    		"'createtemplate', 'Example_table', 'datadatecolumn', 'publicviewtemplate', 'partitionplan')");
		      	    
			      	stmt1.executeUpdate("INSERT INTO DWHPartition (STORAGEID,TABLENAME,STATUS,STARTTIME,ENDTIME,LOADORDER) VALUES('storageid','EXAMPLE_Table','ACTIVE','2011-04-05 22:41:55.0','2011-04-05 22:55:55.0',1)");
			      	stmt1.executeUpdate("INSERT INTO TPActivation (TECHPACK_NAME,STATUS,TYPE) VALUES('DC_E_MGW','ACTIVE','type')");
			      	stmt1.executeUpdate("INSERT INTO TypeActivation (TECHPACK_NAME,STATUS,TYPE,TYPENAME,TABLELEVEL) VALUES('DC_E_MGW','ACTIVE','type','typename','tablelevel')");
			      	stmt1.executeUpdate("INSERT INTO PartitionPlan (PARTITIONPLAN,DEFAULTPARTITIONSIZE,DEFAULTSTORAGETIME,PARTITIONTYPE) VALUES('partitionplan','1',3,1)");		      	  
			      	    
		            stmt.execute("create table public.EXAMPLE_Table (name varchar(255))");
		            stmt.executeUpdate("INSERT INTO public.EXAMPLE_Table VALUES ('test1')");
		            stmt.executeUpdate("INSERT INTO public.EXAMPLE_Table VALUES ('test2')");
		            
		           /** String sqlPartition = "SELECT dwhp.storageid, dwhp.tablename, dwhp.starttime, dwhp.endtime, dwhp.status, dwht.partitionsize, plan.defaultpartitionsize, dwhp.loadorder " 
											+ "FROM DWHPartition dwhp, DWHType dwht, TPActivation tpa, TypeActivation ta, PartitionPlan plan WHERE dwht.storageid = dwhp.storageid " 
											+ "AND dwht.techpack_name = tpa.techpack_name AND dwht.techpack_name = ta.techpack_name AND dwht.typename = ta.typename " 
											+ "AND dwht.tablelevel = ta.tablelevel AND dwht.partitionplan = plan.partitionplan AND tpa.status = 'ACTIVE' AND ta.status='ACTIVE'";
           
		            
		            ResultSet rst = stmt1.executeQuery(sqlPartition);
		            
		            while (rst.next()) {
		            		System.out.println(rst.getString(1));
		            		System.out.println(rst.getString(2));
		            		System.out.println(rst.getString(3));
		            		System.out.println(rst.getString(4));
		            		System.out.println(rst.getString(5));
		            		System.out.println(rst.getString(6));
		            		System.out.println(rst.getString(7));
		            		System.out.println(rst.getString(8));
		            }
		            **/

		      }
		    catch(SQLException sqlE) {
		    	System.out.println("SQLException :" + sqlE);
		    }
		    
		    metaTransferActions = new Meta_transfer_actions(etlrep);
		    metaVersions = new Meta_versions(etlrep);
		    collection = new Meta_collections(etlrep);
		    connectionPool = new ConnectionPool(etlrep);
		    
		    List tables = new ArrayList();
		    tables.add("Example_table_01");
		    sctx.put("tableList", tables);
		  }
		  
	  @Test
	  public void testConstructorTrue() throws EngineMetaDataException {
		try {  
		    metaTransferActions.setAction_contents("select * from public.EXAMPLE_Table");
		    metaTransferActions.setWhere_clause("tablename=Example_table\nuseOnlyLoadedPartitions=1\naggStatusCacheUpdate=false");
			objUnderTest = new PartitionedSQLExecute(metaVersions,collectionSetId,collection,transferActionId,transferBatchId,connectId,etlrep,connectionPool,metaTransferActions,sctx,log);
	 		assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in testConstructorTrue: " + e);
		  }
	  }
	  
	  @Test
	  public void testExecuteTrue() throws EngineMetaDataException {
		try {  
			objUnderTest.execute();
			assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in testExecuteTrue: " + e);
		  }
	  }
	  
	  @Test
	  public void testConstructorFalse() throws EngineMetaDataException {
		try {  
		    metaTransferActions.setAction_contents("select * from public.EXAMPLE_Table");
		    metaTransferActions.setWhere_clause("tablename=Example_table\nuseOnlyLoadedPartitions=0\naggStatusCacheUpdate=true");
			objUnderTest = new PartitionedSQLExecute(metaVersions,collectionSetId,collection,transferActionId,transferBatchId,connectId,etlrep,connectionPool,metaTransferActions,sctx,log);
	 		assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in testConstructorFalse: " + e);
		  }
	  }
	  @Test
	  @Ignore
	  public void testExecuteFalse() throws EngineMetaDataException {
		try {  
			objUnderTest.execute();
			assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in testExecuteFalse: " + e);
		  }
	  }
	  
	  @Test
	  public void testPartitionedSQLExec() throws EngineMetaDataException {
		try {
			objUnderTest=null;
			objUnderTest = new PartitionedSQLExecute();
	 		assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in testPartitionedSQLExec: " + e);
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
