package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

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
public class SQLExtractTest  extends UnitDatabaseTestCase{
	
	  private final Logger log = Logger.getLogger("SQLExtractTest");
	  
	  private static SQLExtract objUnderTest=null;
	  
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
		            stmt.executeUpdate("INSERT INTO public.EXAMPLE VALUES ('test1')");
		            stmt.executeUpdate("INSERT INTO public.EXAMPLE VALUES ('test2')");

		      }
		    catch(SQLException sqlE) {
		    	System.out.println("SQLException :" + sqlE);
		    }
		      final String anypath = new File("").getAbsolutePath();
		      final String fsRoot = anypath.substring(0, anypath.indexOf(File.separator));
		      
		    metaTransferActions = new Meta_transfer_actions(etlrep);
		    metaTransferActions.setAction_contents("clause=select name from public.EXAMPLE\noutputDir=temp_dir");

		    metaVersions = new Meta_versions(etlrep);
		    collection = new Meta_collections(etlrep);
		    connectionPool = new ConnectionPool(etlrep);
		  }
		  
	  @Test
	  public void testConstructor() throws EngineMetaDataException {
		try {  
			objUnderTest = new SQLExtract(metaVersions, collectionSetId, collection,transferActionId, transferBatchId, 
		    		  connectId, etlrep,connectionPool, metaTransferActions);
	 		assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in Constructor: " + e);
		  }
	  }
	  
		
	  @Test
	  public void testExecute(){
		 try{
			 objUnderTest.execute();
	    } catch (Exception e) {
	        e.printStackTrace();
	        fail("testExecute() failed");
	      }
}
	  
	  @Test
	  public void testSQLExtract() throws EngineMetaDataException {
		try {  
	    	objUnderTest = null;
			objUnderTest = new SQLExtract();
	 		assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in testSQLExtract: " + e);
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
