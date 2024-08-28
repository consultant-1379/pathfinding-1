package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

/**
 * @author ejamves
 *
 */
public class ExportActionTest  extends UnitDatabaseTestCase{
	
	  private static ExportAction objUnderTest=null;
	  private static ConnectionPool connectionPool;
	  private static Meta_versions metaVersions=null;
	  private static Meta_transfer_actions metaTransferActions=null;
	  private static Meta_collections collection=null;
	  private final static SetContext sctx = new SetContext();

		
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
		    	    
					  
		    	    stmt.execute("CREATE TABLE EXAMPLE_Table (ID VARCHAR(31),VALUE  VARCHAR(31))");
				    
		    	    stmt.executeUpdate("INSERT INTO EXAMPLE_Table VALUES('1', 'testvalue')");

		      }
		    catch(SQLException sqlE) {
		    	System.out.println("SQLException :" + sqlE);
		    }
		    
		    metaTransferActions = new Meta_transfer_actions(etlrep);
		    metaTransferActions.setAction_contents_01("DC_E_RAN_RNC_RAW_01");
		    metaVersions = new Meta_versions(etlrep);
		    collection = new Meta_collections(etlrep);
		    connectionPool = new ConnectionPool(etlrep);
		    List tables = new ArrayList();
		    tables.add("Example_table");
		    sctx.put("tableList", tables);
		  }
		  
	  @Test
	  public void testConstructor() throws EngineMetaDataException {
		try {  
		      objUnderTest = new ExportAction(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, sctx);
		      assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in Constructor: " + e);
		  }
	  }
	  
	  @Test
	  public void testExportActionNullException() throws Exception {
	    Meta_transfer_actions mta = new Meta_transfer_actions(etlrep);
	    mta.setWhere_clause("tables = EXAMPLE_TABLE \n" + "EXAMPLE_TABLE.sqlSelect = select ID from EXAMPLE_TABLE \n"
	    + "EXAMPLE_TABLE.sqlClause = where VALUE = 'testvalue'");

	    try {
	      objUnderTest = new ExportAction(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, mta, sctx);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	    try {
	      objUnderTest.execute();
	      String expected = "{exportData=<dataset>\n " + " <EXAMPLE_TABLE ID=\"1\"/>\n</dataset>\n, tableList=[Example_table]}";
	      assertEquals(expected, sctx.toString());
	    } catch (Exception e) {
	        e.printStackTrace();	
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
