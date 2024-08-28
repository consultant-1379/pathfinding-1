package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;

import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

import java.sql.Statement;
import java.util.logging.Logger;

import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;

public class DWHMVersionUpdateActionTest extends UnitDatabaseTestCase {
	     
	    private final static Logger clog = Logger.getLogger("DWHMVersionUpdateAction");

		static Meta_transfer_actions metaTransferActions;
		static Meta_versions metaVersions;
		static Meta_collections collection;
		static Meta_columns objMetaCol;
		static ConnectionPool  connectionPool;
		
	    Long collectionSetId = 1L;
	    Long transferActionId = 1L;
	    Long transferBatchId = 1L;
	    Long connectId = 1L;
	    
	    static RockFactory etlrep ;
	 
	  @BeforeClass
	  public static void setUp() throws Exception {
		
	    setup(TestType.unit);
		
	    loadDefaultTechpack(TechPack.stats, "v1");
	    loadDefaultTechpack(TechPack.stats, "v2");
	    
	    etlrep = getRockFactory(Schema.etlrep);
	 	    
	    Statement stmt = etlrep.getConnection().createStatement();
	    
	    try {
	    
	    stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'set_name', 'description', '1', 'Y', 'type')");
	    
		stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'testCOLUMN_NAME'  ,'testCOLUMN_ALIAS_NAME'  ,'testCOLUMN_TYPE', "
				+ "1  ,'Y'  ,'((1))'  ,1  ,1 )");
		
		stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'typenames', 1, 'connectionname', "
		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', 2, 'dwhrep', "
		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', 3, 'dwh', "
		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
	
	    }
	    catch(Exception e){
	    	System.out.println(e);
	    }

	    metaTransferActions = new Meta_transfer_actions(etlrep);
	    metaVersions = new Meta_versions(etlrep);
	    collection = new Meta_collections(etlrep);
	    objMetaCol = new Meta_columns(etlrep ,  1L ,  "((1))",  1L ,  1L );
	    connectionPool = new ConnectionPool(etlrep);
	  }
	  
	  @Test
	  public void testConstructor() throws Exception{
		 try{
			 DWHMVersionUpdateAction dvua = new DWHMVersionUpdateAction(metaVersions,collectionSetId, collection, transferActionId, transferBatchId, connectId, 
		    		  etlrep, connectionPool, metaTransferActions, clog);
			 assertNotNull(dvua);
			} catch (Exception e) {
			    e.printStackTrace();
			    fail("DWHMVersionUpdateActionTest() failed");
				}
	  } 

}
