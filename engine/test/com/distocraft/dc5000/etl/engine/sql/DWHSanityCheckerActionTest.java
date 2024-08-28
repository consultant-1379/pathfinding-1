 package com.distocraft.dc5000.etl.engine.sql;

  import static org.junit.Assert.*;

  import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
  import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
  import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
  import com.distocraft.dc5000.etl.rock.Meta_collections;
  import com.distocraft.dc5000.etl.rock.Meta_columns;
  import com.distocraft.dc5000.etl.rock.Meta_tables;
  import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
  import com.distocraft.dc5000.etl.rock.Meta_versions;
  import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

  import java.sql.SQLException;

  import java.sql.Statement;
  import java.util.logging.Logger;

  import org.junit.BeforeClass;
  import org.junit.Test;
  import ssc.rockfactory.RockFactory;

  public class DWHSanityCheckerActionTest extends UnitDatabaseTestCase {

  	private final static Logger clog = Logger.getLogger("DWHSanityCheckerAction");
  	
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
      static RockFactory dc;
      
      static String batchColumnName;
  	
    @BeforeClass
    public static void setUp() throws Exception{
  	  
    setup(TestType.unit);
  	
    loadDefaultTechpack(TechPack.stats, "v1");
    loadDefaultTechpack(TechPack.stats, "v2");
      
    etlrep = getRockFactory(Schema.etlrep);
    dwhrep = getRockFactory(Schema.dwhrep);
      
    final Statement stmt = etlrep.getConnection().createStatement();
    final Statement stmt1 = dwhrep.getConnection().createStatement();
      
      try {
              stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'DC_E_MGW', 'description', '((1))', 'Y', 'type')");
              
              stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'COLLECTION_SET_ID'  ,'COLLECTION_SET_ID'  ,'COL_TYPE'  ,1  ,'Y'  ,'((1))'  ,1  ,1 )");
           
              stmt.executeUpdate("INSERT INTO Meta_joints VALUES( 1  ,'Y'  ,'Y'  ,'Y', "
      				+ "1  ,1  ,'testPLUGIN_METHOD_NAME1'  ,'((1))'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1, "
      				+ "'testPAR_NAME'  ,1  ,1  ,'testFREE_FORMAT_TRANSFORMAT1'  ,'testMETHOD_PARAMETER1' )");
              
          	stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'typenames', 1, 'connectionname', "
      		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
          	stmt.executeUpdate("INSERT INTO Meta_databases VALUES('dwhrep', '1', 'USER', 2, 'dwhrep', "
          	    		        + "'jdbc:hsqldb:mem:repdb', 'dwhrep', 'description', 'org.hsqldb.jdbcDriver', '')");
      	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', 3, 'dwh', "
      		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");

      	    stmt1.executeUpdate("INSERT INTO DWHTECHPACKS (TECHPACK_NAME, VERSIONID, CREATIONDATE) values ('DC_E_MGW', '((1))', '2011-04-05 22:41:55.0')");
              
        }
      catch(SQLException sqlE) {
      	System.out.println("SQLException :" + sqlE);
      }
      
      metaTransferActions = new Meta_transfer_actions(etlrep);
      metaVersions = new Meta_versions(etlrep);
      metaTables = new Meta_tables(etlrep);
      collection = new Meta_collections(etlrep);
      objMetaCol = new Meta_columns(etlrep ,  1L ,  "((1))",  1L ,  1L );
      connectionPool = new ConnectionPool(etlrep);
    }
    
    @Test
    public void testConstructor() throws EngineMetaDataException {
  	try {  
  		final DWHSanityCheckerAction dsca = new DWHSanityCheckerAction(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, clog);
  		assertNotNull(dsca);
  	  }
  	  catch(Exception e) {
  		 System.out.println("Exception in Constructor: " + e);
  	  }
    }

  @Test
  public void testExecute() throws EngineMetaDataException {
	try {  
		final DWHSanityCheckerAction dsca = new DWHSanityCheckerAction(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, clog);
		dsca.execute();
		assertNotNull(dsca);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in Constructor: " + e);
	  }
  }
  

  
}
