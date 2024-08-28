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

public class SQLLogResultSetTest extends UnitDatabaseTestCase {

	private final static Logger clog = Logger.getLogger("SQLLogResultSet");
	
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
    
    Statement stmt = etlrep.getConnection().createStatement();
    
    try {
            stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'set_name', 'description', '((1))', 'Y', 'type')");
            
            stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'COLLECTION_SET_ID'  ,'COLLECTION_SET_ID'  ,'COL_TYPE'  ,1  ,'Y'  ,'((1))'  ,1  ,1 )");
            
            stmt.execute("create table public.EXAMPLE (name varchar(255))");
            stmt.executeUpdate("INSERT INTO public.EXAMPLE VALUES ('test1')");
            stmt.executeUpdate("INSERT INTO public.EXAMPLE VALUES ('test2')");
            
      }
    catch(SQLException sqlE) {
    	System.out.println("SQLException :" + sqlE);
    }
    
    metaTransferActions = new Meta_transfer_actions(etlrep);
    metaTransferActions.setAction_contents_01("select name from public.EXAMPLE");
    metaVersions = new Meta_versions(etlrep);
    metaTables = new Meta_tables(etlrep);
    collection = new Meta_collections(etlrep);
    connectionPool = new ConnectionPool(etlrep);
  }
  
  @Test
  public void testConstructor() throws EngineMetaDataException {
	try {  
		SQLLogResultSet slrs = new SQLLogResultSet(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, clog);
		assertNotNull(slrs);
	
		slrs.execute();
	  }
	  catch(Exception e) {
		 System.out.println("Exception in Constructor: " + e);
	  }
  }
  
  @Test
  public void testSQLLogResultSet() throws EngineMetaDataException {
	try {  
		SQLLogResultSet slrs = new SQLLogResultSet();
		assertNotNull(slrs);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in Constructor: " + e);
	  }
  }
  
}

