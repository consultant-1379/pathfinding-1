package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

import java.sql.SQLException;

import java.sql.Statement;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;

public class SQLCreateAsSelectTest extends UnitDatabaseTestCase {

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
            stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'set_name', 'description', '((1))', 'Y', 'type')");
            
            stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'COLLECTION_SET_ID'  ,'COLLECTION_SET_ID'  ,'COL_TYPE'  ,1  ,'Y'  ,'((1))'  ,1  ,1 )");
            
            stmt.executeUpdate("INSERT INTO Meta_source_tables VALUES( '2000-01-01 00:00:00.0'  ,1  ,1  ,'Y', "
            		+ "1  ,1  ,1  ,'Y'  ,'testAS_SELECT_OPTIONS'  ,'((1))'  ,'testVERSION_NUMBER'  ,1 )");
            
            stmt.executeUpdate("INSERT INTO Meta_tables VALUES( 1  ,'testTABLE_NAME'  ,'((1))'  ,'Y'  ,'testJOIN_CLAUSE', "
            		+ "'testTABLES_AND_ALIASES'  ,1 )");
    }
    catch(SQLException sqlE) {
    	System.out.println("SQLException :" + sqlE);
    }
    
    metaTransferActions = new Meta_transfer_actions(etlrep);
    metaTransferActions.setAction_contents_01("DC_E_RAN_RNC_RAW_01");
    metaVersions = new Meta_versions(etlrep);
    collection = new Meta_collections(etlrep);
    objMetaCol = new Meta_columns(etlrep ,  1L ,  "((1))",  1L ,  1L );
    
    connectionPool = new ConnectionPool(etlrep);
  }
  
  @Test
  public void testConstructor() throws EngineMetaDataException {
	  
	  SQLCreateAsSelect scs = new SQLCreateAsSelect(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, batchColumnName);
	  String actual = scs.getcreateAsSelectClause();
	  String expected = "CREATE TABLE DC_E_RAN_RNC_RAW_01 testAS_SELECT_OPTIONS TABLESPACE ((1)) AS SELECT SELECT DISTINCT  FROM "
		  + "(SELECT COLLECTION_SET_ID FROM testTABLES_AND_ALIASES WHERE testJOIN_CLAUSE) SRC WHERE COLLECTION_SET_ID > " 
		  + "TO_DATE('2000-01-01 00:00:00.0','yyyy-mm-dd hh24:mi:ss')";
	  assertEquals(expected, actual);

  }
}
