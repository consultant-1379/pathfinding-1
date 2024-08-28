package com.distocraft.dc5000.etl.engine.sql;

import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;

import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class TableCleanerTest extends UnitDatabaseTestCase {
	
	private final static Logger clog = Logger.getLogger("TableCleaner");

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
            
            stmt.execute("CREATE TABLE DC_E_RAN_RNC_RAW ( OSS_ID varchar(31), SESSION_ID varchar(31), BATCH_ID varchar(31), DATE_ID date, YEAR_ID varchar(31), "
    	    		+ "DATETIME_ID timestamp, TIMELEVEL varchar(31), ROWSTATUS varchar(31), pmNoDiscardSduDcch numeric, pmNoReceivedSduDcch numeric, pmNoRetransPduDcch numeric, "
    	    		+ "pmNoSentPduDcch numeric, pmNoDiscardSduDtch	numeric, pmNoReceivedSduDtch numeric, pmNoRetransPduDtch numeric, pmNoSentPduDtch numeric)");
    	    stmt.executeUpdate("INSERT INTO DC_E_RAN_RNC_RAW VALUES('eniqoss1', '1', '1', '2011-07-14', null, '2011-04-08 06:00:00.0', '15MIN', '', 1, 1, 1, 1, 1, 1, 1, 1)");
    }
    catch(SQLException sqlE) {
    	System.out.println("SQLException :" + sqlE);
    }

    final Properties aprops = new Properties();
	aprops.put("tablename", "DC_E_RAN_RNC_RAW");
	aprops.put("datecolumn", "DATE_ID");
	aprops.put("threshold", "0");
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	aprops.store(baos, "");
	final String getAction_contents = baos.toString();
    metaTransferActions = new Meta_transfer_actions(etlrep);
    metaTransferActions.setAction_contents(getAction_contents);

    metaVersions = new Meta_versions(etlrep);
    collection = new Meta_collections(etlrep);
    objMetaCol = new Meta_columns(etlrep ,  1L ,  "((1))",  1L ,  1L );
    
    connectionPool = new ConnectionPool(etlrep);
  }
  
  @Test
  public void testConstructor() throws Exception {
	  try {
		TableCleaner tc = new TableCleaner(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, clog);
	  } catch (RockException e) {
			e.printStackTrace();
	  }
  }
  
  @Test
  public void testExecute() throws Exception {
	  TableCleaner tc = new TableCleaner(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, connectionPool, metaTransferActions, clog);
	  try {
		tc.execute();
	} catch (Exception e) {}
  }
}

