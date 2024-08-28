package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import java.sql.Statement;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;

public class SQLColConstraintTest extends UnitDatabaseTestCase {

	static Meta_transfer_actions metaTransferActions;
	static Meta_versions metaVersions;
	static Meta_collections collection;
	static Meta_columns objMetaCol;
	
    Long collectionSetId = 1L;
    Long transferActionId = 1L;
    Long transferBatchId = 1L;
    Long connectId = 1L;
    
    ConnectionPool connectionPool;
    
    static RockFactory etlrep ;
    static RockFactory dwhrep ;
    
    static List list;
	
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
            
            stmt.executeUpdate("INSERT INTO META_COLUMN_CONSTRAINTS VALUES( 1  ,1  ,'','((1))'  ,1  ,1  , 1 )");
    }
    catch(SQLException sqlE) {
    	System.out.println("SQLException :" + sqlE);
    }
    
    metaTransferActions = new Meta_transfer_actions(etlrep);
    metaVersions = new Meta_versions(etlrep);
    collection = new Meta_collections(etlrep);
    objMetaCol = new Meta_columns(etlrep ,  1L ,  "((1))",  1L ,  1L );
    
    list = new ArrayList();
  }
  
  @Test
  public void testConstructor() throws EngineMetaDataException {
	
	  list.add(objMetaCol);
	  SQLColConstraint scc = new SQLColConstraint(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, etlrep, metaTransferActions, 1L, "Meta_collection_sets", list);
	 
	try {
		int actual = scc.executeColConstCheck();
		assertEquals(1, actual);
	} catch (EngineException e) {
		e.printStackTrace();
	}
  }
}
