package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
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
import com.distocraft.dc5000.repository.cache.ActivationCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

/**
 * @author ejamves
 *
 */
public class DWHMPartitionActionTest  extends UnitDatabaseTestCase {
	
	  private final Logger log = Logger.getLogger("DWHMPartitionActionTest");
	  
	  private static DWHMPartitionAction objUnderTest=null;
 
	  private static ConnectionPool connectionPool;
	  
	  private static Meta_versions metaVersions=null;
	  private static Meta_transfer_actions metaTransferActions=null;
	  private static Meta_collections collection=null;
	  private static Long connectId = 1L;
	  
	  private static Calendar minDateForTableRaw00;
	  private static Calendar maxDateForTableRaw00;
	  private static Calendar minDateForTableRaw01;
	  private static Calendar maxDateForTableRaw01;
	  private static Calendar minDateForTableRaw02;
	  private static Calendar maxDateForTableRaw02;

	  
      Long collectionSetId = 1L;
      Long transferActionId = 1L;
      Long transferBatchId = 1L;
	  long maxStorage = 90;
    Long techPackId=1l;
	  
	  static RockFactory etlrep ;
	  static RockFactory dwhrep ;
	  static RockFactory sa ;
	  
	  @BeforeClass
	  public static void setUp() throws Exception{
		  
			setup(TestType.unit);
			
		    loadDefaultTechpack(TechPack.stats, "v1");
		    loadDefaultTechpack(TechPack.stats, "v2");
		    
		    etlrep = getRockFactory(Schema.etlrep);
		    dwhrep = getRockFactory(Schema.dwhrep);
		    sa = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver",
					"SA", true);

		    Statement stmt = etlrep.getConnection().createStatement();
		    Statement stmt1 = dwhrep.getConnection().createStatement();
		    Statement stmt2 = sa.getConnection().createStatement();

		    stmt2.execute("CREATE SCHEMA SYS AUTHORIZATION SA");
		    
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
		          	stmt.executeUpdate("INSERT INTO Meta_databases VALUES('dwhrep', '1', '', 2, 'dwhrep', "
		          	    		        + "'jdbc:hsqldb:mem:repdb', 'dwhrep', 'description', 'org.hsqldb.jdbcDriver', '')");
		      	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', '', 3, 'dwh', "
		      		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");

		      	    
		      	    minDateForTableRaw00 = getCalendar(2010, 1, 1, 0, 0);
		      	    maxDateForTableRaw00 = getCalendar(2010, 2, 1, 0, 0);
		      	    minDateForTableRaw01 = getCalendar(2010, 3, 1, 0, 0);
		      	    maxDateForTableRaw01 = getCalendar(2010, 4, 1, 0, 0);
		      	    minDateForTableRaw02 = getCalendar(2010, 4, 1, 0, 0);
		      	    maxDateForTableRaw02 = getCalendar(2010, 5, 1, 0, 0);

		      	    stmt1.executeUpdate("INSERT INTO DWHTECHPACKS (TECHPACK_NAME, VERSIONID, CREATIONDATE) values ('DC_E_MGW', '((1))', '2011-04-05 22:41:55.0')");
		      	    stmt1.executeUpdate("INSERT INTO DWHType (TECHPACK_NAME,TYPENAME,TABLELEVEL,STORAGEID,PARTITIONSIZE,PARTITIONCOUNT,STATUS,TYPE," +
		      	    		"OWNER,VIEWTEMPLATE,CREATETEMPLATE,BASETABLENAME,DATADATECOLUMN,PUBLICVIEWTEMPLATE,PARTITIONPLAN) " +
		      	    		"VALUES('DC_E_MGW', 'typename', 'tablelevel', 'storageid', 1, 1, 'status', 'partitioned', 'owner', 'viewtemplate', " +
		      	    		"'createtemplate', 'Example_table', 'datadatecolumn', 'publicviewtemplate', 'partitionplan')");

		      	    stmt1.executeUpdate("insert into DWHPartition VALUES('table:RAW', 'table_RAW_00', '"
			      	        + new Timestamp(minDateForTableRaw00.getTimeInMillis()) + "', '"
			      	        + new Timestamp(maxDateForTableRaw00.getTimeInMillis()) + "', 'ACTIVE', 3)");
		      	  	stmt1.executeUpdate("insert into DWHPartition VALUES('table:RAW', 'table_RAW_01', '"
			      	        + new Timestamp(minDateForTableRaw01.getTimeInMillis()) + "', '"
			      	        + new Timestamp(maxDateForTableRaw01.getTimeInMillis()) + "', 'ACTIVE', 2)");
		      	  	stmt1.executeUpdate("insert into DWHPartition VALUES('table:RAW', 'table_RAW_02', '"
			      	        + new Timestamp(minDateForTableRaw02.getTimeInMillis()) + "', '"
			      	        + new Timestamp(maxDateForTableRaw02.getTimeInMillis()) + "', 'ACTIVE', 1)");
			      	    
		      	    stmt2.execute("CREATE TABLE sys.systable (table_name varchar(32), creator varchar(12), table_type varchar(12))");
		      	    stmt2.execute("CREATE TABLE sys.sysuser (user_id varchar(12), user_name varchar(12))");
		      	    stmt2.execute("CREATE TABLE table_RAW_TIMERANGE (MIN_DATE timestamp, MAX_DATE timestamp, TABLENAME varchar(12))");

		      	    stmt2.executeUpdate("insert into sys.systable VALUES ('table_RAW_TIMERANGE', '1', 'VIEW')");
		      	    stmt2.executeUpdate("insert into sys.sysuser VALUES ('1', 'dc')");

		      	    insertValuesIntoTimeRangeTable(stmt2, new Timestamp(minDateForTableRaw00.getTimeInMillis()), new Timestamp(
		      	        maxDateForTableRaw00.getTimeInMillis()), new Timestamp(minDateForTableRaw01.getTimeInMillis()), new Timestamp(
		      	        maxDateForTableRaw01.getTimeInMillis()), new Timestamp(minDateForTableRaw02.getTimeInMillis()), new Timestamp(
		      	        maxDateForTableRaw02.getTimeInMillis()));

		      }
		    catch(SQLException sqlE) {
		    	System.out.println("SQLException :" + sqlE);
		    }
		    
		    metaTransferActions = new Meta_transfer_actions(etlrep);
		    metaTransferActions.setAction_contents_01("DC_E_RAN_RNC_RAW_01");
		    metaVersions = new Meta_versions(etlrep);
		    collection = new Meta_collections(etlrep);
		    connectionPool = new ConnectionPool(etlrep);

		  }
		  
	  @Test
	  public void testConstructor() throws EngineMetaDataException {
		try {  

			objUnderTest = new DWHMPartitionAction(metaVersions, techPackId, collection, transferActionId, transferBatchId, connectId, etlrep, connectionPool, metaTransferActions, log);
	 		assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in Constructor: " + e);
		  }
	  }
	  
	  @Test
	  public void testExecute() throws EngineMetaDataException {
		try {  
	        PhysicalTableCache.initialize(etlrep);
	        ActivationCache.initialize(etlrep);
      objUnderTest = new DWHMPartitionAction(metaVersions, techPackId, collection, transferActionId, transferBatchId, connectId, etlrep, connectionPool, metaTransferActions, log);
			objUnderTest.execute();
			assertNotNull(objUnderTest);
		  }
		  catch(Exception e) {
			 System.out.println("Exception in Constructor: " + e);
		        e.printStackTrace();
		        assertEquals("",e.getMessage());
		  }
	  }

	  protected static Calendar getCalendar(final int year, final int month, final int day, final int hour, final int min) {
		    final Calendar calendar = new GregorianCalendar();
		    calendar.set(Calendar.YEAR, year);
		    calendar.set(Calendar.MONTH, month);
		    calendar.set(Calendar.DAY_OF_MONTH, day);
		    calendar.set(Calendar.HOUR_OF_DAY, hour);
		    calendar.set(Calendar.MINUTE, min);
		    calendar.set(Calendar.SECOND, 0);
		    calendar.set(Calendar.MILLISECOND, 0);
		    return calendar;
		  }
	  
	  private static void insertValuesIntoTimeRangeTable(final Statement stmt, final Timestamp minTimeStamp00,
              final Timestamp maxTimeStamp00, final Timestamp minTimeStamp01, final Timestamp maxTimeStamp01,
              final Timestamp minTimeStamp02, final Timestamp maxTimeStamp02) throws SQLException {
				stmt.executeUpdate("insert into table_RAW_TIMERANGE VALUES('" + minTimeStamp00 + "', '" + maxTimeStamp00
				+ "', 'table_RAW_00')");
				stmt.executeUpdate("insert into table_RAW_TIMERANGE VALUES('" + minTimeStamp01 + "', '" + maxTimeStamp01
				+ "', 'table_RAW_01')");
				stmt.executeUpdate("insert into table_RAW_TIMERANGE VALUES('" + minTimeStamp02 + "', '" + maxTimeStamp02
				+ "', 'table_RAW_02')");
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
