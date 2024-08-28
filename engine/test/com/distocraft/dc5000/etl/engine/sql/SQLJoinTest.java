package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.fail;

import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;

import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.JUnit4TestAdapter;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class SQLJoinTest {

	  private static SQLJoin objUnderTest;
	  
	  private final static Logger clog = Logger.getLogger("SQLJoin");
	  
	  private static Statement stmt;
	  
	  static RockFactory mockeddwhreprock;
	  
	  static RockFactory mockeddwhrock;
	  
	  static RockFactory rockFactory;
	  
	  static Mockery context = new JUnit4Mockery();
	  
	  private static Logger mockedLog;
	  
	  static Meta_transfer_actions metaTransferActions;
	  
	  static Properties prop = new Properties();
	  
	  @BeforeClass
	  public static void setUpBeforeClass() throws SQLException, RockException {
		  
	    Meta_versions metaVersions;
	    final Long collectionSetId = 1L;
	    Meta_collections collection;
	    final Long transferActionId = 1L;
	    final Long transferBatchId = 1L;
	    final Long connectId = 1L;
	    
	    final ConnectionPool connectionPool;
	    
	    SetContext setContext;
	    
	    {
	        context.setImposteriser(ClassImposteriser.INSTANCE);
	    }
	    
	    mockedLog = context.mock(Logger.class);
	    context.checking(new Expectations() {

	      {
	        allowing(mockedLog).finest(with(any(String.class)));
	        allowing(mockedLog).finer(with(any(String.class)));
	        allowing(mockedLog).fine(with(any(String.class)));
	        allowing(mockedLog).info(with(any(String.class)));
	        allowing(mockedLog).severe(with(any(String.class)));
	        allowing(mockedLog).log(with(any(Level.class)), with(any(String.class)));
	        allowing(mockedLog).warning(with(any(String.class)));
	        allowing(mockedLog).config(with(any(String.class)));
	      }
	    });
	    
	    mockeddwhreprock = context.mock(RockFactory.class, "EtlRep");
	    mockeddwhrock = context.mock(RockFactory.class, "DwhRep");
	    
	    try {
	      Class.forName("org.hsqldb.jdbcDriver").newInstance();
	    } catch (Exception e) {
	      System.out.println(e);
	      System.exit(1);
	    }
	    Connection con = null;
	    try {
	      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
	    } catch (SQLException sqle) {
	      System.out.println(sqle);
	      System.exit(1);
	    }
	    stmt = con.createStatement();
	    try {
	    
	    stmt.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID BIGINT,COLLECTION_SET_NAME VARCHAR(31), "
	        + "DESCRIPTION VARCHAR(31), VERSION_NUMBER VARCHAR(31), ENABLED_FLAG VARCHAR(31), TYPE VARCHAR(31))");
	    stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'set_name', 'description', '1', 'Y', 'type')");
	    
	    stmt.execute("CREATE TABLE Meta_databases (USERNAME VARCHAR(31), VERSION_NUMBER VARCHAR(31), "
	        + "TYPE_NAME VARCHAR(31), CONNECTION_ID BIGINT, CONNECTION_NAME VARCHAR(31), "
	        + "CONNECTION_STRING VARCHAR(31), PASSWORD VARCHAR(31), DESCRIPTION VARCHAR(31), DRIVER_NAME VARCHAR(31), "
	        + "DB_LINK_NAME VARCHAR(31))");
	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'typenames', 1, 'connectionname', "
		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', 2, 'dwhrep', "
		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'USER', 3, 'dwh', "
		        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
	    
	    stmt.execute("CREATE TABLE Dwhtype ( TECHPACK_NAME VARCHAR(31)  ,TYPENAME VARCHAR(31) ,TABLELEVEL VARCHAR(31), "
	    		+ "STORAGEID VARCHAR(31) ,PARTITIONSIZE BIGINT  ,PARTITIONCOUNT BIGINT  ,STATUS VARCHAR(31) ,TYPE VARCHAR(31), "
	    		+ "OWNER VARCHAR(31) ,VIEWTEMPLATE VARCHAR(31) ,CREATETEMPLATE VARCHAR(31) ,NEXTPARTITIONTIME TIMESTAMP, "
	    		+ "BASETABLENAME VARCHAR(31) ,DATADATECOLUMN VARCHAR(31) ,PUBLICVIEWTEMPLATE VARCHAR(31) ,PARTITIONPLAN VARCHAR(31))");
	    stmt.executeUpdate("INSERT INTO Dwhtype VALUES( 'DC_E_RAN'  ,'DC_E_RAN_RNC'  ,'RAW'  ,'testSTORAGEID3', "
	    		+ "3  ,3  ,'testSTATUS3'  ,'testTYPE3'  ,'testOWNER3'  ,'testVIEWTEMPLATE3'  ,'testCREATETEMPLATE3'  ,'2003-03-03 00:00:00.0', "
	    		+ "'DC_E_RAN_RNC_RAW'  ,'testDATADATECOLUMN3'  ,'testPUBLICVIEWTEMPLATE3'  ,'testPARTITIONPLAN3' )");
	    
	    stmt.execute("CREATE TABLE Meta_transfer_actions ( VERSION_NUMBER VARCHAR(31)  ,TRANSFER_ACTION_ID BIGINT  ,COLLECTION_ID BIGINT, "
	    		+ "COLLECTION_SET_ID BIGINT  ,ACTION_TYPE VARCHAR(31) ,TRANSFER_ACTION_NAME VARCHAR(31) ,ORDER_BY_NO BIGINT  ,DESCRIPTION VARCHAR(31), "
	    		+ "WHERE_CLAUSE_01 VARCHAR(31) ,ACTION_CONTENTS_01 VARCHAR(62) ,ENABLED_FLAG VARCHAR(31) ,CONNECTION_ID BIGINT  ,WHERE_CLAUSE_02 VARCHAR(31), "
	    		+ "WHERE_CLAUSE_03 VARCHAR(31) ,ACTION_CONTENTS_02 VARCHAR(31) ,ACTION_CONTENTS_03 VARCHAR(31))");
	    stmt.executeUpdate("INSERT INTO Meta_transfer_actions VALUES( '1'  ,1  ,1  ,1  ,'UpdateDimSession'  ,'testTRANSFER_ACTION_NAME', "
	    		+ "1  ,'testDESCRIPTION'  ,'typeName=DC_E_RAN_RNC_RAW'  ,'select * from DC_E_RAN_RNC_RAW'  ,'testENABLED_FLAG'  ,1  ,'', "
	    		+ "''  ,''  ,'' )");
	    
	    stmt.execute("CREATE TABLE DC_E_RAN_RNC_RAW ( OSS_ID varchar(31), SESSION_ID varchar(31), BATCH_ID varchar(31), DATE_ID date, YEAR_ID varchar(31), "
	    		+ "DATETIME_ID timestamp, TIMELEVEL varchar(31), ROWSTATUS varchar(31), pmNoDiscardSduDcch numeric, pmNoReceivedSduDcch numeric, pmNoRetransPduDcch numeric, "
	    		+ "pmNoSentPduDcch numeric, pmNoDiscardSduDtch	numeric, pmNoReceivedSduDtch numeric, pmNoRetransPduDtch numeric, pmNoSentPduDtch numeric)");
	    stmt.executeUpdate("INSERT INTO DC_E_RAN_RNC_RAW VALUES('eniqoss1', '1', '1', '2011-07-14', null, '2011-04-08 06:00:00.0', '15MIN', '', 1, 1, 1, 1, 1, 1, 1, 1)");
	    
	    stmt.execute("CREATE TABLE TPACTIVATION ( TECHPACK_NAME varchar(30), STATUS varchar(10), VERSIONID varchar(128), TYPE varchar(10), "
	    		+ "MODIFIED integer)");
	    stmt.executeUpdate("INSERT INTO TPACTIVATION VALUES ('DC_E_RAN', 'ACTIVE', '1', 'PM', 0)");
	    
	    stmt.execute("CREATE TABLE TypeActivation ( TECHPACK_NAME varchar(255) not null, STATUS varchar(255) not null, TYPENAME varchar(255) not null, "
	    		+ "TABLELEVEL varchar(255) not null, STORAGETIME numeric(31) null, TYPE varchar(255) not null, PARTITIONPLAN varchar(255) null)");
	    stmt.executeUpdate("INSERT INTO TypeActivation VALUES ('DC_E_RAN', 'ACTIVE', 'DC_E_RAN_RNC', 'RAW', '1', 'Measurement', 'extralarge_plain')"); 
	    
	    stmt.execute("CREATE TABLE Partitionplan ( PARTITIONPLAN varchar(255) not null, DEFAULTSTORAGETIME numeric(31) not null, "
	    		+ "DEFAULTPARTITIONSIZE numeric(31) not null, MAXSTORAGETIME numeric(31) not null, PARTITIONTYPE int)");
	    stmt.executeUpdate("INSERT INTO Partitionplan VALUES ('extralarge_plain', 14, 48, 30, 123)"); 
	    
	    stmt.execute("CREATE TABLE DWHPartition ( STORAGEID varchar(31), TABLENAME varchar(31), STARTTIME timestamp, ENDTIME timestamp, "
	    		+ "STATUS varchar(31), LOADORDER integer)");
	    stmt.executeUpdate("INSERT INTO DWHPartition VALUES('DC_E_RAN_RNC:RAW', 'DC_E_RAN_RNC_RAW_01', '2011-07-05 00:00:00', "
	    		+ "'2011-08-05 00:00:00', 'ACTIVE', 1 )");
	    stmt.executeUpdate("INSERT INTO DWHPartition VALUES('DC_E_RAN_RNC:RAW', 'DC_E_RAN_RNC_RAW_02', '2011-06-04 00:00:00', "
	    		+ "'2011-07-05 00:00:00', 'ACTIVE', 2 )");
	    }
	    
	    catch(Exception e){
	    	System.out.println("Exception in SetUp:" + e);
	    }
	    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

	    collection = new Meta_collections(rockFactory);

	    connectionPool = new ConnectionPool(rockFactory);

	    metaTransferActions = new Meta_transfer_actions(rockFactory);
	    
	    metaTransferActions.setWhere_clause_01("typeName=DC_E_RAN_RNC_RAW");
	    
	    metaTransferActions.setAction_contents_01("select * from DC_E_RAN_RNC_RAW");
	    
	    metaVersions = new Meta_versions(rockFactory);
	    
	    final Connection mockedConnection = context.mock(Connection.class);
	    
	    try {
			context.checking(new Expectations() {
			      {
			        allowing(mockeddwhreprock).getConnection();
			        will(returnValue(mockedConnection));
			        
			        allowing(mockeddwhrock).getConnection();
			        will(returnValue(mockedConnection));
			      }
			    });
	    }
	    catch (Exception e) {}
			
	    setContext = new SetContext();
	    
	    List l = new ArrayList();
	    l.add("DC_E_RAN_RNC_RAW");
	    setContext.put("tableList", l);
	    
	    String measType1 = "DC_E_RAN_RNC";
	    setContext.put("MeasType", measType1);
	    
	    Map<String, Object> sessionLogEntry1 = new HashMap();
	    setContext.put("sessionLogEntry", sessionLogEntry1);
	    	    
	    prop.setProperty("SessionHandling.storageFile", UpdateDIMSession.class.getName());

	    try {
	      StaticProperties.giveProperties(prop);
	        } catch (final Exception e) {
	      e.printStackTrace();
	    }
	    
	    try {
			SessionHandler.init();
			
		} catch (Exception e1) {
		System.out.println("Exceptions :" + e1);
		}
        
        try {
	      objUnderTest = new SQLJoin(metaVersions, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFactory, connectionPool,
	    		  metaTransferActions, setContext, clog);
	        } 
        catch (Exception e) {
	      e.printStackTrace();
	    }
	  }

	  @AfterClass
	  public static void tearDownAfterClass() throws SQLException {
	    try {
	    stmt.execute("DROP TABLE Meta_databases");
	    stmt.execute("DROP TABLE Meta_collection_sets");
	    stmt.execute("DROP TABLE TPACTIVATION");
	    stmt.execute("DROP TABLE TypeActivation");
	    stmt.execute("DROP TABLE Partitionplan");
	    stmt.execute("DROP TABLE DWHPartition");
	    }
	    catch(Exception e) {
	    	System.out.println(e);
	    }
	    objUnderTest = null;
	  }

	  @Test
	  public void testExecuteWillThrowException() throws Exception {
		    
		    final Statement mockedStatement = context.mock(Statement.class);
		    context.checking(new Expectations() {
			      {
			        allowing(mockedStatement).execute("select min(date_id) min, max(date_id) max from null;");
			        allowing(mockedStatement).close();
			      }
			    });
		  try {
			  
		  objUnderTest.execute();
		    }
	    catch(Exception e){
	    	System.out.println("Exception occured: " + e);
	    }
	 }
	  
/*	  @Test
	  public void testcreateTableList() throws Exception {
		  
		  PhysicalTableCache.initialize(rockFactory, DatabaseTestUtils.getTestDbUrl(), "SA", "", DatabaseTestUtils.getTestDbUrl(), "SA", "");
		
		  try {
			  final Method m = SQLJoin.class.getDeclaredMethod("createTableList");
			  m.setAccessible(true);
			  final Map data = (Map) m.invoke(new SQLJoin());
		    }
	    catch(Exception e){
	    	System.out.println("Exception occured: " + e);
	    }
	 }
*/	 

	  public static junit.framework.Test suite() {
	    return new JUnit4TestAdapter(SQLJoinTest.class);
	  }
}

