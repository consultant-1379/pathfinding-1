package com.distocraft.dc5000.etl.engine.sql;

import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
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

public class UpdateMonitoredTypesTest {

	  private static UpdateMonitoredTypes objUnderTest;
	  
	  private final static Logger clog = Logger.getLogger("UpdateMonitoredTypes");

	  private static Statement stmt;
	  
	  static RockFactory mockeddwhreprock;
	  
	  static RockFactory mockeddwhrock;
	  
	  static Mockery context = new JUnit4Mockery();
	  
	  @BeforeClass
	  public static void setUpBeforeClass() throws SQLException, RockException {
		  
	    Meta_versions metaVersions;
	    final Long collectionSetId = 1L;
	    Meta_collections collection;
	    final Long transferActionId = 1L;
	    final Long transferBatchId = 1L;
	    final Long connectId = 1L;
	    final RockFactory rockFactory;
	    final ConnectionPool connectionPool;
	    Meta_transfer_actions metaTransferActions;
	
	    {
	        context.setImposteriser(ClassImposteriser.INSTANCE);
	    }
	    
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
	    
	    stmt.execute("CREATE TABLE Log_Session_Loader ( LOADERSET_ID tinyint, SESSION_ID tinyint, BATCH_ID smallint, DATE_ID date, "
	    	+ "ROWSTATUS varchar(31), TIMELEVEL varchar(31), DATADATE date, DATATIME timestamp, ROWCOUNT tinyint, SESSIONENDTIME timestamp, "
	    	+ "SESSIONSTARTTIME timestamp, SOURCE varchar(31), STATUS varchar(31), TYPENAME varchar(31), FLAG tinyint)");
	    
	    final GregorianCalendar now = new GregorianCalendar();
	    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	    String nowDateString = sdf.format(now.getTime());
	    final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	    String LongDateString = sdf1.format(now.getTime());
	    	    
	    String SqlClause = "INSERT INTO Log_Session_Loader VALUES (1, 2, 3, '" +nowDateString+ "', null, '15MIN','" +nowDateString+ "','" +LongDateString+ "', 5, '" +LongDateString+ "','" +LongDateString+ "', 'BSS1',  'OK', 'DC_E_BSS_TRC', 1)" ;
	    stmt.executeUpdate(SqlClause);
	    
	    stmt.execute("CREATE TABLE LOG_MonitoredTypes ( TYPENAME VARCHAR(31), TIMELEVEL VARCHAR(31), STATUS VARCHAR(31), MODIFIED timestamp, "
	    	+ "ACTIVATIONDAY timestamp, TECHPACK_NAME VARCHAR(31))");
	    stmt.executeUpdate("INSERT INTO LOG_MonitoredTypes VALUES('DC_E_BSS_TRC_RAW','15MIN','ACTIVE','2011-04-08 06:00:00.0','2011-04-08 06:00:00.0','DC_E_BSS')");

	    stmt.execute("CREATE TABLE DataFormat ( DATAFORMATID VARCHAR(62), TYPEID VARCHAR(31), VERSIONID VARCHAR(31), OBJECTTYPE VARCHAR(31), "
		    	+ "FOLDERNAME VARCHAR(31), DATAFORMATTYPE VARCHAR(31))");
	    stmt.executeUpdate("INSERT INTO DataFormat VALUES('DC_E_BSS:((41)):DC_E_BSS_TRC:eniqasn1','DC_E_BSS:((1)):DC_E_BSS_TRC','DC_E_BSS', "
	    		+ "'Measurement','DC_E_BSS_TRC','eniqasn1')");
	    
	    stmt.execute("CREATE TABLE TPActivation ( TECHPACK_NAME varchar(30), STATUS varchar(10), VERSIONID varchar(128), TYPE varchar(10), "
	    		+ "MODIFIED integer)");
	    stmt.executeUpdate("INSERT INTO TPACTIVATION (TECHPACK_NAME, STATUS, VERSIONID, TYPE, MODIFIED) values ('DC_E_BSS', 'active', 'DC_E_BSS', 'PM', 0)");
	    
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
	    
	    }
	    catch(Exception e){
	    	System.out.println("Exception in SetUp:" + e);
	    }
	    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

	    collection = new Meta_collections(rockFactory);

	    connectionPool = new ConnectionPool(rockFactory);

	    metaTransferActions = new Meta_transfer_actions(rockFactory);
	    
	    metaTransferActions.setAction_contents("INSERT INTO Example_table VALUES ('test')");

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
			
	    try {
	      objUnderTest = new UpdateMonitoredTypes(metaVersions, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFactory, connectionPool,
	    		  metaTransferActions, clog);
	    
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	  }

	  @AfterClass
	  public static void tearDownAfterClass() throws SQLException {
	    try {
	    stmt.execute("DROP TABLE Meta_databases");
	    stmt.execute("DROP TABLE Meta_collection_sets");
	    stmt.execute("DROP TABLE LOG_SESSION_LOADER");
	    stmt.execute("DROP TABLE LOG_MonitoredTypes");
	    stmt.execute("DROP TABLE TPACTIVATION");
	    stmt.execute("DROP TABLE Dataformat");
	    }
	    catch(Exception e) {}
	    objUnderTest = null;
	  }

	  @Test
	  public void testExecuteWillThrowException() throws Exception {
		  
		  try {
		  objUnderTest.execute();
		    }
	    catch(Exception e){
	    	System.out.println("Exception occured: " + e);
	    }
	 }

	  public static junit.framework.Test suite() {
	    return new JUnit4TestAdapter(UpdateMonitoredTypesTest.class);
	  }
}
