/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jmock.Expectations;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlot;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfile;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfileHandler;
import com.distocraft.dc5000.etl.engine.priorityqueue.PersistenceHandler;
import com.distocraft.dc5000.etl.engine.priorityqueue.PriorityQueue;
import com.distocraft.dc5000.etl.engine.system.SetStatusTO;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_tables;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.ActivationCache;
import com.distocraft.dc5000.repository.cache.AggregationRuleCache;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.distocraft.dc5000.repository.cache.DBLookupCache;
import com.distocraft.dc5000.repository.cache.DataFormatCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.testutilities.DirectoryHelper;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase.Schema;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase.TechPack;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase.TestType;
import com.ericsson.eniq.licensing.cache.LicenseDescriptor;
import com.ericsson.eniq.licensing.cache.LicenseInformation;
import com.ericsson.eniq.licensing.cache.LicensingCache;
import com.ericsson.eniq.licensing.cache.LicensingResponse;
import com.ericsson.eniq.repository.AsciiCrypter;

/**
 * @author eemecoy
 *
 */
public class TransferEngineTest extends BaseMock {

  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "TransferEngineTest");

  private TransferEngine objToTest;

  static Logger log = Logger.getLogger("TransferEngineTest");

  static ExecutionSlotProfileHandler esph;
  static ExecutionSlotProfile esp;
  static RockFactory etlrep;
  static RockFactory dwhrep;
  static RockFactory etlrepRock;
  static RockFactory dcRock;
  static RockFactory saRock;

  static Meta_transfer_actions metaTransferActions;
  static Meta_versions metaVersions;
  static Meta_collections collection;
  static Meta_tables metaTables;
  static ConnectionPool  connectionPool;

  Iterator mockedIterator;

  EngineThread mockedEngineThread;

  PriorityQueue mockedPriorityQueue;

  ExecutionSlotProfileHandler mockedExecutionSlotProfileHandler;

  ExecutionSlotProfile mockedExecutionSlotProfile;

  RockFactory mockedEtlRepRockFactory;

  RockFactory mockedDwhRepRockFactory;

  Meta_collection_setsFactory mockedMetaCollectionSetsFactory;

  Meta_collectionsFactory mockedMetaCollectionsFactory;

  static String expectedSqlQuery = " ORDER BY COLLECTION_DC_E_RAN_RNC_RAW_01, ENABLED_FLAG DESC;";

  static String expectedCollectionQuery = " ORDER BY COLLECTION_NAME DESC;";

  LicensingResponse mockedLicensingResponse;

  Logger mockedLog;

  AggregationStatusCache mockedAggStatusCache;

  TransferEngine mockedTransferEngine;

  ScheduledThreadPoolExecutor mockedExecutor;

  static PriorityQueue priorityQueue;
  static PersistenceHandler ph;

  static String dwhRep_url;
  static String dwhRep_driver;
  static String dwhRep_password;
  static String dwhRep_username;

  private static File staticProperties;
  static final String DC5000_CONFIG_DIR = "dc5000.config.directory";

  @Before
  public void setUp() throws RemoteException {

    StaticProperties.giveProperties(new Properties());

    try {
		setUpPropertiesFileAndProperty();
	} catch (IOException e1) {
		e1.printStackTrace();
	}
    try {
        Class.forName("org.hsqldb.jdbcDriver");
      } catch (ClassNotFoundException e2) {
        e2.printStackTrace();
        fail("execute() failed, ClassNotFoundException");
      }

    /**try {
    	setupEtlcProperties();
      } catch (Exception e) {
        e.printStackTrace();
        fail("setupEtlcProperties set up failed");
      }
	**/
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

    mockedIterator = context.mock(Iterator.class);
    mockedEngineThread = context.mock(EngineThread.class);
    mockedPriorityQueue = context.mock(PriorityQueue.class);
    mockedExecutionSlotProfileHandler = context.mock(ExecutionSlotProfileHandler.class);
    mockedEtlRepRockFactory = context.mock(RockFactory.class, "EtlRep");
    mockedDwhRepRockFactory = context.mock(RockFactory.class, "DwhRep");
    mockedMetaCollectionSetsFactory = context.mock(Meta_collection_setsFactory.class);
    mockedMetaCollectionsFactory = context.mock(Meta_collectionsFactory.class);
    mockedLicensingResponse = context.mock(LicensingResponse.class);
    mockedExecutionSlotProfile = context.mock(ExecutionSlotProfile.class);
    mockedTransferEngine = context.mock(TransferEngine.class);
    mockedExecutor = context.mock(ScheduledThreadPoolExecutor.class);

    objToTest= new StubbedTransferEngine(log);
	objToTest = new StubbedTransferEngine(true, true, 1, log);

  }
  
  @BeforeClass
  public static void setUpDB() throws SQLException, RockException, RemoteException{
    
    
    if(!TMP_DIR.exists() && !TMP_DIR.mkdirs()){
      fail("Failed to create " + TMP_DIR.getPath());
    }

    /* Set this Property in Jenkins and not in the test case itself.*/
  System.setProperty("integration_host", "localhost");
	UnitDatabaseTestCase.setup(TestType.unit);

	UnitDatabaseTestCase.loadDefaultTechpack(TechPack.stats, "v1");
	UnitDatabaseTestCase.loadDefaultTechpack(TechPack.stats, "v2");

    etlrep = UnitDatabaseTestCase.getRockFactory(Schema.etlrep);
    dwhrep = UnitDatabaseTestCase.getRockFactory(Schema.dwhrep);
    dcRock = UnitDatabaseTestCase.getRockFactory(Schema.dc);

//    etlrepRock = new RockFactory(ETLREP_URL, "etlrep", "etlrep", TESTDB_DRIVER, "", true);
    //dcRock = new RockFactory(ETLREP_URL, "dc", "dc", TESTDB_DRIVER, "", true);
//    dcRock = new RockFactory(DCSYBBASE_URL, "dwhrep", "dwhrep", DCSYBBASE_DRIVER, "", true);
//    saRock = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "SA", true);

    final Statement stmt = etlrep.getConnection().createStatement();
    final Statement stmt1 = dwhrep.getConnection().createStatement();
    final Statement stmt2 = dcRock.getConnection().createStatement();

    esph = new ExecutionSlotProfileHandler(etlrep.getDbURL(), "etlrep", "etlrep", UnitDatabaseTestCase.getSybaseDriver());

	ph = new PersistenceHandler(etlrep.getDbURL(), etlrep.getUserName(),
			etlrep.getPassword(), etlrep.getDriverName());



    dwhRep_url = dwhrep.getDbURL();
    dwhRep_driver = dwhrep.getDriverName();
    dwhRep_password = dwhrep.getPassword();
    dwhRep_username = dwhrep.getUserName();

    try {

            stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'DC_E_RAN_RNC_RAW_01', 'description', '1', 'Y', 'Techpack')");
            stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(2, 'DWH_MONITOR', 'description', '((1))', 'Y', 'interface')");
            stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(3, 'DC-RAN', 'description', '((2))', 'y', 'interface')");

            stmt.executeUpdate("INSERT INTO Meta_transfer_batches VALUES( 1  ,'2000-01-01 00:00:00.0'  ,'2000-01-01 00:00:00.0'  ,'Y', "
            		+ "'Executed'  ,'((1))'  ,2  ,1  ,'testMETA_COLLECTION_NAME'  ,'testMETA_COLLECTION_SET_NAME'  ,'Loader', "
            		+ "1  ,'testSCHEDULING_INFO', null )");

            stmt.executeUpdate("INSERT INTO Meta_errors VALUES( 1  ,'testTEXT'  ,'testMETHOD_NAME'  ,'testERR_TYPE', "
            		+ "'2000-01-01 00:00:00.0'  ,'((1))'  ,1  ,1  ,1  ,1 )");

            stmt.executeUpdate("INSERT INTO Meta_collections VALUES( 1  ,'AggregationRuleCopy'  ,'testCOLLECTION1', "
               		+ "'testMAIL_ERROR_ADDR1'  ,'testMAIL_FAIL_ADDR1'  ,'testMAIL_BUG_ADDR1'  ,1  ,1  ,1  ,'Y', "
            		+ "'N'  ,'2001-01-01 00:00:00.0'  ,'((1))'  ,2  ,'Y', "
            		+ "1  ,1  ,'Y'  ,'PM'  ,''  ,'testMEASTYPE1'  ,'N'  ,'testSCHEDULING_INFO1' )");

            stmt.executeUpdate("INSERT INTO Meta_collections VALUES( 1  ,'AggregationRuleCopy'  ,'testCOLLECTION1', "
               		+ "'testMAIL_ERROR_ADDR1'  ,'testMAIL_FAIL_ADDR1'  ,'testMAIL_BUG_ADDR1'  ,1  ,1  ,2  ,'Y', "
            		+ "'N'  ,'2001-01-01 00:00:00.0'  ,'1'  ,1  ,'Y', "
            		+ "1  ,1  ,'Y'  ,'PM'  ,''  ,'testMEASTYPE1'  ,'N'  ,'testSCHEDULING_INFO1' )");

            stmt.executeUpdate("INSERT INTO Meta_collections VALUES( 2  ,'DC_E_RAN_RNC_RAW_01'  ,'testCOLLECTION1', "
               		+ "'testMAIL_ERROR_ADDR1'  ,'testMAIL_FAIL_ADDR1'  ,'testMAIL_BUG_ADDR1'  ,1  ,2  ,1  ,'Y', "
            		+ "'N'  ,'2001-01-01 00:00:00.0'  ,'((1))'  ,1  ,'Y', "
            		+ "1  ,1  ,'Y'  ,'Techpack'  ,''  ,'testMEASTYPE1'  ,'N'  ,'testSCHEDULING_INFO1' )");

            stmt.executeUpdate("INSERT INTO Meta_collections VALUES( 4  ,'Restore_Count_EVENT_E_SGEH_RAW'  ,'testCOLLECTION1', "
               		+ "'testMAIL_ERROR_ADDR1'  ,'testMAIL_FAIL_ADDR1'  ,'testMAIL_BUG_ADDR1'  ,1  ,1  ,1  ,'Y', "
            		+ "'N'  ,'2001-01-01 00:00:00.0'  ,'1'  ,1  ,'Y', "
            		+ "1  ,1  ,'Y'  ,'PM'  ,''  ,'testMEASTYPE1'  ,'N'  ,'testSCHEDULING_INFO1' )");

            stmt.executeUpdate("INSERT INTO Meta_collections VALUES( 5  ,'CountingReAgg_DC_E_RAN_RNC_RAW_01'  ,'testCOLLECTION1', "
               		+ "'testMAIL_ERROR_ADDR1'  ,'testMAIL_FAIL_ADDR1'  ,'testMAIL_BUG_ADDR1'  ,1  ,1  ,1  ,'Y', "
            		+ "'N'  ,'2001-01-01 00:00:00.0'  ,'1'  ,1  ,'Y', "
            		+ "1  ,1  ,'Y'  ,'PM'  ,''  ,'testMEASTYPE1'  ,'N'  ,'testSCHEDULING_INFO1' )");

            stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'COLLECTION_SET_ID'  ,'COLLECTION_SET_ID'  ,'COL_TYPE'  ,1  ,'Y'  ,'((1))'  ,1  ,1 )");

            stmt.executeUpdate("INSERT INTO Meta_source_tables VALUES( '2000-01-01 00:00:00.0'  ,1  ,1  ,'Y', "
            		+ "1  ,1  ,1  ,'Y'  ,'testAS_SELECT_OPTIONS'  ,'((1))'  ,'((1))'  ,1 )");

            stmt.executeUpdate("INSERT INTO Meta_schedulings VALUES( '((1))'  ,1  ,'onStartup'  ,'testOS_COMMAND1'  ,1  ,1  ,1  ,1  ,1  ,1, "
            		+ "'Y'  ,'Y'  ,'Y'  ,'Y'  ,'Y'  ,'Y'  ,'Y', "
            		+ "'testSTATUS1'  ,'2001-01-01 00:00:00.0'  ,1  ,1  ,'testNAME1'  ,'Y'  ,1  ,1  ,'testTRIGGER_COMMAND1'  ,1 )");

            stmt.executeUpdate("INSERT INTO Meta_schedulings VALUES( '((2))'  ,2  ,'onStartup'  ,'testOS_COMMAND1'  ,1  ,1  ,1  ,1  ,3  ,2, "
           		+ "'Y'  ,'Y'  ,'Y'  ,'Y'  ,'Y'  ,'Y'  ,'Y', "
           		+ "'testSTATUS1'  ,'2001-01-01 00:00:00.0'  ,1  ,1  ,'TriggerAdapterINTF'  ,'N'  ,1  ,1  ,'testTRIGGER_COMMAND1'  ,1 )");

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
      		        + "'jdbc:hsqldb:mem:repdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");

    	    stmt1.executeUpdate("INSERT INTO TPACTIVATION VALUES ('DC_E_RAN_RNC_RAW_01', 'ACTIVE', '1:1', 'Techpack', 0)");

    	    stmt.executeUpdate("INSERT INTO Meta_transfer_actions VALUES( '((1))'  ,1  ,1  ,1  ,'testACTION_TYPE', "
    	    		+ "'testTRANSFER_ACTION_NAME'  ,1  ,'testDESCRIPTION'  ,'testWHERE_CLAUSE_01'  ,'testACTION_CONTENTS_01', "
    	    		+ "'N'  ,1  ,'testWHERE_CLAUSE_02'  ,'testWHERE_CLAUSE_03'  ,'testACTION_CONTENTS_02'  ,'testACTION_CONTENTS_03' )");

    	    stmt.executeUpdate("INSERT INTO Meta_transfer_actions VALUES( '((1))'  ,1  ,4  ,1  ,'testACTION_TYPE', "
    	    		+ "'testTRANSFER_ACTION_NAME'  ,1  ,'testDESCRIPTION'  ,'testWHERE_CLAUSE_01'  ,'testACTION_CONTENTS_01', "
    	    		+ "'N'  ,1  ,'testWHERE_CLAUSE_02'  ,'testWHERE_CLAUSE_03'  ,'testACTION_CONTENTS_02'  ,'testACTION_CONTENTS_03' )");

    	    stmt.executeUpdate("INSERT INTO Meta_transfer_actions VALUES( '((1))'  ,1  ,5  ,1  ,'testACTION_TYPE', "
    	    		+ "'testTRANSFER_ACTION_NAME'  ,1  ,'testDESCRIPTION'  ,'testWHERE_CLAUSE_01'  ,'testACTION_CONTENTS_01', "
    	    		+ "'N'  ,1  ,'testWHERE_CLAUSE_02'  ,'testWHERE_CLAUSE_03'  ,'testACTION_CONTENTS_02'  ,'testACTION_CONTENTS_03' )");

    	    stmt.execute("create table LOG_AGGREGATIONSTATUS(AGGREGATION varchar(255),"+
    	              "TYPENAME varchar(255)," +
    	              "TIMELEVEL varchar(10),"+
    	              "DATADATE date,"+
    	              "INITIAL_AGGREGATION timestamp,"+
    	            "STATUS varchar(16),"+
    	            "DESCRIPTION varchar(250),"+
    	            "ROWCOUNT integer,"+
    	            "AGGREGATIONSCOPE varchar(50),"+
    	            "LAST_AGGREGATION timestamp,"+
    	            "LOOPCOUNT integer,"+
    	            "THRESHOLD timestamp)");

    	      stmt.executeUpdate("INSERT INTO LOG_AGGREGATIONSTATUS VALUES('aggregation', 'typename', 'DAYBH', '1970-01-01', null, " +
    	      "'BLOCKED', null, 0, 'DAY', null, 0, null)");

    	      stmt1.executeUpdate("INSERT INTO CONFIGURATION VALUES ('etlc.MemoryUsageFactor.EngineMemoryNeedMB','512')");
    	      stmt1.executeUpdate("INSERT INTO CONFIGURATION VALUES ('etlc.workerLimitationRegexp.','1')");
    	      stmt1.executeUpdate("INSERT INTO CONFIGURATION VALUES ('etlc.EngineMemoryNeedMB','512 + 5%')");

	      	  stmt1.executeUpdate("INSERT INTO DWHType (TECHPACK_NAME,TYPENAME,TABLELEVEL,STORAGEID,PARTITIONSIZE,PARTITIONCOUNT,STATUS,TYPE," +
	      	    		"OWNER,VIEWTEMPLATE,CREATETEMPLATE,BASETABLENAME,DATADATECOLUMN,PUBLICVIEWTEMPLATE,PARTITIONPLAN) " +
	      	    		"VALUES('DC_E_RAN_RNC_RAW_01', 'EVENT_E_SGEH:RAW', 'DAY', 'storageid', 1, 1, 'ACTIVE', 'type', 'owner', 'viewtemplate', " +
	      	    		"'createtemplate', 'Example_table', 'datadatecolumn', 'publicviewtemplate', 'partitionplan')");
	      	  
	      	  // Add row to ENIQ_EVENTS_ADMIN_PROPERTIES:
	      	  stmt1.executeUpdate("insert into ENIQ_EVENTS_ADMIN_PROPERTIES (PARAM_NAME, PARAM_VALUE, DATE_MODIFIED, MODIFIED_BY) values (" +
	      	  		"'ENIQ_EVENTS_LOCK_USERS', 'true', '2013-07-10 09:13:27', 'dcuser')");

	      	  //stmt2.execute("create table sys.syscollation(collation_name varchar(30))");
	    	  //stmt2.executeUpdate("INSERT INTO sys.syscollation VALUES ('ISO8859-1 ISO Latin 1 Western')");

	    	 /** ResultSet rst = stmt2.executeQuery("SELECT SUBSTRING(collation_name, 1, CHARINDEX(',', collation_name)-1) AS collation_name FROM sys.syscollation;");
	    	  while (rst.next()){
	    		  System.out.println(rst.getString(1));
	    	  }
	  	    **/
      }
    catch(SQLException sqlE) {
    	System.out.println("SQLException :" + sqlE);
    }finally {
    	stmt.close();
    	stmt1.close();
    	stmt2.close();
    }

    metaTransferActions = new Meta_transfer_actions(etlrep);
    metaTransferActions.setAction_contents_01("DC_E_RAN_RNC_RAW_01");
    metaVersions = new Meta_versions(etlrep);
    metaTables = new Meta_tables(etlrep);
    collection = new Meta_collections(etlrep);
    connectionPool = new ConnectionPool(etlrep);

    Registry registry = LocateRegistry.getRegistry(1200);
    if(registry != null){
      try {
        registry.list();
      } catch (RemoteException e) {
        registry = null;
      }
    }

    if(registry == null){
      LocateRegistry.createRegistry(1200);
    }
  }
  @AfterClass
  public static void afterClass() throws SQLException {
    DirectoryHelper.delete(TMP_DIR);
    UnitDatabaseTestCase.__afterclass__();
  }
  @Test
  public void testconnectLicensingCache() throws Exception {
	  try{
		objToTest = new StubbedTransferEngine(true, true, 1, log);
        objToTest.getProperties();

  	  final Method m = TransferEngine.class.getDeclaredMethod("connectLicensingCache");
	  m.setAccessible(true);
	  m.invoke(objToTest);
	  }catch (Exception e){
		assertEquals(null, e.getMessage());
  	}
  }
  @Test
  public void testreloadAggregationCache() throws Exception {
	  try{
		objToTest = new StubbedTransferEngine(true, true, 1, log);
        objToTest.getProperties();
        AggregationRuleCache.initialize(etlrep);
        objToTest.reloadAggregationCache();

	  }catch (Exception e){
		assertNotNull(e);
  	}
  }

  @Test
  public void testCurrentProfile() throws Exception {
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedExecutionSlotProfileHandler).getActiveExecutionProfile();
			      }
		    });
		objToTest = new StubbedTransferEngine(true, true, 1, log);
        objToTest.getProperties();
        objToTest.currentProfile();

	  }catch (Exception e){
		assertNotNull(e);
  	}
  }
  @Test
  public void testgetProperties() throws Exception {
	  try{
		objToTest = new StubbedTransferEngine(true, true, 1, log);
        objToTest.getProperties();
	  }catch (Exception e){
		assertNotNull(e);
  	}
  }
  @Test
  public void testgetFailedSets() throws Exception {

	  objToTest = new StubbedTransferEngine(true, true, 1, log);
	  final List<Map<String, String>> expected = new ArrayList<Map<String, String>>();
	  final Map<String, String> m = new HashMap<String, String>();
		m.put("techpackName", "DWH_MONITOR");
		m.put("setName", "AggregationRuleCopy");
		m.put("setType", "PM");
		m.put("startTime", "2000-01-01 00:00:00.0");
		m.put("endTime", "2000-01-01 00:00:00.0");
		m.put("status", "failed");
		m.put("failureReason", "testTEXT");
		m.put("priority", "");
		m.put("runningSlot", "");
		m.put("runningAction", "");
		m.put("version", "((1))");
		expected.add(m);

        objToTest.getProperties();
        final List<Map<String, String>> actual = objToTest.getFailedSets();
		assertNotNull(actual);
  }
  @Test
  public void testgetFailedSetsException() throws Exception {
	  try{
		objToTest = new StubbedTransferEngine(true, true, 1, log);
        objToTest.getFailedSets();

  		}catch( Exception e) {
  			assertEquals("Error while fetching executed sets from DB; nested exception is: \n\tssc.rockfactory.RockException: Driver not found", e.getMessage());
  	  }
  }
  @Test
  public void testIsTechPackEnabledReturnTrue() throws Exception{
	  try{
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final boolean result = objToTest.isTechPackEnabled("DC_E_RAN_RNC_RAW_01", "Techpack");
	        assertTrue(result);
	  }catch( Exception e) {
		  e.printStackTrace();
	  }
  }
  @Test
  public void testIsTechPackEnabledReturnFalse() throws Exception{
	  try{
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final boolean result = objToTest.isTechPackEnabled("invalidTechPack", "PM");
	        assertFalse(result);
	  }catch( Exception e) {
		  e.printStackTrace();
	  }
  }
  @Test
  public void testShowDisabledSets() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final List<String> alSetsActions = objToTest.showDisabledSets();
	        assertNotNull(alSetsActions);
	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testShowDisabledSetsException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.showDisabledSets();
	  }
	  catch (Exception e ) {
		  assertEquals("Could not connect to etlrep", e.getMessage());
	  }
}

  @Test
  public void testGetTableNamesForRawEvents_RAW() throws Exception {

	final Timestamp startTime = new Timestamp(System.currentTimeMillis());

	final String storageId = "EVENT_E_SGEH:RAW";
	final String tableName1 = "EVENT_E_SGEH_RAW_01";
	final String tableName2 = "EVENT_E_SGEH_RAW_02";
	final String tableName3 = "EVENT_E_SGEH_RAW_03";
	final String tableName4 = "EVENT_E_SGEH_RAW_04";
	final String viewName = "EVENT_E_SGEH_RAW";
    PhysicalTableCache.testInit(storageId, tableName1, 1L, 10L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName2, 1L, 5L, "DEACTIVE");
    PhysicalTableCache.testInit(storageId, tableName3, 1L, 4L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName4, 1L, 2L, "ACTIVE");

    final Timestamp endTime = new Timestamp(System.currentTimeMillis());

    final List<String> tableNames = objToTest.getTableNamesForRawEvents(viewName, startTime, endTime);
    assertNotNull(tableNames);
  }

  @Test
  public void testGetTableNamesForRawEventsERR_RAW() throws Exception {

    final Timestamp startTime = new Timestamp(System.currentTimeMillis());

    final String storageId = "EVENT_E_SGEH_ERR:RAW";
    final String tableName1 = "EVENT_E_SGEH_ERR_RAW_01";
    final String tableName2 = "EVENT_E_SGEH_ERR_RAW_02";
    final String tableName3 = "EVENT_E_SGEH_ERR_RAW_03";
    final String tableName4 = "EVENT_E_SGEH_ERR_RAW_04";
    final String viewName = "EVENT_E_SGEH_ERR_RAW";
    PhysicalTableCache.testInit(storageId, tableName1, 1L, 1L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName2, 1L, 5L, "DEACTIVE");
    PhysicalTableCache.testInit(storageId, tableName3, 1L, 4L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName4, 1L, 2L, "ACTIVE");

	final Timestamp endTime = new Timestamp(System.currentTimeMillis());

    final List<String> tableNames = objToTest.getTableNamesForRawEvents(viewName, startTime, endTime);
    assertNotNull(tableNames);
  }

  @Test
  public void testGetTableNamesForRawEventsSUC_RAW() throws Exception {

    final Timestamp startTime = new Timestamp(System.currentTimeMillis());

    final String storageId = "EVENT_E_SGEH_SUC:RAW";
    final String tableName1 = "EVENT_E_SGEH_SUC_RAW_01";
    final String tableName2 = "EVENT_E_SGEH_SUC_RAW_02";
    final String tableName3 = "EVENT_E_SGEH_SUC_RAW_03";
    final String tableName4 = "EVENT_E_SGEH_SUC_RAW_04";
    final String viewName = "EVENT_E_SGEH_SUC_RAW";
    PhysicalTableCache.testInit(storageId, tableName1, 1L, 10L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName2, 1L, 5L, "DEACTIVE");
    PhysicalTableCache.testInit(storageId, tableName3, 1L, 4L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName4, 1L, 2L, "ACTIVE");

    final Timestamp endTime = new Timestamp(System.currentTimeMillis());

    final List<String> tableNames = objToTest.getTableNamesForRawEvents(viewName, startTime, endTime);
    assertNotNull(tableNames);
  }
  @Test
  public void testGetTableNamesForRawEventsException() throws Exception {
	  try{
		    final Timestamp startTime = new Timestamp(System.currentTimeMillis());
			final String viewName = " invalidViewName";
		    final Timestamp endTime = new Timestamp(System.currentTimeMillis());

		    objToTest.getTableNamesForRawEvents(viewName, startTime, endTime);
		  }
		  catch( Exception e) {
				assertEquals("Invalid viewName invalidViewName", e.getMessage());
		  	  }
	  }

  @Test
  public void testGetLatestTableNamesForRawEvents_RAW() throws Exception {
	final String storageId = "EVENT_E_SGEH:RAW";
	final String tableName1 = "EVENT_E_SGEH_RAW_01";
	final String tableName2 = "EVENT_E_SGEH_RAW_02";
	final String tableName3 = "EVENT_E_SGEH_RAW_03";
	final String tableName4 = "EVENT_E_SGEH_RAW_04";
	final String viewName = "EVENT_E_SGEH_RAW";
    PhysicalTableCache.testInit(storageId, tableName1, 0L, 10L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName2, 0L, 5L, "DEACTIVE");
    PhysicalTableCache.testInit(storageId, tableName3, 0L, 4L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName4, 0L, 2L, "ACTIVE");

    final List<String> tableNames = objToTest.getLatestTableNamesForRawEvents(viewName);
    assertNotNull(tableNames);
    }

  @Test
  public void testGetLatestTableNamesForRawEventsERR_RAW() throws Exception {
	final String storageId = "EVENT_E_SGEH_ERR:RAW";
	final String tableName1 = "EVENT_E_SGEH_ERR_RAW_01";
	final String tableName2 = "EVENT_E_SGEH_ERR_RAW_02";
	final String tableName3 = "EVENT_E_SGEH_ERR_RAW_03";
	final String tableName4 = "EVENT_E_SGEH_ERR_RAW_04";
	final String viewName = "EVENT_E_SGEH_ERR_RAW";
    PhysicalTableCache.testInit(storageId, tableName1, 0L, 1L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName2, 0L, 5L, "DEACTIVE");
    PhysicalTableCache.testInit(storageId, tableName3, 0L, 4L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName4, 0L, 2L, "ACTIVE");

    final List<String> tableNames = objToTest.getLatestTableNamesForRawEvents(viewName);
    assertNotNull(tableNames);
  }

  @Test
  public void testGetLatestTableNamesForRawEventsSUC_RAW() throws Exception {
	final String storageId = "EVENT_E_SGEH_SUC:RAW";
	final String tableName1 = "EVENT_E_SGEH_SUC_RAW_01";
	final String tableName2 = "EVENT_E_SGEH_SUC_RAW_02";
	final String tableName3 = "EVENT_E_SGEH_SUC_RAW_03";
	final String tableName4 = "EVENT_E_SGEH_SUC_RAW_04";
	final String viewName = "EVENT_E_SGEH_SUC_RAW";
    PhysicalTableCache.testInit(storageId, tableName1, 0L, 10L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName2, 0L, 5L, "DEACTIVE");
    PhysicalTableCache.testInit(storageId, tableName3, 0L, 4L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName4, 0L, 2L, "ACTIVE");

    final List<String> tableNames = objToTest.getLatestTableNamesForRawEvents(viewName);
    assertNotNull(tableNames);
  }
  @Test
  public void testGetLatestTableNamesForRawEventsException() throws Exception {
	  try{
		final String viewName = "invalidViewName";
	    objToTest.getLatestTableNamesForRawEvents(viewName);
	  }
	  catch( Exception e) {
			assertEquals("Invalid viewName invalidViewName", e.getMessage());
	  	  }
  }
  @Test
  public void testgetExecutedSets() throws Exception {

	  final List<Map<String, String>> expected = new ArrayList<Map<String, String>>();
	  final Map<String, String> m = new HashMap<String, String>();
		m.put("techpackName", "DWH_MONITOR");
		m.put("setName", "AggregationRuleCopy");
		m.put("setType", "PM");
		m.put("startTime", "2000-01-01 00:00:00.0");
		m.put("endTime", "2000-01-01 00:00:00.0");
		m.put("status", "ok");
		m.put("failureReason", "");
		m.put("priority", "");
		m.put("runningSlot", "");
		m.put("runningAction", "");
		m.put("version", "((1))");
		expected.add(m);

	    objToTest.getProperties();
	    final List<Map<String, String>> actual = objToTest.getExecutedSets();
		assertNotNull(actual);
  }
  @Test
  public void testgetExecutedSetsException() throws Exception {
	  	try{
	  	objToTest.getExecutedSets();
	  	}catch( Exception e) {
  			assertEquals("Error while fetching executed sets from DB; nested exception is: \n\tssc.rockfactory.RockException: Driver not found", e.getMessage());
	  	  }
  }

  @Test
  public void testgetQueuedSetsReturnNull() throws Exception {
	  try{
	  final List<Map<String, String>> expected = new ArrayList<Map<String, String>>();
	  final Map<String, String> m = new HashMap<String, String>();

		m.put("techpackName", "DWH_MONITOR");
		m.put("setName", "AggregationRuleCopy");
		m.put("setType", "PM");
		m.put("version", "((1))");
		m.put("startTime", "2000-01-01 00:00:00.0");
		m.put("endTime", "2000-01-01 00:00:00.0");
		m.put("status", "failed");
		m.put("failureReason", "testTEXT");
		m.put("priority", "");
		m.put("runningSlot", "");
		m.put("runningAction", "");
		m.put("ID", "0");
		m.put("creationDate", "");
		m.put("schedulingInfo", "");
		expected.add(m);

        objToTest.getProperties();
        objToTest.getQueuedSets();

	  } catch( Exception e) {
  			assertEquals(null, e.getMessage());
	  	  }
  }
  @Test
  public void testgetRunningSets() throws Exception {

	  final List<Map<String, String>> expected = new ArrayList<Map<String, String>>();
	  final Map<String, String> m = new HashMap<String, String>();

		m.put("techpackName", "DWH_MONITOR");
		m.put("setName", "AggregationRuleCopy");
		m.put("setType", "PM");
		m.put("version", "((1))");
		m.put("startTime", "2000-01-01 00:00:00.0");
		m.put("endTime", "2000-01-01 00:00:00.0");
		m.put("status", "failed");
		m.put("failureReason", "testTEXT");
		m.put("priority", "");
		m.put("runningSlot", "");
		m.put("runningAction", "");
		m.put("ID", "0");
		m.put("creationDate", "");
		m.put("schedulingInfo", "");
		expected.add(m);

		try{
		   context.checking(new Expectations() {
			      {
			        allowing(mockedExecutionSlotProfileHandler).getActiveExecutionProfile();
			        allowing(mockedExecutionSlotProfile).getAllExecutionSlots();
			        will(returnValue(mockedIterator));
			      }
		    });

        objToTest.getProperties();
        final List<Map<String, String>> actual = objToTest.getRunningSets();
		assertNotNull(actual);
		}catch (Exception e) {
	        e.printStackTrace();
	      }
  }
	@Test
	 public void testformatSchedulingInfo() throws Exception {
		 try{
			objToTest = new TransferEngine(true, true, 1, log);
			final String returnVal;
			final String sPropString ="timelevel=Time_Level\naggDate=1\nlockTable=Lock_Table";
		    returnVal=objToTest.formatSchedulingInfo(sPropString);
			assertNotNull(returnVal);
		    } catch (Exception e) {
		        e.printStackTrace();
		      }
	 }
	@Test
	@Ignore
	 public void testInit() throws Exception {
			try{

	        objToTest.getProperties();
	        objToTest.init();

		    } catch (Exception e) {
				  assertNull(e);
		      }
	 }
  @Test
    public void testaddStartupSetsToQueue() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

		    context.checking(new Expectations() {
		        {
		          allowing(mockedPriorityQueue).addSet(mockedEngineThread);
		        }
		      });


	  objToTest.getPriorityQueue();
	  final Method m = TransferEngine.class.getDeclaredMethod("addStartupSetsToQueue", RockFactory.class);
	  m.setAccessible(true);
	  m.invoke(objToTest, etlrep);
	  }
	  catch (Exception e ) {
		  assertEquals("WARNING: Could not create engineThread from (techpack/set): DC_E_RAN_RNC_RAW_01/DC_E_RAN_RNC_RAW_01", e.getMessage());
	  }
  }

  @Test
  public void testReloadAlarmConfigCache() {
  	try {
  	    objToTest = new StubbedTransferEngine(false, false, 2, log);
        objToTest.getProperties();
  		objToTest.reloadAlarmConfigCache();
	}catch (Exception e){
			e.printStackTrace();
	  }
  }
  @Test
  public void testCheckStarterLicense() throws Exception {

	final LicensingCache mockedLicensingCache = context.mock(LicensingCache.class);
    expectGetExpiryDate(mockedLicensingCache, TransferEngine.ENIQ_17_STARTER_LICENSE, 10);

    context.checking(new Expectations() {
      {
        allowing(mockedLicensingResponse).isValid();
        will(returnValue(true));

        allowing(mockedLicensingResponse).getMessage();
        will(returnValue(true));

        allowing(mockedLicensingCache).checkLicense(with(any(LicenseDescriptor.class)));
        will(returnValue(mockedLicensingResponse));
      }
    });

    final Method m = TransferEngine.class.getDeclaredMethod("checkStarterLicense", LicensingCache.class, String.class);
    m.setAccessible(true);

    final boolean result = (Boolean) m.invoke(objToTest, mockedLicensingCache, TransferEngine.ENIQ_17_STARTER_LICENSE);
    assertEquals(true, result);
  }

  @Test
  public void testCheckStarterLicenseForSTATS2LicenseNoLicense() throws Exception {

    final LicensingCache licensingCache = context.mock(LicensingCache.class);

    context.checking(new Expectations() {
      {
        oneOf(licensingCache).checkLicense(with(any(LicenseDescriptor.class)));
        will(returnValue(mockedLicensingResponse));

        oneOf(mockedLicensingResponse).isValid();
        will(returnValue(false));

      }
    });

  final Method starterMethod = TransferEngine.class.getDeclaredMethod("checkStarterLicense", LicensingCache.class, String.class);
  starterMethod.setAccessible(true);

  final boolean result = (Boolean) starterMethod.invoke(objToTest, licensingCache, TransferEngine.ENIQ_17_STARTER_LICENSE);
  assertEquals(false, result);
}

  @Test
  public void testCheckStarterLicenseForSTATS2LicenseNoExpiryLicense() throws Exception {

  final LicensingCache licensingCache = context.mock(LicensingCache.class);

  expectGetExpiryDate(licensingCache, TransferEngine.ENIQ_17_STARTER_LICENSE, -1);

  context.checking(new Expectations() {
    {
      allowing(licensingCache).checkLicense(with(any(LicenseDescriptor.class)));
      will(returnValue(mockedLicensingResponse));

      allowing(mockedLicensingResponse).isValid();
      will(returnValue(true));

      allowing(mockedLicensingResponse).getMessage();
      will(returnValue(true));
    }
  });

  final Method starterMethod = TransferEngine.class.getDeclaredMethod("checkStarterLicense", LicensingCache.class, String.class);
  starterMethod.setAccessible(true);

  final boolean result = (Boolean) starterMethod.invoke(objToTest, licensingCache, TransferEngine.ENIQ_17_STARTER_LICENSE);
  assertEquals(true, result);
}

  @Test
  public void testCheckStarterLicenseForSTATS2LicenseUnLimitedLicense() throws Exception {

  final LicensingCache licensingCache = context.mock(LicensingCache.class);

  final LicenseInformation liMock = context.mock(LicenseInformation.class);
  final Vector<LicenseInformation> testVector = new Vector<LicenseInformation>();

  context.checking(new Expectations() {
    {
      allowing(licensingCache).checkLicense(with(any(LicenseDescriptor.class)));
      will(returnValue(mockedLicensingResponse));

      allowing(mockedLicensingResponse).isValid();
      will(returnValue(true));

      allowing(mockedLicensingResponse).getMessage();
      will(returnValue(true));

      allowing(licensingCache).getLicenseInformation();
      will(returnValue(testVector));

      allowing(liMock).getFeatureName();
      will(returnValue(TransferEngine.ENIQ_17_STARTER_LICENSE));

      allowing(liMock).getDeathDay();
      will(returnValue(10));

    }
  });

  final Method starterMethod = TransferEngine.class.getDeclaredMethod("checkStarterLicense", LicensingCache.class, String.class);
  starterMethod.setAccessible(true);

  final boolean result = (Boolean) starterMethod.invoke(objToTest, licensingCache, TransferEngine.ENIQ_17_STARTER_LICENSE);
  assertEquals(true, result);
}

  @Test
  public void testcheckCapacityLicense() throws Exception {

	final LicensingCache mockedLicensingCache = context.mock(LicensingCache.class);
    expectGetExpiryDate(mockedLicensingCache, TransferEngine.ENIQ_17_STARTER_LICENSE, 10);

    context.checking(new Expectations() {
      {
        allowing(mockedLicensingResponse).isValid();
        will(returnValue(true));

        allowing(mockedLicensingResponse).getMessage();
        will(returnValue("License is valid"));

        allowing(mockedLicensingCache).checkCapacityLicense(with(any(LicenseDescriptor.class)), with(any(int.class)));
        will(returnValue(mockedLicensingResponse));
      }
    });

    final Method m = TransferEngine.class.getDeclaredMethod("checkCapacityLicense", LicensingCache.class, String.class, int.class);
    m.setAccessible(true);

    final boolean result = (Boolean) m.invoke(objToTest, mockedLicensingCache, TransferEngine.ENIQ_17_STARTER_LICENSE, 2);
    assertEquals(true, result);
  }
  @Test
  public void testclearCountingManagementCache() throws Exception{

	  try {
		final String storageId="EVENT_E_SGEH_SUC:RAW";
		objToTest.clearCountingManagementCache(storageId);

		}catch (Exception e){
			  assertNull(e);
		  }
  }
  @Test
  public void testActivateSchedulerException() throws Exception{
	  try{
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.activateScheduler();

		}catch (Exception e){
			assertEquals("Could not activate Scheduler.", e.getMessage().substring(0, 29));
		  }
}
  @Test
  public void testActivateSetInPriorityQueue() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.activateSetInPriorityQueue(1000l);

		}catch (Exception e){
			assertEquals(null, e.getMessage());
		  }
}
  @Test
  public void testExecute() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.execute(etlrep, "DC_E_RAN_RNC_RAW_01", "AggregationRuleCopy");

		}catch (Exception e){
			  assertNull(e);
		  }
  }
  @Test
  public void testExecuteEngineThreadWithListener() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			        allowing(mockedEngineThread);
			      }
		    });
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.executeEngineThreadWithListener(mockedEngineThread);

		}catch (Exception e){
			  assertNull(e);
		  }
  }
  @Test
  public void testExecuteWithSetListener() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        final String result=objToTest.executeWithSetListener("DC_E_RAN_RNC_RAW_01", "AggregationRuleCopy", "timelevel=Time_Level\naggDate=1\nlockTable=Lock_Table");
	        assertNotNull(result);

	  }catch( Exception e) {
		  e.printStackTrace();
	  }
  }
  @Test
  public void testExecuteWithSetListenerException() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
			objToTest.executeWithSetListener("DC_E_RAN_RNC_RAW_01", "AggregationRuleCopy", "timelevel=Time_Level\naggDate=1\nlockTable=Lock_Table");

	  }catch( Exception e) {
		  assertEquals("Could not start a Set; nested exception is: \n\tcom.distocraft.dc5000.etl.engine.common.EngineMetaDataException: " +
		  		"EngineThread (DC_E_RAN_RNC_RAW_01/AggregationRuleCopy) not created. Database connection failed", e.getMessage());
	  }
  }
  @Test
  public void testExecuteSetViaSetManager() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

		    final Properties prop = new Properties();
		    prop.setProperty("testExecuteSetViaSetManager", "value");
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        final SetStatusTO result = objToTest.executeSetViaSetManager("DC_E_RAN_RNC_RAW_01", "AggregationRuleCopy", "timelevel=Time_Level\naggDate=1\nlockTable=Lock_Table", prop);
	        assertNotNull(result);

	  }catch( Exception e) {
		  e.printStackTrace();
	  }
  }
  @Test
  public void testExecuteSetViaSetManagerException() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

		    final Properties prop = new Properties();
		    prop.setProperty("testExecuteSetViaSetManager", "value");
			objToTest = new StubbedTransferEngine(true, true, 1, log);
			objToTest.executeSetViaSetManager("DC_E_RAN_RNC_RAW_01", "AggregationRuleCopy", "timelevel=Time_Level\naggDate=1\nlockTable=Lock_Table", prop);

	  }catch( Exception e) {
		  assertEquals("Could not start a Set; nested exception is: \n\tcom.distocraft.dc5000.etl.engine.common.EngineMetaDataException: " +
			  		"EngineThread (DC_E_RAN_RNC_RAW_01/AggregationRuleCopy) not created. Database connection failed", e.getMessage());
	  }
  }
  @Test
  public void testExecuteString() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.execute("DC_E_RAN_RNC_RAW_01", "AggregationRuleCopy", "timelevel=Time_Level\naggDate=1\nlockTable=Lock_Table");

		}catch (Exception e){
			  assertNull(e);
		  }
  }
  @Test
  public void testExecuteStringException() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.execute("invalidcollection", "AggregationRuleCopy", "timelevel=Time_Level\naggDate=1\nlockTable=Lock_Table");

		}catch (Exception e){
			  assertEquals("Cannot start set, collection set invalidcollection doesn't exist", e.getMessage());
		  }
  }
  @Test
  public void testExecuteString1() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.execute(etlrep.getDbURL(), etlrep.getUserName(), etlrep.getPassword(), etlrep.getDriverName(), "DC_E_RAN_RNC_RAW_01", "AggregationRuleCopy","timelevel=Time_Level\naggDate=1\nlockTable=Lock_Table");

	  }catch (Exception e){
			  assertNull(e);
		  }
  }
  @Test
  public void testExecuteString1Exception() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.execute(null, null, null, null, null, null,null);

	  }catch (Exception e){
			  assertEquals("Could not start a Set; nested exception is: \n\t" +
					  "com.distocraft.dc5000.etl.engine.common.EngineMetaDataException: EngineThread (null/null) not created. Database connection failed",e.getMessage());
		  }
  }
  @Test
  public void testExecuteString3() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.execute(etlrep.getDbURL(), etlrep.getUserName(), etlrep.getPassword(), etlrep.getDriverName(), "DC_E_RAN_RNC_RAW_01", "AggregationRuleCopy");

		}catch (Exception e){
			  assertNull(e);
		  }
  }
  @Test
  public void testExecuteAndWaitException() throws Exception{
	  try{
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.executeAndWait("DC_E_RAN_RNC_RAW_01", "AggregationRuleCopy","timelevel=Time_Level\naggDate=1\nlockTable=Lock_Table");

	  }catch (Exception e){
			assertEquals("Could not start a Set; nested exception is: \n\tcom.distocraft.dc5000.etl.engine.common.EngineMetaDataException: " +
					"EngineThread (DC_E_RAN_RNC_RAW_01/AggregationRuleCopy) not created. Database connection failed", e.getMessage());
	  }
  }
  @Test
  public void testReloadPropertiesException() throws Exception{
	  try{
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final Properties props = new Properties();
			staticProperties = new File(TMP_DIR , "static.properties");

		    staticProperties.deleteOnExit();
		    props.put("SessionHandling.storageFile", TMP_DIR.getPath() + "\n");
		    props.put("SessionHandling.log.types", "");
		    props.put("firstDayOfTheWeek", "2");
		    StaticProperties.giveProperties(props);

		    final PrintWriter pw = new PrintWriter(new FileWriter(staticProperties));
		    pw.print("SessionHandling.storageFile=" + TMP_DIR.getPath()  + "\n");
		    pw.print("SessionHandling.log.types=LOADER\n");
		    pw.print("firstDayOfTheWeek=" + "2" + "\n");
		    pw.close();

			DataFormatCache.initialize(etlrep);
			PhysicalTableCache.initialize(etlrep);
			ActivationCache.initialize(etlrep);
			AggregationRuleCache.initialize(etlrep);

	        objToTest.reloadProperties();

		}catch (Exception e){
			assertEquals("Reload config failed; nested exception is: \n\tjava.lang.NullPointerException", e.getMessage());
		  }
  }
  @Test
  public void testReloadPropertiesLogDebugTrue() throws Exception{
	  try{

	        objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final Properties props = new Properties();

			staticProperties = new File(TMP_DIR.getPath() , "static.properties");

		    staticProperties.deleteOnExit();

		    props.put("SessionHandling.storageFile", TMP_DIR.getPath() + "\n");
		    props.put("SessionHandling.log.types", "");
		    props.put("firstDayOfTheWeek", "2");
		    props.put("log.debug", "true");
		    StaticProperties.giveProperties(props);

		    final PrintWriter pw = new PrintWriter(new FileWriter(staticProperties));
		    pw.print("SessionHandling.storageFile=" + TMP_DIR.getPath() +"\n");
		    pw.print("SessionHandling.log.types=LOADER\n");
		    pw.print("firstDayOfTheWeek=" + "2" + "\n");
		    pw.print("log.debug=" + "true" + "\n");
		    pw.close();

			DataFormatCache.initialize(etlrep);
			PhysicalTableCache.initialize(etlrep);
			ActivationCache.initialize(etlrep);
			AggregationRuleCache.initialize(etlrep);

	        objToTest.reloadProperties();

		}catch (Exception e){
			assertEquals("Reload config failed; nested exception is: \n\tjava.lang.NullPointerException", e.getMessage());
		  }
  }
  @Test
  public void testStatusRemoteException() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedExecutionSlotProfileHandler).getActiveExecutionProfile();
			      }
		    });

		  	objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        objToTest.status();
	  }catch( Exception e) {
		  assertEquals("Engine initialization has not been completed yet", e.getMessage());
	  }
  }
  @Test
  public void testUpdateTransformation() throws Exception{
	  try{
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.updateTransformation("DC_E_RAN_RNC_RAW_01");

		}catch (Exception e){
			assertEquals("Error while updating transformer cache; nested exception is: \n" +
					"	java.lang.NullPointerException", e.getMessage());
		  }
		}
  @Test
  public void testUpdateTransformationException() throws Exception{
	  try{
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.updateTransformation("invalid_tpname");

		}catch (Exception e){
			assertEquals("Error while updating transformer cache; nested exception is: \n\tssc.rockfactory.RockException: Driver not found", e.getMessage());
		  }
		}

  @Test
  public void testReloadTransformations() throws Exception{
	  try{
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        objToTest.reloadTransformations();

		}catch (Exception e){
			  assertNull(e);
		  }
  }
  @Test
  public void testReloadTransformationsException() throws Exception{
	  try{
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.reloadTransformations();

		}catch (Exception e){
			assertEquals("Error while revalidating transformer cache; nested exception is: \n\tssc.rockfactory.RockException: Driver not found", e.getMessage());
		  }
  }
  @Test
  public void testSlotInfo() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedExecutionSlotProfileHandler).getActiveExecutionProfile().getAllExecutionSlots();
			      }
		    });
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final List<Map<String, String>> result =objToTest.slotInfo();
	        assertNotNull(result);

	  }catch( Exception e) {
		  assertNull(e);
	  }
  }
  @Test
  public void testRestore() throws Exception{
	  try{
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });

		    final String storageId = "EVENT_E_SGEH:RAW";
		    final String tableName1 = "EVENT_E_SGEH_RAW_01";
		    final String tableName2 = "EVENT_E_SGEH_RAW_02";
		    final String tableName3 = "EVENT_E_SGEH_RAW_03";
		    final String tableName4 = "EVENT_E_SGEH_RAW_04";
		    PhysicalTableCache.testInit(storageId, tableName1, 0L, 10L, "ACTIVE");
		    PhysicalTableCache.testInit(storageId, tableName2, 0L, 5L, "DEACTIVE");
		    PhysicalTableCache.testInit(storageId, tableName3, 0L, 4L, "ACTIVE");
		    PhysicalTableCache.testInit(storageId, tableName4, 0L, 2L, "ACTIVE");

		    final List<String> measurementTypes = new ArrayList<String>();
		    measurementTypes.add("EVENT_E_SGEH:RAW");

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.restore("DC_E_RAN_RNC_RAW_01", measurementTypes, "1900:01:01", "2011:08:31");

		}catch (Exception e){
			  assertNull(e);
		  }
  }
  
  /**
   * If TransferEngine.getProperties() is not called,
   * connection to etlrep will fail.
   * A remote exception should be thrown if this happens.
   * @throws Exception
   */
  @Test(expected = RemoteException.class) 
  public void testRestoreException() throws Exception{
    context.checking(new Expectations() {

      {
        allowing(mockedPriorityQueue);
      }
    });

    final String storageId = "EVENT_E_SGEH:RAW";
    final String tableName1 = "EVENT_E_SGEH_RAW_01";
    final String tableName2 = "EVENT_E_SGEH_RAW_02";
    final String tableName3 = "EVENT_E_SGEH_RAW_03";
    final String tableName4 = "EVENT_E_SGEH_RAW_04";
    PhysicalTableCache.testInit(storageId, tableName1, 0L, 10L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName2, 0L, 5L, "DEACTIVE");
    PhysicalTableCache.testInit(storageId, tableName3, 0L, 4L, "ACTIVE");
    PhysicalTableCache.testInit(storageId, tableName4, 0L, 2L, "ACTIVE");

    final List<String> measurementTypes = new ArrayList<String>();
    measurementTypes.add("EVENT_E_SGEH:RAW");

    objToTest = new StubbedTransferEngine(true, true, 1, log);
    objToTest.restore("DC_E_RAN_RNC_RAW_01", measurementTypes, "1900:01:01", "2011:08:31");
  }
  @Test
    public void testConfigureWorkerLimits() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

		    System.setProperty("EngineMemoryNeedMB", "512");
		    System.setProperty("HEAP_SIZE", "512");


		    final Method m = TransferEngine.class.getDeclaredMethod("configureWorkerLimits", RockFactory.class);
			m.setAccessible(true);
			dwhrep=objToTest.getDwhRepRockFactory(null);
			m.invoke(objToTest, dwhrep);

		}catch (Exception e){
			  assertNull(e);
		  }
  }
  @Test
  public void testConfigureWorkerLimitsException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

		    System.setProperty("EngineMemoryNeedMB", "512");
		    System.setProperty("HEAP_SIZE", "512");


		    final Method m = TransferEngine.class.getDeclaredMethod("configureWorkerLimits", RockFactory.class);
			m.setAccessible(true);
			final RockFactory testDWHRep = null;
			m.invoke(objToTest, testDWHRep);

		}	catch (Exception e ) {
			  assertNull(e);
	  }
}
  @Test
  public void testGetMemoryUsageFactorsException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);

		    final Method m = TransferEngine.class.getDeclaredMethod("getMemoryUsageFactors", RockFactory.class);
			m.setAccessible(true);
			m.invoke(objToTest, etlrep);

		}	catch (Exception e ) {
			  assertEquals(null, e.getMessage());
	  }
}
  @Test
  public void testGetRegexpsForWorkerLimitationsException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);

		    final Method m = TransferEngine.class.getDeclaredMethod("getRegexpsForWorkerLimitations", RockFactory.class);
			m.setAccessible(true);
			m.invoke(objToTest, etlrep);

		}	catch (Exception e ) {
			  assertEquals(null, e.getMessage());
	  }
}
  @Test
  public void testLoggingStatus() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

			final List<String> result = objToTest.loggingStatus();
			assertNotNull(result);

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testDisableAction() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

			objToTest.disableAction("DC_E_RAN_RNC_RAW_01", "DC_E_RAN_RNC_RAW_01", 1);

	  }
	  catch (Exception e ) {
			assertEquals("Unable to disable action for techpack DC_E_RAN_RNC_RAW_01; nested exception is: \n" +
					"	java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: ENABLED_FLAG", e.getMessage());
	  }
}
  @Test
  public void testDisableActionException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
			objToTest.disableAction("DC_E_RAN_RNC_RAW_01", "DC_E_RAN_RNC_RAW_01", 1);

	  }
	  catch (Exception e ) {
			assertTrue(e.getMessage().contains("Unable to disable action for techpack DC_E_RAN_RNC_RAW_01"));
	  }
}
  @Test
  public void testEnableActionException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

			objToTest.enableAction("DC_E_RAN_RNC_RAW_01", "DC_E_RAN_RNC_RAW_01", 1);

	  }
	  catch (Exception e ) {
			assertEquals("Unable to enable action for techpack DC_E_RAN_RNC_RAW_01; nested exception is: \n" +
					"	java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: ENABLED_FLAG", e.getMessage());
	  }
}
  @Test
  public void testDisableTechpackException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

			objToTest.disableTechpack("DC_E_RAN_RNC_RAW_01");

	  }
	  catch (Exception e ) {
			assertEquals("Unable to disable techpack DC_E_RAN_RNC_RAW_01; nested exception is: \n" +
					"	java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: HOLD_FLAG", e.getMessage());
	  }
}
  @Test
  public void testEnableTechpackException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

			objToTest.enableTechpack("DC_E_RNC");

	  }
	  catch (Exception e ) {
			assertEquals("Unable to enable techpack DC_E_RNC; nested exception is: \n" +
					"	java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: HOLD_FLAG", e.getMessage());
	  }
}
  @Test
  public void testDisableSetException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

			objToTest.disableSet("DC_E_RNC", "DC_E_RNC_SET");

	  }
	  catch (Exception e ) {
			assertEquals("Unable to disable set DC_E_RNC_SET; nested exception is: \n" +
					"	java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: HOLD_FLAG", e.getMessage());
	  }
}
  @Test
  public void testEnableSetException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

			objToTest.enableSet("DC_E_RNC", "DC_E_RNC_SET");

	  }
	  catch (Exception e ) {
			assertEquals("Unable to enable set DC_E_RNC_SET; nested exception is: \n" +
					"	java.sql.SQLSyntaxErrorException: user lacks privilege or object not found: HOLD_FLAG", e.getMessage());
	  }
}
  @Test
  public void testgetMeasurementTypesAll() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final List<String> result = objToTest.getMeasurementTypesForRestore("DC_E_RAN_RNC_RAW_01", "ALL");
	        assertNotNull(result);
	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testgetMeasurementTypesNotAll() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final List<String> result = objToTest.getMeasurementTypesForRestore("DC_E_RAN_RNC_RAW_01", "NOTALL");
	        assertNotNull(result);
	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testManualCountReAgg() throws Exception {

	  try {
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });
			final Timestamp startTime = new Timestamp(System.currentTimeMillis());

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	    	final Timestamp endTime = new Timestamp(System.currentTimeMillis());

			objToTest.manualCountReAgg("DC_E_RAN_RNC_RAW_01", startTime, endTime, "DAY", true);

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test

  public void testManualCountReAggException() throws Exception {

	  try {
		    context.checking(new Expectations() {
			      {
			        allowing(mockedPriorityQueue);
			      }
		    });
			final Timestamp startTime = new Timestamp(System.currentTimeMillis());
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	    	final Timestamp endTime = new Timestamp(System.currentTimeMillis());

			objToTest.manualCountReAgg("DC_E_RAN_RNC_RAW_01", startTime, endTime, "DAY", true);

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testShowActiveInterfaces() throws Exception {

	  try {

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

			final List<String> result = objToTest.showActiveInterfaces();
			assertNotNull(result);

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testShowActiveInterfacesException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	    	objToTest.showActiveInterfaces();
	  }
	  catch (Exception e ) {
		  assertEquals("Unable to connect to database", e.getMessage());
	  }
}
  @Test
  public void testGetOldestReAggTimeInMs() throws Exception {

	  try {

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

			final long result = objToTest.getOldestReAggTimeInMs("DC_E_RAN_RNC_RAW_01");
			assertNotNull(result);

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testGetEarliestTimeForRawEventsException() throws Exception {

	  try {

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	    	dwhrep=etlrep;
			final long result = objToTest.getOldestReAggTimeInMs("DC_E_RAN_RNC_RAW_01");
			assertNotNull(result);

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }finally {
	  	    dwhrep = UnitDatabaseTestCase.getRockFactory(Schema.dwhrep);
		}
}
  @Test
  public void testGetStatusEventsWithId() throws Exception {

	  try {

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final SetStatusTO result =  objToTest.getStatusEventsWithId("0", 1, 1);
			assertNotNull(result);

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testGetDWHDBCharsetEncoding() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final Method m = TransferEngine.class.getDeclaredMethod("getDWHDBCharsetEncoding", RockFactory.class);
			m.setAccessible(true);
			m.invoke(objToTest, dcRock);

	  }
	  catch (Exception e ) {
			assertEquals(null, e.getMessage());		//syntax error returned here from getDWHDBCharsetEncoding
	  }
}
  @Test
  public void testWriteSQLLoadFile() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        objToTest.writeSQLLoadFile("fileContents", "newFile");

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testIsIntervalNameSupported() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final boolean result = objToTest.isIntervalNameSupported("DAY");
	        assertNotNull(result);

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testIsSetRunning() throws Exception {

	  try {
		    context.checking(new Expectations() {
			      {
			        allowing(mockedExecutionSlotProfileHandler).getActiveExecutionProfile();
			      }
		    });
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final boolean result=objToTest.isSetRunning(1L, 1L);
	        assertNotNull(result);

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testSetActiveExecutionProfile() throws Exception {

	  try {
		    context.checking(new Expectations() {
			      {
			        allowing(mockedExecutionSlotProfileHandler);
			        
			        oneOf(mockedPriorityQueue).isActive();
			        will(returnValue(false));
			        
			        oneOf(mockedPriorityQueue).setActive(true);            			       
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final boolean result=objToTest.setActiveExecutionProfile("profileName");
	        assertNotNull(result);
	  }
	  catch (Exception e ) {
			assertEquals("Could not activate Execution Profile; nested exception is: \n\tjava.lang.NullPointerException", e.getMessage());
	  }
}
  @Test
  public void testSetActiveExecutionProfileString() throws Exception {

	  try {
		    context.checking(new Expectations() {
			      {
			        allowing(mockedExecutionSlotProfileHandler).resetProfiles();
			        allowing(mockedExecutionSlotProfileHandler).writeProfile();
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final boolean result=objToTest.setActiveExecutionProfile("profileName","messageText");
	        assertNotNull(result);
	  }
	  catch (Exception e ) {
			assertEquals("Could not activate Execution Profile; nested exception is: \n\tjava.lang.NullPointerException", e.getMessage());
	  }
}
  @Test
  public void testGetPluginConstructorParameterInfo() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();
	        objToTest.getPluginConstructorParameterInfo("TimePlug");

	  }
	  catch (Exception e ) {
			assertEquals("; nested exception is: \n\tjava.lang.NullPointerException", e.getMessage());
	  }
}
  @Test
  public void testReloadDBLookups() throws Exception {

	  try {

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        DBLookupCache.initialize(etlrep);
	        objToTest.reloadDBLookups();

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testGetSetStatusViaSetManager() throws Exception {

	  try {

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final SetStatusTO result = objToTest.getSetStatusViaSetManager("DC_E_RAN_RNC_RAW_01", "AggregationRuleCopy", 1, 1);
	        assertNotNull(result);

	  }
	  catch (Exception e ) {
		  assertNull(e);
	  }
}
  @Test
  public void testReaggregate() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.getProperties();

	        final Properties props = new Properties();
		    staticProperties = new File(TMP_DIR.getPath(), "static.properties");

		    staticProperties.deleteOnExit();
		    props.put("SessionHandling.storageFile", TMP_DIR.getPath()+ "\n");
		    props.put("SessionHandling.log.types", "");
		    props.put("firstDayOfTheWeek", "2");
		    StaticProperties.giveProperties(props);

		    final PrintWriter pw = new PrintWriter(new FileWriter(staticProperties));
		    pw.print("SessionHandling.storageFile=" + TMP_DIR.getPath() + "\n");
		    pw.print("SessionHandling.log.types=LOADER\n");
		    pw.print("firstDayOfTheWeek=" + "2" + "\n");
		    pw.close();

			AggregationStatusCache.init(etlrep.getDbURL(), etlrep.getUserName(), etlrep.getPassword(), etlrep.getDriverName());
			AggregationRuleCache.initialize(etlrep);
			PhysicalTableCache.initialize(etlrep);

	        objToTest.reaggregate("aggregation", 1l);
	  }
	  catch (Exception e ) {
		  	e.printStackTrace();
	  }
}
  @Test
  public void testReaggregateException() throws Exception {

	  try {
			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.reaggregate("aggregation", 1l);
	  }
	  catch (Exception e ) {
		  assertEquals("Error while setting reAggregation status MANUAL :aggregation 1970-01-01 01:00:00; nested exception is: \n\tssc.rockfactory.RockException: Driver not found", e.getMessage());
	  }
}
  @Test
  public void testFastGracefulShutdown() throws Exception {

	  try {
		    context.checking(new Expectations() {
			      {
			        allowing(mockedExecutor).shutdown();		        
			        allowing(mockedExecutionSlotProfileHandler);
			      }
		    });

			objToTest = new StubbedTransferEngine(true, true, 1, log);
	        objToTest.fastGracefulShutdown();
	  }
	  catch (Exception e ) {
		  assertEquals("Could not start a graceful Shutdown; nested exception is: \n\tjava.lang.NullPointerException", e.getMessage());
	  }
}
  
  /**
   * Test removing tech pack sets from the priority queue.
   */
  @Test
  public void testRemoveTechPacksInPriorityQueue() {
    try {
      
      ArrayList<EngineThread> setsInQueue= new ArrayList<EngineThread>();
      setsInQueue.add(mockedEngineThread);      
      final Iterator<EngineThread> setsInQueueIter = setsInQueue.iterator();
      
      context.checking(new Expectations() {
        {          
          oneOf(mockedPriorityQueue).getAll();
          will(returnValue(setsInQueueIter));
          
          // We shouldn't check the whole list, only until we get
          // a match which in this case is the 2nd item
          // (see testListOfTPs below):
          exactly(2).of(mockedEngineThread).getTechpackName();
          will(returnValue("DC_E_BSS"));
          
          oneOf(mockedPriorityQueue).removeSet(mockedEngineThread);
          will(returnValue(true));
          
          allowing(mockedEngineThread).getSetName();
          will(returnValue("Test_Set_name"));
          
          allowing(mockedEngineThread).getTechpackName();
          will(returnValue("Test_Tech_pack_name"));
        }
      });

      // Test list of tech packs to remove:
      final List<String> testListOfTPs = new ArrayList<String>();
      testListOfTPs.add("DC_E_MGW");
      testListOfTPs.add("DC_E_BSS");
      testListOfTPs.add("DC_E_GGSN");
      testListOfTPs.add("DC_E_SGSN");

      objToTest = new StubbedTransferEngine(true, true, 1, log);
      objToTest.removeTechPacksInPriorityQueue(testListOfTPs);
    } catch (Exception e) {
      fail("Error in test for removeTechPacksInPriorityQueue()");
    }
  }
  
  /**
   * Test kill running sets for a list of tech packs.
   */
  @Test
  public void testKillRunningSets() {
    try {
      final ExecutionSlot mockedExecSlot = new ExecutionSlot(1, "slotName");
      mockedExecSlot.setRunningSet(mockedEngineThread);
      
      final ArrayList<ExecutionSlot> executionSlots = new ArrayList<ExecutionSlot>();
      executionSlots.add(mockedExecSlot);
      
      final Iterator<ExecutionSlot> iterator1 = executionSlots.iterator();
      final Iterator<ExecutionSlot> iterator2 = executionSlots.iterator();
      
      context.checking(new Expectations() {
        {          
          oneOf(mockedExecutionSlotProfileHandler).getActiveExecutionProfile();
          will(returnValue(mockedExecutionSlotProfile));
          
          oneOf(mockedExecutionSlotProfile).getAllRunningExecutionSlots();
          will(returnValue(iterator1));
          
          oneOf(mockedExecutionSlotProfile).getAllRunningExecutionSlots();
          will(returnValue(iterator2)); 
          
          oneOf(mockedEngineThread).getTechpackName();
          will(returnValue("DC_E_BSS"));
          
          // One tech pack is found that is running.
          // Kill the EngineThread for it.
          oneOf(mockedEngineThread).interrupt();
          
          // The second time around, set up the mocks so that
          // only a techpack that we don't want to kill is running.
          // So interrupt() should only be called once here. 
          oneOf(mockedEngineThread).getTechpackName();
          will(returnValue("DC_E_SASN"));
          
          allowing(mockedEngineThread).getSetName();
          will(returnValue("Test_Set_name"));
          
          allowing(mockedEngineThread).getTechpackName();
          will(returnValue("Test_Tech_pack_name"));
        }
      });

      // Test list of tech packs to remove:
      final List<String> testListOfTPs = new ArrayList<String>();
      testListOfTPs.add("DC_E_MGW");
      testListOfTPs.add("DC_E_BSS");
      testListOfTPs.add("DC_E_GGSN");
      testListOfTPs.add("DC_E_SGSN");

      objToTest = new StubbedTransferEngine(true, true, 1, log);
      objToTest.killRunningSets(testListOfTPs);
    } catch (Exception e) {
      fail("Error in test for killRunningSets()");
    }
  }
  
  @Test
  public void testListContainsIgnoreCase() {
    List<String> techPackNames = new ArrayList<String>();
    techPackNames.add("DWH_MONITOR");
    techPackNames.add("DC_E_CPP");
    techPackNames.add(" DC_E_SGSN ");
    techPackNames.add("dc_e_ggsn");
  
    assertTrue("List with tech pack should return true for search string in lower case", 
        objToTest.listContainsIgnoreCase(techPackNames, "dwh_monitor"));
    
    assertTrue("List with tech pack should return true for search string in mixed case", 
        objToTest.listContainsIgnoreCase(techPackNames, "DC_E_ggsn"));
    
    assertTrue("List with tech pack and white spaces should return true for search string in lower case", 
        objToTest.listContainsIgnoreCase(techPackNames, "dc_e_sgsn"));
    
    assertTrue("List with tech pack in upper case should return true for search string in lower case", 
        objToTest.listContainsIgnoreCase(techPackNames, "DC_E_GGSN"));
    
    assertTrue("List with tech pack in lower case should return true for search string in upper case", 
        objToTest.listContainsIgnoreCase(techPackNames, "DC_E_GGSN"));
        
    techPackNames.clear();
    assertFalse("Empty list should return false for contains check", 
        objToTest.listContainsIgnoreCase(techPackNames, "dwh_monitor"));
    
    techPackNames.add("DWH_MONITOR");
    assertTrue("List with tech pack should return true", 
        objToTest.listContainsIgnoreCase(techPackNames, "dwh_monitor"));
    
    techPackNames.clear();
    techPackNames.add("DWH_MONITOR");
    assertFalse("List without tech pack should return true", 
        objToTest.listContainsIgnoreCase(techPackNames, "dc_e_sgsn"));
    
    techPackNames = null;
    assertFalse("Null arguments should return fase", 
        objToTest.listContainsIgnoreCase(techPackNames, null));    
  }
  
  @Test
  public void testLockEventsUIusers() {
        
    try {
      objToTest = new StubbedTransferEngine(true, true, 1, log);      
      objToTest.lockEventsUIusers(true);
      Statement statement = dwhrep.getConnection().createStatement();
      ResultSet rs =  statement.executeQuery("SELECT * FROM ENIQ_EVENTS_ADMIN_PROPERTIES");
      String paramValue = "";
      while (rs.next()) {
        paramValue = rs.getString("PARAM_VALUE");
      }
      assertTrue("PARAM_VALUE should be true if Events UI users are locked", paramValue.equalsIgnoreCase("true"));
      
      // Test the negative case:
      objToTest.lockEventsUIusers(false);
      statement = dwhrep.getConnection().createStatement();
      rs =  statement.executeQuery("SELECT * FROM ENIQ_EVENTS_ADMIN_PROPERTIES");
      paramValue = "";
      while (rs.next()) {
        paramValue = rs.getString("PARAM_VALUE");
      }
      assertTrue("PARAM_VALUE should be False if Events UI users are unlocked", paramValue.equalsIgnoreCase("false"));
    } catch (Exception exc) {
      fail("Error in test for lockEventsUIusers()");
    }
  }
  
  private static void setUpPropertiesFileAndProperty() throws IOException {

      System.setProperty(Constants.DC_CONFIG_DIR_PROPERTY_NAME, TMP_DIR.getPath());
      System.setProperty("dc5000.config.directory", TMP_DIR.getPath()); //dc5000.config.directory
    
    final File staticProprties = new File(TMP_DIR, "static.properties");
    if(!staticProprties.exists() && !staticProprties.createNewFile()){
      fail("Failed to create " + staticProprties.getPath());
    }
    
      final File prop = new File(TMP_DIR, "ETLCServer.properties");
      prop.deleteOnExit();

      try {
			final PrintWriter pw = new PrintWriter(new FileWriter(prop));
			pw.println("ENGINE_DB_URL = jdbc:hsqldb:mem:repdb");
			pw.println("ENGINE_DB_USERNAME = etlrep");
			pw.println("ENGINE_DB_PASSWORD = " + AsciiCrypter.getInstance().encrypt("etlrep"));
			pw.println("ENGINE_DB_DRIVERNAME = org.hsqldb.jdbcDriver");
			pw.println("MAXIMUM_PRIORITY_LEVEL = 15");
			pw.println("SCHEDULER_POLL_INTERVALL = 1000");
			pw.println("PRIORITY_QUEUE_POLL_INTERVALL = 1000");
			pw.println("SCHEDULER_PORT = 12367");
			pw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	  final File niqProperties = new File(TMP_DIR,"niq.ini");
	  niqProperties.deleteOnExit();
		try {
			final PrintWriter pw = new PrintWriter(new FileWriter(niqProperties));
			pw.write(";--------------------------------------------------------------------------\n");
			pw.write("; ENIQ Network Information\n");
			pw.write("[ENIQ_NET_INFO]\n");
			pw.write("ManagedNodesCORE=200\n");
			pw.write("ServerType=Events\n");
			pw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
  }

  /**
   *
   * @param cacheMock
   * @param license
   * @throws Exception
   */
  private void expectGetExpiryDate(final LicensingCache cacheMock, final String license,
      final long deathDayValue) {
    final LicenseInformation liMock = context.mock(LicenseInformation.class);
    final Vector<LicenseInformation> testVector = new Vector<LicenseInformation>();
    testVector.add(liMock);

    try{
    context.checking(new Expectations() {

      {
        allowing(cacheMock).getLicenseInformation();
        will(returnValue(testVector));

        allowing(liMock).getFeatureName();
        will(returnValue(license));

        allowing(liMock).getDeathDay();
        will(returnValue(deathDayValue));
      }
    });
    }catch (final Exception e) {
		e.printStackTrace();
	}
  }
  public int getNumberOfPhysicalCPUs()
  {
      return 0;
  }
  class StubbedTransferEngine extends TransferEngine {

    private static final long serialVersionUID = 1L;
    
    {
    	setTimer(mockedExecutor);
    }
    /**
     * @param usePQ
     * @param useDefaultEXSlots
     * @param EXSlots
     * @param log
     * @throws RemoteException
     */
    public StubbedTransferEngine(final boolean usePQ, final boolean useDefaultEXSlots, final int EXSlots,
            final Logger log) throws RemoteException {
          super(usePQ, useDefaultEXSlots, EXSlots, log);

    }

        /**
         * @param mockedLog
         * @throws RemoteException
         */
        public StubbedTransferEngine(final Logger mockedLog) throws RemoteException {
          super(mockedLog);
        }
//    @Override
    EngineThread createNewEngineThread(final String collectionSetName, final String collectionName,
        final String ScheduleInfo) {
      return mockedEngineThread;
    }

    @Override
    public boolean isInitialized() throws RemoteException {
      return true;
    }

	@Override
	PriorityQueue getPriorityQueue() {
		return mockedPriorityQueue;
	}

	@Override
    ExecutionSlotProfileHandler getExecutionSlotProfileHandler() {
      return mockedExecutionSlotProfileHandler;
    }
   @Override
     RockFactory getDwhRepRockFactory(final String conName) {
      dwhrep = UnitDatabaseTestCase.getRockFactory(Schema.dwhrep);
      return dwhrep;		//dwhrep;	//mockedDwhRepRockFactory;
    }
   
   protected EngineThread getRunningSet(ExecutionSlot slot) {
     return mockedEngineThread;
   }

  }
 }