package com.distocraft.dc5000.etl.engine.sql;

import com.distocraft.dc5000.common.AdapterLog;
import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_tables;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.DataFormatCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.velocity.VelocityContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import ssc.rockfactory.RockFactory;
import static org.junit.Assert.assertNotNull;
@Ignore
public class PartitionedLoaderTest extends UnitDatabaseTestCase {

  private final Logger log = Logger.getLogger("PartitionedLoaderTest");
  private final static SetContext sctx = new SetContext();

  private static PartitionedLoader objUnderTest = null;
  static Meta_transfer_actions metaTransferActions;
  static Meta_versions metaVersions;
  static Meta_collections collection;
  static Meta_columns objMetaCol;
  static Meta_tables metaTables;
  static ConnectionPool connectionPool;

  private static File fRaw = null;

  Long collectionSetId = 1L;
  Long transferActionId = 1L;
  Long transferBatchId = 1L;
  Long connectId = 1L;

  static RockFactory etlrep;
  static RockFactory dwhrep;
  static RockFactory sa;

  static String batchColumnName;
  static String todaysDate = "";


  @BeforeClass
  public static void setUp() throws Exception {

    setup(TestType.unit);

    etlrep = getRockFactory(Schema.etlrep);
    dwhrep = getRockFactory(Schema.dwhrep);

    sa = getSaConnection_REPDB();

    Statement stmt = etlrep.getConnection().createStatement();
    Statement stmt1 = dwhrep.getConnection().createStatement();
    Statement stmt2 = sa.getConnection().createStatement();

    try {
      stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'DC_E_MGW', 'description', '((1))', 'Y', 'type')");

      stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'COLLECTION_SET_ID'  ,'COLLECTION_SET_ID'  ," +
        "'COL_TYPE'  ,1  ,'Y'  ,'((1))'  ,1  ,1 )");

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

      stmt.executeUpdate("INSERT INTO Meta_versions VALUES( '((1))'  ,'testDESCRIPTION'  ,'Y'  ,'Y'  ," +
        "'testENGINE_SERVER', "
        + "'testMAIL_SERVER'  ,'testSCHEDULER_SERVER'  ,1 )");

      stmt.executeUpdate("INSERT INTO Meta_transformation_tables VALUES( 1  ,'testTABLE_NAME'  ,'testDESCRIPTION', "
        + "'((1))'  ,'Y'  ,1  ,1  ,1  ,1 )");


      stmt1.executeUpdate("INSERT INTO DWHType (TECHPACK_NAME,TYPENAME,TABLELEVEL,STORAGEID,PARTITIONSIZE,PARTITIONCOUNT,STATUS,TYPE," +
        "OWNER,VIEWTEMPLATE,CREATETEMPLATE,BASETABLENAME,DATADATECOLUMN,PUBLICVIEWTEMPLATE,PARTITIONPLAN) " +
        "VALUES('DC_E_MGW', 'typename', 'tablelevel', 'storageid', 1, 1, 'ACTIVE', 'type', 'owner', 'viewtemplate', " +
        "'createtemplate', 'Example_table', 'datadatecolumn', 'publicviewtemplate', 'partitionplan')");

      stmt1.executeUpdate("INSERT INTO DWHPartition (STORAGEID,TABLENAME,STATUS,STARTTIME,ENDTIME,LOADORDER) " +
        "VALUES('storageid','EXAMPLE_Table','ACTIVE','2011-04-05 22:41:55.0','2011-04-05 22:55:55.0',1)");
      stmt1.executeUpdate("INSERT INTO TPActivation (TECHPACK_NAME,STATUS,TYPE) VALUES('DC_E_MGW','ACTIVE','type')");

      stmt1.executeUpdate("INSERT INTO TypeActivation (TECHPACK_NAME,STATUS,TYPE,TYPENAME,TABLELEVEL) VALUES" +
        "('DC_E_MGW','ACTIVE','type','typename','tablelevel')");
      stmt1.executeUpdate("INSERT INTO PartitionPlan (PARTITIONPLAN,DEFAULTPARTITIONSIZE,DEFAULTSTORAGETIME,PARTITIONTYPE) " +
        "VALUES('partitionplan','1',3,1)");

      stmt.execute("create table public.EXAMPLE_Table (name varchar(255))");
      stmt.executeUpdate("INSERT INTO public.EXAMPLE_Table VALUES ('test1')");
      stmt.executeUpdate("INSERT INTO public.EXAMPLE_Table VALUES ('test2')");

      final Calendar minDateForTableRaw00 = getCalendar(2010, 1, 1, 0, 0);
      final Calendar maxDateForTableRaw00 = getCalendar(2010, 2, 1, 0, 0);
      final Calendar minDateForTableRaw01 = getCalendar(2010, 3, 1, 0, 0);
      final Calendar maxDateForTableRaw01 = getCalendar(2010, 4, 1, 0, 0);
      final Calendar minDateForTableRaw02 = getCalendar(2010, 4, 1, 0, 0);
      final Calendar maxDateForTableRaw02 = getCalendar(2010, 5, 1, 0, 0);

      insertValuesIntoTimeRangeTable(stmt2, new Timestamp(minDateForTableRaw00.getTimeInMillis()), new Timestamp(
        maxDateForTableRaw00.getTimeInMillis()), new Timestamp(minDateForTableRaw01.getTimeInMillis()), new Timestamp(
        maxDateForTableRaw01.getTimeInMillis()), new Timestamp(minDateForTableRaw02.getTimeInMillis()), new Timestamp(
        maxDateForTableRaw02.getTimeInMillis()));


    } catch (SQLException sqlE) {
      System.out.println("SQLException :" + sqlE);
    }

    metaTransferActions = new Meta_transfer_actions(etlrep);
    metaTransferActions.setAction_contents_01("DC_E_RAN_RNC_RAW_01");
    metaTransferActions.setWhere_clause("useRAWSTATUS=true\nloaderParameters_BINARY=''\nloaderParameters=''\ntablename=" +
      "DC_E_RAN_RNC_RAW_01\nfileDuplicateCheck=true\ntaildir=raw");
    metaVersions = new Meta_versions(etlrep);
    metaTables = new Meta_tables(etlrep);
    collection = new Meta_collections(etlrep);
    objMetaCol = new Meta_columns(etlrep, 1L, "((1))", 1L, 1L);
    connectionPool = new ConnectionPool(etlrep);

    setupProperties();
    DataFormatCache.initialize(etlrep);

    SessionHandler.init();

    todaysDate = getDateAsYYYYMMDD();

    File homeDir = new File(System.getProperty("java.io.tmpdir"));
    homeDir.canExecute();
    homeDir.canRead();
    homeDir.canWrite();
    System.setProperty("ETLDATA_DIR", homeDir.getPath());
    fRaw = new File(System.getProperty("ETLDATA_DIR") + File.separator + "dc_e_ran_rnc_raw_01" + File.separator + "raw");
    fRaw.canExecute();
    fRaw.canRead();
    fRaw.canWrite();
    if (!fRaw.exists() && !fRaw.mkdirs()) {
      //
    }

    FileWriter fstream = new FileWriter(fRaw + File.separator + "dc_e_ran_rnc_raw_" + todaysDate.substring(0, 4) + "-" +
      todaysDate.substring(4, 6) + "-" + todaysDate.substring(6, 8) + ".txt");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write(fRaw.getAbsolutePath());
    out.close();

    final List<String> tables = new ArrayList<String>();
    tables.add(fRaw + File.separator + "dc_e_ran_rnc_raw_" + todaysDate.substring(0, 4) + "-" +
      todaysDate.substring(4, 6) + "-" + todaysDate.substring(6, 8) + ".txt");
    sctx.put("tableList", tables);

  }

  @Test
  public void testConstructor() throws EngineMetaDataException {
    try {
      PhysicalTableCache.initialize(etlrep);
      objUnderTest = new PartitionedLoader(metaVersions, collectionSetId, collection, transferActionId,
        transferBatchId, connectId, etlrep, connectionPool, metaTransferActions, sctx, log);
      assertNotNull(objUnderTest);
    } catch (Exception e) {
      System.out.println("Exception in Constructor: " + e);
    }
  }

  @Test
  public void testConstructor1() throws Exception {
      final PartitionedLoader objUnderTest1 = new PartitionedLoader();
      assertNotNull(objUnderTest1);
  }

  @Test
  public void testfillVelocityContext() throws Exception {
      VelocityContext vSctx = new VelocityContext();
      objUnderTest.fillVelocityContext(vSctx);
  }

  @Test
  public void testgetTableToFileMap() throws Exception {
      final Map<String, List<String>> tableMap = objUnderTest.getTableToFileMap();
      assertNotNull(tableMap);
  }

  @Test
  public void testupdateSessionLog() throws Exception {
      objUnderTest.updateSessionLog();
  }


  private static void setupProperties() throws Exception {
    String inputTableDir;
    Map<String, String> env = System.getenv();
    inputTableDir = env.get("WORKSPACE") + File.separator + "inputTableDir";
    Properties props = new Properties();
    props.setProperty("maxLoadClauseLength", "1000");
    props.setProperty("SessionHandling.storageFile", PartitionedLoader.class.getName());
    props.setProperty("SessionHandling.log.types", "SessionHandler");
    props.setProperty("SessionHandling.log.SessionHandler.class", AdapterLog.class.getName());
    props.setProperty("SessionHandling.log.ADAPTER.inputTableDir", inputTableDir);

    StaticProperties.giveProperties(props);
  }

  private static String getDateAsYYYYMMDD() {
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    return sdf.format(new Date());
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
                                                     final Timestamp maxTimeStamp00, final Timestamp minTimeStamp01,
                                                     final Timestamp maxTimeStamp01, final Timestamp minTimeStamp02,
                                                     final Timestamp maxTimeStamp02) throws SQLException {
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
      final File fTeardown = new File(System.getProperty("ETLDATA_DIR") + File.separator + "dc_e_ran_rnc_raw_01");
      final File fFailed = new File(System.getProperty("ETLDATA_DIR") + File.separator + "dc_e_ran_rnc_raw_01" +
        File.separator + "failed");
      final File fFile = new File(System.getProperty("ETLDATA_DIR") + File.separator + "dc_e_ran_rnc_raw_01" +
        File.separator + "failed" + File.separator + "dc_e_ran_rnc_raw_" + todaysDate.substring(0, 4) + "-" +
        todaysDate.substring(4, 6) + "-" + todaysDate.substring(6, 8) + ".txt");

      fFile.delete();
      fRaw.delete();
      fFailed.delete();
      fTeardown.delete();    //delete file & directory again...
      objUnderTest = null;
    } catch (Exception e) {
    }
  }

}
