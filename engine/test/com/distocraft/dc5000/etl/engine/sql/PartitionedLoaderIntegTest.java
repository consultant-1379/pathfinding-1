package com.distocraft.dc5000.etl.engine.sql;

import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.DataFormatCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.Utils;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
import com.ericsson.eniq.exception.ConfigurationException;
import com.ericsson.eniq.exception.FileException;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class PartitionedLoaderIntegTest extends UnitDatabaseTestCase {

  private static final File baseDir = new File(System.getProperty("java.io.tmpdir"), "PartitionedLoader");
  private static RockFactory etlrep = null;

  private static final String TP_NAME = "DC_E_TEST";
  private static final String TYPE_NAME = TP_NAME + "_TYPE";

  private static final String REG_STORAGEID = TYPE_NAME + ":RAW";
  private static final String REG_TABLENAME_01 = TYPE_NAME + "_RAW_01";

  private static final long COLLECTION_SET_ID = 91;
  private static final long COLLECTION_ID = 191;
  private static final long TRANSFER_ACTION_ID = 291;
  private static Long CONNECTION_ID = null;

  private static Meta_versions meta_versions = null;
  private static Meta_collections meta_collections = null;
  private static Meta_transfer_actions meta_transfer_actions = null;
  private static ConnectionPool connection_pool = null;
  private static SetContext set_context = null;
  private static Logger logger = Logger.getAnonymousLogger();

  private static final String raw_regression_relative = TYPE_NAME.toLowerCase() + "/raw/";
  private static final File raw_regression = new File(baseDir, raw_regression_relative);

  private static final String fileDate_cache = "2222-11-22";
  private static final String dataFormat = "yyyy-MM-dd";
  private static final String loadFileName = "load_" + fileDate_cache + ".txt";
  private static final File raw_regression_load_cache = new File(raw_regression, loadFileName);

  @BeforeClass
  public static void beforeClass() throws SQLException, IOException, RockException,
    ConfigurationException, NoSuchFieldException, ParseException {
    System.setProperty("ETLDATA_DIR", baseDir.getPath());
    System.setProperty("EVENTS_ETLDATA_DIR", baseDir.getPath());
    final Map<String, Long> connectionIds = setup(TestType.unit);
    CONNECTION_ID = connectionIds.get(Schema.etlrep.name() + ":USER");
    set_context = new SetContext();

    etlrep = getRockFactory(Schema.etlrep);
    final Statement stmt = etlrep.getConnection().createStatement();
    try {
      stmt.execute("insert into META_COLLECTION_SETS " +
        "(COLLECTION_SET_ID, COLLECTION_SET_NAME, DESCRIPTION, VERSION_NUMBER, ENABLED_FLAG, TYPE)" +
        " values " +
        "(" + COLLECTION_SET_ID + ", '" + TP_NAME + "', '', '((131))', 'Y', 'Techpack');");

      stmt.execute("insert into META_COLLECTIONS " +
        "(COLLECTION_ID, COLLECTION_NAME, COLLECTION, MAIL_ERROR_ADDR, MAIL_FAIL_ADDR, MAIL_BUG_ADDR, MAX_ERRORS, " +
        "MAX_FK_ERRORS, MAX_COL_LIMIT_ERRORS, CHECK_FK_ERROR_FLAG, CHECK_COL_LIMITS_FLAG, LAST_TRANSFER_DATE, " +
        "VERSION_NUMBER, COLLECTION_SET_ID, USE_BATCH_ID, PRIORITY, QUEUE_TIME_LIMIT, ENABLED_FLAG, SETTYPE, " +
        "FOLDABLE_FLAG, MEASTYPE, HOLD_FLAG, SCHEDULING_INFO) " +
        "values " +
        "(" + COLLECTION_ID + ", 'Loader_" + TYPE_NAME + "', null, null, null, null, 0, 0, 0, 'N', 'N', null, '((131))', " +
        "" + COLLECTION_SET_ID + ", null, 0, 30, 'Y', 'Loader', 'Y', null, 'N', null);");

      final Properties where_clause_props = new Properties();
      where_clause_props.put("dateformat", "yyyy-MM-dd");
      where_clause_props.put("taildir", "raw");
      where_clause_props.put("techpack", TP_NAME);
      where_clause_props.put("tablename", TYPE_NAME);
      final String where_string = Utils.propertyToString(where_clause_props);
      stmt.execute("insert into META_TRANSFER_ACTIONS " +
        "(VERSION_NUMBER, TRANSFER_ACTION_ID, COLLECTION_ID, COLLECTION_SET_ID, ACTION_TYPE, TRANSFER_ACTION_NAME, " +
        "ORDER_BY_NO, DESCRIPTION, WHERE_CLAUSE_01, ACTION_CONTENTS_01, ENABLED_FLAG, CONNECTION_ID)" +
        " values " +
        "('((131))', " + TRANSFER_ACTION_ID + ", " + COLLECTION_ID + ", " + COLLECTION_SET_ID + ", 'Loader', " +
        "'Loader_" + TYPE_NAME + "', 1, null, '" + where_string + "', '', 'Y', 2);");
    } finally {
      try {
        stmt.close();
      } catch (Throwable t) {/**/}
    }
    final Meta_collections wherec = new Meta_collections(etlrep);
    wherec.setCollection_set_id(COLLECTION_SET_ID);
    wherec.setCollection_id(COLLECTION_ID);
    meta_collections = new Meta_collections(etlrep, wherec);

    final Meta_transfer_actions wheret = new Meta_transfer_actions(etlrep);
    wheret.setTransfer_action_id(TRANSFER_ACTION_ID);
    meta_transfer_actions = new Meta_transfer_actions(etlrep, wheret);

    meta_versions = new Meta_versions(etlrep);
    meta_versions.setVersion_number("0");
    connection_pool = new ConnectionPool(etlrep);

    delete(baseDir);
    if (!baseDir.exists() && !baseDir.mkdirs()) {
      Assert.fail("Errors creating base directory " + baseDir.getPath());
    }

    final Properties static_properies = new Properties();
    final File sp_file = new File(baseDir, "static.properties");
    if (!sp_file.exists() && !sp_file.createNewFile()) {
      Assert.fail("Error creating file " + sp_file.getPath());
    }
    sp_file.deleteOnExit();
    static_properies.put("SessionHandling.storageFile", sp_file.getPath());
    static_properies.put("SessionHandling.log.types", "");


    StaticProperties.giveProperties(static_properies);
    DataFormatCache.initialize(etlrep);
    SessionHandler.init();

    final SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
    final Date loadData = dateFormat.parse(fileDate_cache);

    PhysicalTableCache.initialize(etlrep);
    final PhysicalTableCache ptc = PhysicalTableCache.getCache();
    final List<PhysicalTableCache.PTableEntry> entries = new ArrayList<PhysicalTableCache.PTableEntry>();

    final PhysicalTableCache.PTableEntry entry1 = ptc.new PTableEntry();
    entry1.storageID = REG_STORAGEID;
    entry1.tableName = REG_TABLENAME_01;
    entry1.status = "ACTIVE";
    entry1.startTime = loadData.getTime() - 60000;
    entry1.endTime = loadData.getTime() + 60000;
    entry1.loadOrder = 0;
    entries.add(entry1);

    final Map<String, List<PhysicalTableCache.PTableEntry>> testCache =
      new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
    testCache.put(REG_STORAGEID, entries);
    PhysicalTableCache.testInit(testCache);
  }

  @AfterClass
  public static void afterClass(){
    delete(baseDir);
  }

  /*@Before
  public void before() throws IOException {
    delete(baseDir);
    if (!raw_regression.exists() && !raw_regression.mkdirs()) {
      Assert.fail("Errors creating raw directory " + raw_regression.getPath());
    }
    if (!raw_regression_load_cache.exists() && !raw_regression_load_cache.createNewFile()) {
      Assert.fail("Errors creating load file " + raw_regression_load_cache.getPath());
    }
  }*/

  private static File setup_regression() throws IOException {
    delete(baseDir);
    if (!raw_regression.exists() && !raw_regression.mkdirs()) {
      Assert.fail("Errors creating raw directory " + raw_regression.getPath());
    }
    if (!raw_regression_load_cache.exists() && !raw_regression_load_cache.createNewFile()) {
      Assert.fail("Errors creating load file " + raw_regression_load_cache.getPath());
    }
    return raw_regression_load_cache;
  }

  private static List<File> setup_multiple_etl_subdirs(final int etlDirCount) throws IOException {
    final List<File> created = new ArrayList<File>(etlDirCount);
    for(int i=0;i<etlDirCount;i++){
      String mountNumber = i <= 9 ? "0" : "";
      mountNumber += i;
      final File mpFile = new File(new File(baseDir, mountNumber), raw_regression_relative);
      if(!mpFile.exists() && !mpFile.mkdirs()){
        Assert.fail("Errors creating raw directory " + mpFile.getPath());
      }
      final File loadFile = new File(mpFile, loadFileName);
      if(!loadFile.exists() && !loadFile.createNewFile()){
        Assert.fail("Errors creating load file " + loadFile.getPath());
      }
      created.add(loadFile);
    }
    return created;
  }


  private static boolean delete(final File file) {
    if (!file.exists()) {
      return true;
    }
    if (file.isDirectory()) {
      final File[] sub = file.listFiles();
      for (File sf : sub) {
        if (!delete(sf)) {
          System.out.println("Couldn't delete directory " + sf.getPath());
          return false;
        }
      }
    }
    if (!file.delete()) {
      System.out.println("Couldn't delete file " + file.getPath());
      return false;
    }
    return true;
  }


  private PartitionedLoader getLoader(final int etlSubs) throws EngineMetaDataException {
    return new PartitionedLoader(
      meta_versions, COLLECTION_SET_ID, meta_collections, TRANSFER_ACTION_ID, (long) -1, CONNECTION_ID, etlrep,
      connection_pool, meta_transfer_actions, set_context, logger){
      @Override
      protected int getNumOfDirectories() {
        return etlSubs;
      }
    };
  }


  @Test
  public void test_loadBadFilename() throws IOException, EngineMetaDataException, FileException {
    final File loadFile = setup_regression();
    final String filename = "abc..ttt";
    final File renamed = new File(loadFile.getParent(), filename);
    if(!loadFile.renameTo(renamed)){
      Assert.fail("Errors creating load file " + renamed.getPath());
    }
    final PartitionedLoader loader = getLoader(0);
    final Map<String, List<String>> tableFileMap = loader.getTableToFileMap();
    Assert.assertNotNull(tableFileMap);
    Assert.assertTrue("Table->File mappings should be empty", tableFileMap.isEmpty());

    final char sep = File.separatorChar;
    final File failedFile = new File(baseDir, sep +  TYPE_NAME.toLowerCase() + sep + "failed" + sep + filename);
    Assert.assertTrue("Failed file not found, move failed.", failedFile.exists());
  }

  @Test
  public void test_loadBadFilename_MP() throws IOException, EngineMetaDataException, FileException {
    final int etlSubs = 1;
    final List<File> expected = setup_multiple_etl_subdirs(etlSubs);

    final File file = expected.get(0);
    final String filename = "abc..ttt";
    final File renamed = new File(file.getParent(), filename);
    if(!file.renameTo(renamed)){
      Assert.fail("Errors creating load file " + renamed.getPath());
    }
    final PartitionedLoader loader = getLoader(etlSubs);
    final Map<String, List<String>> tableFileMap = loader.getTableToFileMap();
    Assert.assertNotNull(tableFileMap);
    Assert.assertTrue("Table->File mappings should be empty", tableFileMap.isEmpty());

    final char sep = File.separatorChar;
    final File failedFile = new File(baseDir, "00" + sep +  TYPE_NAME.toLowerCase() + sep + "failed" + sep + filename);
    Assert.assertTrue("Failed file not found, move failed.", failedFile.exists());
  }

  @Test
  public void test_getTableToFileMap_MultipleDirs() throws Exception {
    final int etlSubs = 4;
    final List<File> expected = setup_multiple_etl_subdirs(etlSubs);
    final PartitionedLoader loader = getLoader(etlSubs);
    final Map<String, List<String>> tableFileMap = loader.getTableToFileMap();
    Assert.assertNotNull(tableFileMap);
    Assert.assertTrue("Table->File mappings wrong size", tableFileMap.size() == 1);
    Assert.assertTrue("Wrong table mapped to file", tableFileMap.containsKey(REG_TABLENAME_01));
    final List<String> files = tableFileMap.get(REG_TABLENAME_01);
    Assert.assertTrue("Wrong number of files mapped to table", files.size() == etlSubs);
    for(File expectedFile : expected){
      Assert.assertTrue("Wrong file mapped to table", files.contains(expectedFile.getPath()));
    }
  }

  @Test
  public void test_getTableToFileMap_Regression() throws Exception {
    setup_regression();
    final PartitionedLoader loader = getLoader(0);
    final Map<String, List<String>> tableFileMap = loader.getTableToFileMap();
    Assert.assertNotNull(tableFileMap);
    Assert.assertTrue("Table->File mappings wrong size", tableFileMap.size() == 1);
    Assert.assertTrue("Wrong table mapped to file", tableFileMap.containsKey(REG_TABLENAME_01));
    final List<String> files = tableFileMap.get(REG_TABLENAME_01);
    Assert.assertTrue("Wrong number of files mapped to table", files.size() == 1);
    Assert.assertTrue("Wrong file mapped to table", files.contains(raw_regression_load_cache.getPath()));
  }
}
