package com.distocraft.dc5000.etl.engine.sql;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.DataFormatCache;
import com.ericsson.eniq.common.CommonUtils;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
import com.ericsson.eniq.exception.FileException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class UnPartitionedLoaderTest extends UnitDatabaseTestCase {

  private static final String COLLECTION_SET_NAME = "DIM_E_TEST";
  private static final String TYPE_NAME = COLLECTION_SET_NAME + "_TYPE";
  private static final String LOAD_TABLE = TYPE_NAME + "_LOADED";
  private static final String SET_NAME = "TopologyLoader_" + TYPE_NAME;
  private static final String ACTION_NAME = "UnPartitioned_Loader_" + TYPE_NAME;
  private static RockFactory etlrep = null;

  private static Meta_versions meta_versions = null;
  private static Meta_collections meta_collections = null;
  private static Meta_transfer_actions meta_transfer_actions = null;

  private static final long COLLECTION_SET_ID = 1;
  private static final long COLLECTION_ID = 2;

  private static final long TRANSFER_ACTION_ID = (long) 3;
  private static final long TRANSFER_BATCH_ID = (long) 789;
  private static Long CONNECTION_ID = null;

  private static ConnectionPool connection_pool = null;
  private static SetContext set_context = null;

  private static Logger logger = Logger.getAnonymousLogger();

  private static final File baseDir = new File(System.getProperty("java.io.tmpdir"), "UnPartitionedLoader");

  private static final File NIQ_INI = new File(baseDir, "niq.ini");

  private static Logger log;

  private static final String raw_regression_relative = TYPE_NAME.toLowerCase() + "/raw/";
  private static final File raw_regression = new File(baseDir, raw_regression_relative);

  private static final File raw_regression_load = new File(raw_regression, "load_2222-11-22.txt");

  @BeforeClass
  public static void beforeClass() throws SQLException, EngineMetaDataException, RockException, IOException {
    System.setProperty("ETLDATA_DIR", baseDir.getPath());
    System.setProperty("EVENTS_ETLDATA_DIR", baseDir.getPath());
    System.setProperty("CONF_DIR", baseDir.getPath());
    raw_regression.deleteOnExit();
    raw_regression_load.deleteOnExit();

    setup(TestType.unit); // Need to change
    etlrep = getRockFactory(Schema.etlrep);

    meta_versions = new Meta_versions(etlrep);
    meta_versions.setVersion_number("0");
    connection_pool = new ConnectionPool(etlrep);

    final Properties where_clause = new Properties();
    where_clause.setProperty("techpack", COLLECTION_SET_NAME);
    where_clause.setProperty("tablename", LOAD_TABLE);
    where_clause.setProperty("pattern", ".+");
    where_clause.setProperty("dir", "${ETLDATA_DIR}/" + TYPE_NAME.toLowerCase() + "/raw/");
    final String where_clause_string = TransferActionBase.propertiesToString(where_clause);

    final Statement stmt = etlrep.getConnection().createStatement();
    try {
      stmt.execute("insert into META_COLLECTION_SETS (" +
        "COLLECTION_SET_ID, COLLECTION_SET_NAME, DESCRIPTION, VERSION_NUMBER, ENABLED_FLAG, TYPE) " +
        "values " +
        "(" + COLLECTION_SET_ID + ", '" + COLLECTION_SET_NAME + "', '', '((26))', 'Y', 'Techpack');");

      stmt.execute("insert into META_COLLECTIONS (" +
        "COLLECTION_ID, COLLECTION_NAME, COLLECTION, LAST_TRANSFER_DATE, VERSION_NUMBER, COLLECTION_SET_ID, " +
        "USE_BATCH_ID, PRIORITY, ENABLED_FLAG, SETTYPE, MEASTYPE, HOLD_FLAG, SCHEDULING_INFO, MAX_ERRORS, " +
        "MAX_FK_ERRORS, MAX_COL_LIMIT_ERRORS, CHECK_FK_ERROR_FLAG, CHECK_COL_LIMITS_FLAG) " +
        "values " +
        "(" + COLLECTION_ID + ", '" + SET_NAME + "', null, null, '((26))', " + COLLECTION_SET_ID + ", null, 1, 'Y', 'Topology', " +
        "null, 'N', null, 0, 0, 0, 0, 0);");

      stmt.execute("insert into META_TRANSFER_ACTIONS (" +
        "VERSION_NUMBER, TRANSFER_ACTION_ID, COLLECTION_ID, COLLECTION_SET_ID, ACTION_TYPE, TRANSFER_ACTION_NAME, " +
        "ORDER_BY_NO, DESCRIPTION, WHERE_CLAUSE_01, ACTION_CONTENTS_01, ENABLED_FLAG, CONNECTION_ID) " +
        "values " +
        "('((26))', " + TRANSFER_ACTION_ID + ", " + COLLECTION_ID + ", " + COLLECTION_SET_ID + ", 'UnPartitioned Loader', " +
        "'" + ACTION_NAME + "', 1, 'description_text', " +
        "'" + where_clause_string + "', 'sql_load_template', 'Y', " + CONNECTION_ID + ");");
    } finally {
      stmt.close();
    }

    final Meta_collections wherec = new Meta_collections(etlrep);
    wherec.setCollection_set_id(COLLECTION_SET_ID);
    wherec.setCollection_id(COLLECTION_ID);
    meta_collections = new Meta_collections(etlrep, wherec);

    final Meta_transfer_actions wheret = new Meta_transfer_actions(etlrep);
    wheret.setTransfer_action_id(TRANSFER_ACTION_ID);
    meta_transfer_actions = new Meta_transfer_actions(etlrep, wheret);

    StaticProperties.giveProperties(new Properties());
    DataFormatCache.initialize(etlrep);
    log = Logger.getLogger("UnpartitionedLoaderTest");
  }

  @AfterClass
  public static void afterClass() {
    meta_versions = null;
    meta_collections = null;
    meta_transfer_actions = null;
    connection_pool = null;
    set_context = null;
    logger = null;
  }

  @Before
  public void before() throws IOException {
    delete(baseDir);
    if (!baseDir.exists() && !baseDir.mkdirs()) {
      Assert.fail("Errors creating base directory " + baseDir.getPath());
    }
    if (!raw_regression.exists() && !raw_regression.mkdirs()) {
      Assert.fail("Errors creating raw directory " + raw_regression.getPath());
    }
    if (!raw_regression_load.exists() && !raw_regression_load.createNewFile()) {
      Assert.fail("Errors creating load file " + raw_regression_load.getPath());
    }
  }

  @After
  public void after() {
    delete(baseDir);
    NIQ_INI.delete();
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

  private UnPartitionedLoader getLoader() throws Exception {
    return new UnPartitionedLoader(
      meta_versions,
      COLLECTION_SET_ID,
      meta_collections,
      TRANSFER_ACTION_ID,
      TRANSFER_BATCH_ID,
      CONNECTION_ID,
      etlrep,
      connection_pool,
      meta_transfer_actions,
      set_context,
      logger);
  }

  @Test
  public void testExpandPaths() throws Exception {

    final int mpCount = 4;
    // Initialiaze the niq.ini file.
    try {
      final PrintWriter pw = new PrintWriter(new FileWriter(NIQ_INI));

      pw.write("[DIRECTORY_STRUCTURE]\n");
      pw.write("FileSystems=" + mpCount);
      pw.close();
    } catch (Exception e) {
      e.printStackTrace();
    }


    final UnPartitionedLoader loader = getLoader();

    final List<File> expectedFiles = new ArrayList<File>(mpCount);
    for (int i = 0; i < mpCount; i++) {
      String mountNumber = i <= 9 ? "0" : "";
      mountNumber += i;
      final File mpFile = new File(new File(baseDir, mountNumber), raw_regression_relative);
      if (!mpFile.exists() && !mpFile.mkdirs()) {
        Assert.fail("Errors creating raw directory " + mpFile.getPath());
      }
      expectedFiles.add(mpFile);
    }
    List<File> expanded = loader.expandEtlPaths(raw_regression);
    Assert.assertNotNull(expanded);
    Assert.assertTrue("Base file expanded to the wrong number of files", expectedFiles.size() == expanded.size());
    for (File expected : expectedFiles) {
      Assert.assertTrue("Erroneous Expected Expanded Entry!", expanded.contains(expected));
    }
  }


  @Test
  public void testExpandPaths_No_HS() throws Exception {
    final UnPartitionedLoader loader = getLoader();

    final List<File> expectedFiles = new ArrayList<File>();
    final String fileName = baseDir + File.separator + raw_regression_relative;
    final File mpFile = new File(fileName);
    mpFile.deleteOnExit();
    expectedFiles.add(mpFile);

    List<File> expanded = loader.expandEtlPaths(raw_regression);
    Assert.assertNotNull(expanded);
    Assert.assertTrue("Base file expanded to the wrong number of files", expectedFiles.size() == expanded.size());
    for (File expected : expectedFiles) {
      Assert.assertTrue("Erroneous Expected Expanded Entry!", expanded.contains(expected));
    }
  }

  @Test
  public void testContructor() throws Exception {
    final UnPartitionedLoader loader = getLoader();
    Assert.assertNotNull(loader);
  }

  @Test
  public void testGetTableToFileMap_Regression_NoDir() throws Exception {
    final UnPartitionedLoader loader = getLoader();
    delete(raw_regression);
    final Map<String, List<String>> tableMap = loader.getTableToFileMap();
    Assert.assertTrue("Non existing dir should result in empty map", tableMap.isEmpty());
  }

  @Test
  public void testGetTableToFileMap_Regression() throws Exception {
    final UnPartitionedLoader loader = getLoader();
    final Map<String, List<String>> tableFileMap = loader.getTableToFileMap();
    Assert.assertNotNull("Table->File map should not be null", tableFileMap);
    Assert.assertTrue("Table->File doesnt contain expected mapping for " + LOAD_TABLE,
      tableFileMap.containsKey(LOAD_TABLE));
    final List<String> loadFiles = tableFileMap.get(LOAD_TABLE);
    Assert.assertTrue("Wrong number of load file found", loadFiles.size() == 1);
    final String foundFile = loadFiles.get(0);
    Assert.assertEquals("Wrong load file found", raw_regression_load.getPath().toLowerCase(), foundFile.toLowerCase());
  }

  @Test
  public void testGetTableToFileMap() throws Exception {
    // Initialiaze the niq.ini file.
    // Setting the number of file systems to 4
    final int mpCount = 4;
    try {
      final PrintWriter pw = new PrintWriter(new FileWriter(NIQ_INI));

      pw.write("[DIRECTORY_STRUCTURE]\n");
      pw.write("FileSystems=" + mpCount);
      pw.close();
    } catch (Exception e) {
      e.printStackTrace();
    }


    final List<File> expectedFiles = new ArrayList<File>(mpCount);
    for (int i = 0; i < mpCount; i++) {
      String mountNumber = i <= 9 ? "0" : "";
      mountNumber += i;
      final File mpFile = new File(new File(baseDir, mountNumber), raw_regression_relative);
      mpFile.mkdirs();
      mpFile.deleteOnExit();
      final File txtFile = new File(mpFile, mountNumber + "_load.txt");
      txtFile.createNewFile();
      txtFile.deleteOnExit();
      if (!mpFile.exists() && !mpFile.mkdirs()) {
        Assert.fail("Errors creating raw directory " + mpFile.getPath());
      }
      if (!txtFile.exists()) {
        Assert.fail("Errors creating file" + txtFile.getPath());
      }
      expectedFiles.add(txtFile);
    }
    final UnPartitionedLoader loader = getLoader();
    final Map<String, List<String>> tableFileMap = loader.getTableToFileMap();
    Assert.assertNotNull("Table->File map should not be null", tableFileMap);
    Assert.assertTrue("Table->File doesnt contain expected mapping for " + LOAD_TABLE,
      tableFileMap.containsKey(LOAD_TABLE));
    final List<String> loadFiles = tableFileMap.get(LOAD_TABLE);
    Assert.assertTrue("Wrong number of load file found", loadFiles.size() == 4);
  }

  @Test
  public void testGetDirectorySpreadCount_Zero() throws Exception {
    try {
      final PrintWriter pw = new PrintWriter(new FileWriter(NIQ_INI));
      pw.write("PriorityQueue.unremovableSetTypes=Loader");
      pw.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    final int directoryCount = CommonUtils.getNumOfDirectories(log);
    Assert.assertEquals(0, directoryCount);
  }

  @Test
  public void testGetDirectorySpreadCount() throws Exception {
    final int mpCount = 4;
    try {
      final PrintWriter pw = new PrintWriter(new FileWriter(NIQ_INI));

      pw.write("[DIRECTORY_STRUCTURE]\n");
      pw.write("FileSystems="+mpCount);
      pw.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    final int directoryCount = CommonUtils.getNumOfDirectories(log);
    Assert.assertEquals(mpCount, directoryCount);
  }
}
