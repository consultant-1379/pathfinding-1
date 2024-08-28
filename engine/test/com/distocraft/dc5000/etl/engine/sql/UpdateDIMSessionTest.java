package com.distocraft.dc5000.etl.engine.sql;

import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;
import static org.junit.Assert.fail;

public class UpdateDIMSessionTest extends UnitDatabaseTestCase {

  private static UpdateDIMSession objUnderTest;

  static Properties prop = new Properties();

  private static final File tmpDir =
    new File(System.getProperty("java.io.tmpdir"), UpdateDIMSessionTest.class.getSimpleName());

  private static final String sessionRelPath = "session/LOADER";

  private static final String loadDate_A = "2011-04-08";
  private static final String loadDate_B = "2011-05-23";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    delete(tmpDir);
    if (!tmpDir.exists() && !tmpDir.mkdirs()) {
      fail("Failed to create temp dir " + tmpDir.getPath());
    }
    System.setProperty("ETLDATA_DIR", tmpDir.getPath());

    setup(TestType.unit);

    Meta_versions metaVersions;
    final Long collectionSetId = 1L;
    Meta_collections collection;
    final Long transferActionId = 1L;
    final Long transferBatchId = 1L;
    final Long connectId = 1L;
    final ConnectionPool connectionPool;
    Meta_transfer_actions metaTransferActions;
    SetContext setContext;

    final RockFactory etlrep = getRockFactory(Schema.etlrep);

    final Statement stmt = etlrep.getConnection().createStatement();
    stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'set_name', 'description', '1', 'Y', 'type')");

    stmt.execute("CREATE TABLE DC_E_RAN_RNC_RAW_01 ( OSS_ID varchar(31), SESSION_ID varchar(31), BATCH_ID varchar(31), DATE_ID date, YEAR_ID varchar(31), "
      + "DATETIME_ID timestamp, TIMELEVEL varchar(31), ROWSTATUS varchar(31), pmNoDiscardSduDcch numeric, pmNoReceivedSduDcch numeric, pmNoRetransPduDcch numeric, "
      + "pmNoSentPduDcch numeric, pmNoDiscardSduDtch	numeric, pmNoReceivedSduDtch numeric, pmNoRetransPduDtch numeric, pmNoSentPduDtch numeric)");

    stmt.executeUpdate("INSERT INTO DC_E_RAN_RNC_RAW_01 VALUES('eniqoss_A', '1', '1', '2011-07-14', null, '"+loadDate_A+" 06:00:00.0', '15MIN', '', 1, 1, 1, 1, 1, 1, 1, 1)");
    stmt.executeUpdate("INSERT INTO DC_E_RAN_RNC_RAW_01 VALUES('eniqoss_B', '1', '1', '2011-07-14', null, '"+loadDate_B+" 06:00:00.0', '15MIN', '', 1, 1, 1, 1, 1, 1, 1, 1)");

    stmt.close();

    collection = new Meta_collections(etlrep);

    connectionPool = new ConnectionPool(etlrep);

    metaTransferActions = new Meta_transfer_actions(etlrep);

    metaTransferActions.setWhere_clause("useRAWSTATUS=true");

    metaVersions = new Meta_versions(etlrep);

    setContext = new SetContext();

    List<String> l = new ArrayList<String>();
    l.add("DC_E_RAN_RNC_RAW_01");
    setContext.put("tableList", l);

    String measType1 = "DC_E_RAN_RNC";
    setContext.put("MeasType", measType1);

    prop.setProperty("SessionHandling.storageFile", UpdateDIMSession.class.getName());
    prop.setProperty("SessionHandling.log.types", "LOADER");
    prop.setProperty("SessionHandling.log.LOADER.class", "com.distocraft.dc5000.common.LoaderLog");
    prop.setProperty("SessionHandling.log.LOADER.inputTableDir", "${ETLDATA_DIR}/"+sessionRelPath);
    prop.setProperty("SessionHandling.log.LOADER.inputFileLength", "1");
    StaticProperties.giveProperties(prop);

    SessionHandler.init();

    final Map<String, Object> sessionLogEntry = new HashMap<String, Object>();
    final long loadersetID = 123;
    sessionLogEntry.put("LOADERSET_ID", Long.toString(loadersetID));
		sessionLogEntry.put("SESSION_ID", "");
		sessionLogEntry.put("BATCH_ID", "");
		sessionLogEntry.put("TIMELEVEL", "");
		sessionLogEntry.put("DATATIME", "");
		sessionLogEntry.put("DATADATE", "");
		sessionLogEntry.put("ROWCOUNT", "");
		sessionLogEntry.put("SESSIONSTARTTIME", Long.toString(System.currentTimeMillis()));
		sessionLogEntry.put("SESSIONENDTIME", "");
		sessionLogEntry.put("STATUS", "");
		sessionLogEntry.put("TYPENAME", "");

    setContext.put("sessionLogEntry", sessionLogEntry);

    objUnderTest = new UpdateDIMSession(metaVersions, collectionSetId, collection, transferActionId,
      transferBatchId, connectId, etlrep, connectionPool, metaTransferActions, setContext);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    objUnderTest = null;
    delete(tmpDir);
  }

  @Test
  public void testExecuteLoaderDimSessionUpdate() throws Exception {
    objUnderTest.execute();
    final File unfinishedDir = new File(tmpDir, sessionRelPath);
    final File expectedFile = new File(unfinishedDir, "LOADER."+loadDate_A+".unfinished");
    Assert.assertTrue("No .unfinished file generated", expectedFile.exists());
    final BufferedReader reader = new BufferedReader(new FileReader(expectedFile));
    String line;
    boolean found_DateA = false, found_DateB = false;
    try{
      while ( (line=reader.readLine()) != null){
        if(line.contains(loadDate_A)){
          found_DateA = true;
        }
        if(line.contains(loadDate_B)){
          found_DateB = true;
        }
      }
    } finally {
      reader.close();
    }
    Assert.assertTrue("No row found in for DATE_ID "+loadDate_A+"' in " + expectedFile.getPath(), found_DateA);
    Assert.assertTrue("No row found in for DATE_ID "+loadDate_B+"' in " + expectedFile.getPath(), found_DateB);
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
}
