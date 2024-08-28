package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.Utils;

public class DimEventsCreateTPDirCheckerSetTest {

  private static final String EVENT_E_TERM = "EVENT_E_TERM";

  private static final String setVersion = "((12))";

  private static String setName = "Loader";

  private static final String versionId = setName + ":" + setVersion;

  private static RockFactory dwhrep = null;

  private static RockFactory etlrep = null;

  @BeforeClass
  public static void setUp() throws Exception {
    StaticProperties.giveProperties(new Properties());
    etlrep = CreateRockFactoryHelper.createEtlRepRockFactory();
    dwhrep = CreateRockFactoryHelper.createDwhRepFactory();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    CreateRockFactoryHelper.cleanUpRockFactory();
    etlrep = null;
    dwhrep = null;
  }
  
  @Test
  public void checkFactoryReturnsDimEventsCreateTPDirCheckerSet() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, EVENT_E_TERM);
    assertTrue(createTPDirCheckerSet instanceof DimEventsCreateTPDirCheckerSet);
  }

  @Test
  public void checkThatBaseDirectoryIsCorrectForDimEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, EVENT_E_TERM);

    assertEquals("/eniq/data/etldata_", createTPDirCheckerSet.getBaseEtlDirectory());
  }

  @Test
  public void checkThatCorrectEtlDirectoriesAreCreatedForMeasurementTableForDimEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, EVENT_E_TERM);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    final long transferActionId = createTPDirCheckerSet.createETLDirsAndAddToDbForMeasurementTable(0, 0, "DC_E_TEST", "raw", prop);

    // Check that the transferActionId has been updated correctly
    assertEquals(80, transferActionId);

    ResultSet resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");

    // Everything up to 10 should have an extra zero
    for (int i = 0; i < 10; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/DC_E_TEST", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/DC_E_TEST/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/DC_E_TEST_raw", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/DC_E_TEST/raw/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/DC_E_TEST_joined", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/DC_E_TEST/joined/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/DC_E_TEST_raw1", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/DC_E_TEST/raw/1/", resultSet.getString(2));
    }

    for (int i = 10; i < 16; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/DC_E_TEST", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/DC_E_TEST/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/DC_E_TEST_raw", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/DC_E_TEST/raw/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/DC_E_TEST_joined", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/DC_E_TEST/joined/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/DC_E_TEST_raw1", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/DC_E_TEST/raw/1/", resultSet.getString(2));
    }

    assertFalse(resultSet.next());

    // Delete ETL directories
    createTPDirCheckerSet.removeETLDirsFromDbForMeasurementTable("DC_E_TEST", Utils.getSetMaxID(etlrep) + 1 + 0);
    resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");
    assertFalse(resultSet.next());
  }

  @Test
  public void checkThatCorrectEtlDirectoriesAreCreatedForReferenceTableForDimEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, EVENT_E_TERM);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    final long transferActionId = createTPDirCheckerSet.createETLDirsAndAddToDbForReferenceTable(0, 0, "DC_E_TEST", prop);

    // Check that the transferActionId has been updated correctly
    assertEquals(64, transferActionId);

    ResultSet resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");

    // Everything up to 10 should have an extra zero
    for (int i = 0; i < 10; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/DC_E_TEST", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/DC_E_TEST/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/DC_E_TEST_raw", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/DC_E_TEST/raw/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/DC_E_TEST_joined", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/DC_E_TEST/joined/", resultSet.getString(2));
    }

    for (int i = 10; i < 16; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/DC_E_TEST", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/DC_E_TEST/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/DC_E_TEST_raw", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/DC_E_TEST/raw/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/DC_E_TEST_joined", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/DC_E_TEST/joined/", resultSet.getString(2));
    }

    assertFalse(resultSet.next());

    // Delete ETL directories
    createTPDirCheckerSet.removeETLDirsFromDbForMeasurementTable("DC_E_TEST", Utils.getSetMaxID(etlrep) + 1 + 0);
    resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");
    assertFalse(resultSet.next());
  }

  @Test
  public void checkThatCorrectBackupDirectoriesAreCreatedForDayTablesForDimEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, EVENT_E_TERM);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    final long transferActionId = createTPDirCheckerSet
        .createBackupDirsAndAddToDb(0, 0, "DC_E_TEST", prop, "day");

    // Check that the transferActionId has been updated correctly
    assertEquals(48, transferActionId);

    ResultSet resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");

    // Everything up to 10 should have an extra zero
    for (int i = 0; i < 10; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_0" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/backup/dwh_0" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_0" + i + "/" + EVENT_E_TERM.toLowerCase() + "/", resultSet
          .getString(1));
      assertEquals("/eniq/backup/dwh_0" + i + "/" + EVENT_E_TERM.toLowerCase() + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_0" + i + "/" + EVENT_E_TERM.toLowerCase() + "/" + "DC_E_TEST_day"
          + "/", resultSet
          .getString(1));
      assertEquals("/eniq/backup/dwh_0" + i + "/" + EVENT_E_TERM.toLowerCase() + "/" + "DC_E_TEST_day" + "/",
          resultSet
          .getString(2));
    }

    for (int i = 10; i < 16; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/backup/dwh_" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_" + i + "/" + EVENT_E_TERM.toLowerCase() + "/", resultSet.getString(1));
      assertEquals("/eniq/backup/dwh_" + i + "/" + EVENT_E_TERM.toLowerCase() + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals(
          "CreateDir_/eniq/backup/dwh_" + i + "/" + EVENT_E_TERM.toLowerCase() + "/" + "DC_E_TEST_day" + "/",
          resultSet
          .getString(1));
      assertEquals("/eniq/backup/dwh_" + i + "/" + EVENT_E_TERM.toLowerCase() + "/" + "DC_E_TEST_day" + "/",
          resultSet
          .getString(2));
    }

    assertFalse(resultSet.next());

    // Delete backup directories
    createTPDirCheckerSet.removeBackupDirs("DC_E_TEST", Utils.getSetMaxID(etlrep) + 1 + 0, "day");
    resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");
    assertFalse(resultSet.next());
  }

  @Test
  public void checkThatCorrectBaseBackupDirectoriesIsCreatedForDayTablesForDimEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, EVENT_E_TERM);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    final  long transferActionId = createTPDirCheckerSet.createBaseBackupDirAndAddToDb(0, prop, 0);

    // Check that the transferActionId has been updated correctly
    assertEquals(1, transferActionId);

    ResultSet resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");

    assertTrue(resultSet.next());
    assertEquals("CreateDir_/eniq/backup/", resultSet.getString(1));
    assertEquals("/eniq/backup/", resultSet.getString(2));

    assertFalse(resultSet.next());

    // Delete backup directories
    createTPDirCheckerSet.removeBaseBackupDirAndAddToDb(Utils.getSetMaxID(etlrep) + 1 + 0);
    resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");
    assertFalse(resultSet.next());
  }
  

  @Test
  public void checkThatCorrectEtlDirectoriesAreCreatedForTac() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, EVENT_E_TERM);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    final long transferActionId = createTPDirCheckerSet.createETLDirsAndAddToDbForReferenceTable(0, 0, "DIM_E_SGEH_TAC_CURRENT_DC", prop);

    // Check that the transferActionId has been updated correctly
    assertEquals(180, transferActionId);

    ResultSet resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DIM_E_SGEH_TAC_loader", resultSet.getString(1));
    assertEquals("${LOG_DIR}/iqloader/EVENT_E_TERM/DIM_E_SGEH_TAC/", resultSet.getString(2));
    
    assertTrue(resultSet.next());
    assertEquals("CreateDir_DIM_E_SGEH_TAC_rejected", resultSet.getString(1));
    assertEquals("${REJECTED_DIR}/EVENT_E_TERM/DIM_E_SGEH_TAC/", resultSet.getString(2));
    
    //CURRENT_DC directories
    // Everything up to 10 should have an extra zero
    for (int i = 0; i < 10; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/dim_e_sgeh_tac", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/dim_e_sgeh_tac/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/dim_e_sgeh_tac_raw", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/dim_e_sgeh_tac/raw/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/dim_e_sgeh_tac_joined", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/dim_e_sgeh_tac/joined/", resultSet.getString(2));
    }

    for (int i = 10; i < 16; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/dim_e_sgeh_tac", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/dim_e_sgeh_tac/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/dim_e_sgeh_tac_raw", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/dim_e_sgeh_tac/raw/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/dim_e_sgeh_tac_joined", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/dim_e_sgeh_tac/joined/", resultSet.getString(2));
    }
    
    assertTrue(resultSet.next());
    assertEquals("CreateDir_DIM_E_SGEH_TAC_RAW_loader", resultSet.getString(1));
    assertEquals("${LOG_DIR}/iqloader/EVENT_E_TERM/DIM_E_SGEH_TAC_RAW/", resultSet.getString(2));
    
    assertTrue(resultSet.next());
    assertEquals("CreateDir_DIM_E_SGEH_TAC_RAW_rejected", resultSet.getString(1));
    assertEquals("${REJECTED_DIR}/EVENT_E_TERM/DIM_E_SGEH_TAC_RAW/", resultSet.getString(2));
    
    //backup directories
    // Everything up to 10 should have an extra zero
    for (int i = 0; i < 10; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_0" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/backup/dwh_0" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_0" + i + "/" + EVENT_E_TERM.toLowerCase() + "/", resultSet
          .getString(1));
      assertEquals("/eniq/backup/dwh_0" + i + "/" + EVENT_E_TERM.toLowerCase() + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_0" + i + "/" + EVENT_E_TERM.toLowerCase() + "/" + "dim_e_sgeh_tac_plain"
          + "/", resultSet
          .getString(1));
      assertEquals("/eniq/backup/dwh_0" + i + "/" + EVENT_E_TERM.toLowerCase() + "/" + "dim_e_sgeh_tac_plain" + "/",
          resultSet
          .getString(2));
    }

    for (int i = 10; i < 16; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/backup/dwh_" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_" + i + "/" + EVENT_E_TERM.toLowerCase() + "/", resultSet.getString(1));
      assertEquals("/eniq/backup/dwh_" + i + "/" + EVENT_E_TERM.toLowerCase() + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals(
          "CreateDir_/eniq/backup/dwh_" + i + "/" + EVENT_E_TERM.toLowerCase() + "/" + "dim_e_sgeh_tac_plain" + "/",
          resultSet
          .getString(1));
      assertEquals("/eniq/backup/dwh_" + i + "/" + EVENT_E_TERM.toLowerCase() + "/" + "dim_e_sgeh_tac_plain" + "/",
          resultSet
          .getString(2));
    }
    
    //Plain table directories
    // Everything up to 10 should have an extra zero
    for (int i = 0; i < 10; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/DIM_E_SGEH_TAC_CURRENT_DC", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/DIM_E_SGEH_TAC_CURRENT_DC/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/DIM_E_SGEH_TAC_CURRENT_DC_raw", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/DIM_E_SGEH_TAC_CURRENT_DC/raw/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/0" + i + "/DIM_E_SGEH_TAC_CURRENT_DC_joined", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/0" + i + "/DIM_E_SGEH_TAC_CURRENT_DC/joined/", resultSet.getString(2));
    }

    for (int i = 10; i < 16; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/DIM_E_SGEH_TAC_CURRENT_DC", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/DIM_E_SGEH_TAC_CURRENT_DC/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/DIM_E_SGEH_TAC_CURRENT_DC_raw", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/DIM_E_SGEH_TAC_CURRENT_DC/raw/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/data/etldata_/" + i + "/DIM_E_SGEH_TAC_CURRENT_DC_joined", resultSet.getString(1));
      assertEquals("/eniq/data/etldata_/" + i + "/DIM_E_SGEH_TAC_CURRENT_DC/joined/", resultSet.getString(2));
    }

    assertFalse(resultSet.next());

    // Delete ETL directories
    createTPDirCheckerSet.removeETLDirsFromDbForReferenceTable("DIM_E_SGEH_TAC_CURRENT_DC", Utils.getSetMaxID(etlrep) + 1 + 0);
    resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");
    assertFalse(resultSet.next());
  }
}
