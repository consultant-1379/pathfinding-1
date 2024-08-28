package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.Utils;

public class EventsCreateTPDirCheckerSetTest {

  private static final String TECH_PACK_NAME = "testTechpackName";

  private static String versionDir = "1";

  private static final String setVersion = "((12))";

  private static String setName = "Loader";

  private static final String versionId = setName + ":" + setVersion;

  private static RockFactory dwhrep = null;

  private static RockFactory etlrep = null;

  private static List<String> testList = Arrays.asList("DIM_E_GSN","DIM_E_IMSI_IMEI","DIM_E_IMSI_MSISDN","DIM_E_LTE");
  
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
  public void checkThatBaseDirectoryIsCorrectForEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);

    assertEquals("/eniq/data/etldata_", createTPDirCheckerSet.getBaseEtlDirectory());
  }

  @Test
  public void checkVersionOfDirectoryNumberIsCorrect() throws Exception {
    Field strippedVer = EventsCreateTPDirCheckerSet.class.getDeclaredField("versionDir");
    strippedVer.setAccessible(true);
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);

    assertEquals("/eniq/data/etldata_", createTPDirCheckerSet.getBaseEtlDirectory());
    assertEquals(versionDir, strippedVer.get(createTPDirCheckerSet));
  }

  @Test
  public void checkThatCorrectEtlDirectoriesAreCreatedForMeasurementTableForEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    long transferActionId = createTPDirCheckerSet.createETLDirsAndAddToDbForMeasurementTable(0, 0, "DC_E_TEST", "raw", prop);

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
  public void checkThatCorrectEtlDirectoriesAreCreatedForReferenceTableForEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    long transferActionId = createTPDirCheckerSet.createETLDirsAndAddToDbForReferenceTable(0, 0, "DC_E_TEST", prop);

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
  public void checkThatCorrectBackupDirectoriesAreCreatedForDayTablesForEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    long transferActionId = createTPDirCheckerSet
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
      assertEquals("CreateDir_/eniq/backup/dwh_0" + i + "/" + TECH_PACK_NAME.toLowerCase() + "/", resultSet
          .getString(1));
      assertEquals("/eniq/backup/dwh_0" + i + "/" + TECH_PACK_NAME.toLowerCase() + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_0" + i + "/" + TECH_PACK_NAME.toLowerCase() + "/" + "DC_E_TEST_day"
          + "/", resultSet
          .getString(1));
      assertEquals("/eniq/backup/dwh_0" + i + "/" + TECH_PACK_NAME.toLowerCase() + "/" + "DC_E_TEST_day" + "/",
          resultSet
          .getString(2));
    }

    for (int i = 10; i < 16; i++) {
      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_" + i + "/", resultSet.getString(1));
      assertEquals("/eniq/backup/dwh_" + i + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals("CreateDir_/eniq/backup/dwh_" + i + "/" + TECH_PACK_NAME.toLowerCase() + "/", resultSet.getString(1));
      assertEquals("/eniq/backup/dwh_" + i + "/" + TECH_PACK_NAME.toLowerCase() + "/", resultSet.getString(2));

      assertTrue(resultSet.next());
      assertEquals(
          "CreateDir_/eniq/backup/dwh_" + i + "/" + TECH_PACK_NAME.toLowerCase() + "/" + "DC_E_TEST_day" + "/",
          resultSet
          .getString(1));
      assertEquals("/eniq/backup/dwh_" + i + "/" + TECH_PACK_NAME.toLowerCase() + "/" + "DC_E_TEST_day" + "/",
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
  public void checkThatCorrectBaseBackupDirectoriesIsCreatedForDayTablesForEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    long transferActionId = createTPDirCheckerSet.createBaseBackupDirAndAddToDb(0, prop, 0);

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
  public void checkNameIsInListNameFound() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);

    assertTrue(createTPDirCheckerSet.nameIsInList("DIM_E_GSN_TEST1", testList));
    assertTrue(createTPDirCheckerSet.nameIsInList("DIM_E_IMSI_MSISDN_TEST2", testList));
  }

  @Test
  public void checkNameIsInListNameNotFound() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);

    assertFalse(createTPDirCheckerSet.nameIsInList("DIM_E_XYZ_TEST1", testList));
    assertFalse(createTPDirCheckerSet.nameIsInList(null, testList));
  }

  @Test
  public void needToBackUpRawNameFound() throws Exception {
	    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
	            setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);

        assertTrue(createTPDirCheckerSet.needToBackUpRaw("DC_E_LTE_SONV_CM_XYZ"));
  }
  
  @Test
  public void needToBackUpRawNameNotFound() throws Exception {
	    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
	            setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);

        assertFalse(createTPDirCheckerSet.needToBackUpRaw("DIM_E_XYZ_TEST1"));
        assertFalse(createTPDirCheckerSet.needToBackUpRaw("DIM_E_IMSI_IMEI"));
  }


  @Test
  public void needToBackUpPlainNameFound() throws Exception {
	    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
	            setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);

        assertTrue(createTPDirCheckerSet.needToBackUpPlain("DC_E_LTE_SONV_CM_XYZ"));
        assertTrue(createTPDirCheckerSet.needToBackUpPlain("DIM_E_IMSI_IMEI_ABCD"));
		assertTrue(createTPDirCheckerSet.needToBackUpPlain("DIM_Z_SGEH_HIER321_CELL"));
        assertTrue(createTPDirCheckerSet.needToBackUpPlain("DIM_Z_SGEH_HIER3_RNCID"));
  }
  
  @Test
  public void needToBackUpPlainNameNotFound() throws Exception {
	    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
	            setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);

        assertFalse(createTPDirCheckerSet.needToBackUpPlain("DIM_E_XYZ_TEST1"));
		assertFalse(createTPDirCheckerSet.needToBackUpPlain("DIM_Z_XYZ_TEST1"));
  }
}
