package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.ericsson.eniq.common.Utils;

public class StatsCreateTPDirCheckerSetTest {

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
  public void checkThatBaseDirectoryIsCorrectForStats() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, "PM", dwhrep, etlrep, 123, "topologyName");

    assertEquals("${ETLDATA_DIR}", createTPDirCheckerSet.getBaseEtlDirectory());
  }

  @Test
  public void checkThatCorrectEtlDirectoriesAreCreatedAndDeletedForMeasurementTableForStats() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, "PM", dwhrep, etlrep, 123, "topologyName");
    final Properties prop = new Properties();
    prop.put("permission", "750");
    // Create ETL directories
    createTPDirCheckerSet.createETLDirsAndAddToDbForMeasurementTable(0, 0, "DC_E_TEST", "raw", prop);

    ResultSet resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DC_E_TEST", resultSet.getString(1));
    assertEquals("${ETLDATA_DIR}/DC_E_TEST/", resultSet.getString(2));

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DC_E_TEST_raw", resultSet.getString(1));
    assertEquals("${ETLDATA_DIR}/DC_E_TEST/raw/", resultSet.getString(2));

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DC_E_TEST_joined", resultSet.getString(1));
    assertEquals("${ETLDATA_DIR}/DC_E_TEST/joined/", resultSet.getString(2));

    assertFalse(resultSet.next());

    // Delete ETL directories
    createTPDirCheckerSet.removeETLDirsFromDbForMeasurementTable("DC_E_TEST", Utils.getSetMaxID(etlrep) + 1 + 0);
    resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");
    assertFalse(resultSet.next());
  }

  @Test
  public void checkThatCorrectEtlDirectoriesAreCreatedAndDeletedForReferenceTableForStats() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, "PM", dwhrep, etlrep, 123, "topologyName");
    final Properties prop = new Properties();
    prop.put("permission", "750");
    // Create ETL directories
    createTPDirCheckerSet.createETLDirsAndAddToDbForReferenceTable(0, 0, "DC_E_TEST", prop);

    ResultSet resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DC_E_TEST", resultSet.getString(1));
    assertEquals("${ETLDATA_DIR}/DC_E_TEST/", resultSet.getString(2));

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DC_E_TEST_raw", resultSet.getString(1));
    assertEquals("${ETLDATA_DIR}/DC_E_TEST/raw/", resultSet.getString(2));

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DC_E_TEST_joined", resultSet.getString(1));
    assertEquals("${ETLDATA_DIR}/DC_E_TEST/joined/", resultSet.getString(2));

    assertFalse(resultSet.next());

    // Delete ETL directories
    createTPDirCheckerSet.removeETLDirsFromDbForMeasurementTable("DC_E_TEST", Utils.getSetMaxID(etlrep) + 1 + 0);
    resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");
    assertFalse(resultSet.next());
  }
}
