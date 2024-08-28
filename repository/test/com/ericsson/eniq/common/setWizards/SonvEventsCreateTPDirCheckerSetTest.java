/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
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

/**
 * @author epaujor
 * @since 2012
 * 
 */
public class SonvEventsCreateTPDirCheckerSetTest {

  private static final String TECH_PACK_NAME = "TEST_DC_E_SONV";

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
  public void checkThatCorrectEtlDirectoriesAreCreatedForReferenceTableForSonvEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    long transferActionId = createTPDirCheckerSet.createETLDirsAndAddToDbForReferenceTable(0, 0, "DC_E_TEST", prop);

    // Check that the transferActionId has been updated correctly
    assertEquals(66, transferActionId);

    ResultSet resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DC_E_TEST", resultSet.getString(1));
    assertEquals("${ETLDATA_DIR}/DC_E_TEST/", resultSet.getString(2));

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DC_E_TEST_raw", resultSet.getString(1));
    assertEquals("${ETLDATA_DIR}/DC_E_TEST/raw/", resultSet.getString(2));
    
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
    createTPDirCheckerSet.removeETLDirsFromDbForReferenceTable("DC_E_TEST", Utils.getSetMaxID(etlrep) + 1 + 0);

    resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");
    assertFalse(resultSet.next());
  }
  

  @Test
  public void checkThatCorrectEtlDirectoriesAreCreatedForMeasurementTableForSonvEvents() throws Exception {
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, Constants.ENIQ_EVENT, dwhrep, etlrep, 123, TECH_PACK_NAME);
    final Properties prop = new Properties();
    prop.put("permission", "750");
    long transferActionId = createTPDirCheckerSet.createETLDirsAndAddToDbForMeasurementTable(0, 0, "DC_E_TEST", "raw", prop);

    // Check that the transferActionId has been updated correctly
    assertEquals(82, transferActionId);

    ResultSet resultSet = CreateRockFactoryHelper
        .executeQueryOnEtlRep("SELECT TRANSFER_ACTION_NAME, WHERE_CLAUSE_01 FROM META_TRANSFER_ACTIONS");

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DC_E_TEST", resultSet.getString(1));
    assertEquals("${ETLDATA_DIR}/DC_E_TEST/", resultSet.getString(2));

    assertTrue(resultSet.next());
    assertEquals("CreateDir_DC_E_TEST_raw", resultSet.getString(1));
    assertEquals("${ETLDATA_DIR}/DC_E_TEST/raw/", resultSet.getString(2));
    
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
}
