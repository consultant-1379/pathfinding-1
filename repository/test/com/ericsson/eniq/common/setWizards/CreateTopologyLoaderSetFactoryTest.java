package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;

public class CreateTopologyLoaderSetFactoryTest {

  private static final String LOADER_SET = "Loader_Set";

  private static final String TEMPLATE_DIR = "5.2";

  private static final String setVersion = "((12))";

  private static String techPackName = null;

  private static final String versionId = techPackName + ":" + setVersion;

  private static RockFactory dwhrep = null;

  private static RockFactory etlrep = null;

  private static final int techPackID = 123;

  boolean isScheduling = true;

  @BeforeClass
  public static void setUp() throws Exception {
    StaticProperties.giveProperties(new Properties());

    etlrep = CreateRockFactoryHelper.createEtlRepRockFactory();
    dwhrep = CreateRockFactoryHelper.createDwhRepFactory();
  }

  @AfterClass
  public static void tearDownAfterClass() throws SQLException {
    CreateRockFactoryHelper.cleanUpRockFactory();
    etlrep = null;
    dwhrep = null;
  }

  @Test
  public void checkThatCreateTopologyLoaderSetIsCreated() throws SQLException {
    techPackName = "DC_E_BSS";

    final CreateTopologyLoaderSet createTopologyLoaderSet = CreateTopologyLoaderSetFactory.createTopologyLoaderSet(
        TEMPLATE_DIR, LOADER_SET, setVersion, versionId, dwhrep, etlrep, techPackID, techPackName, isScheduling);

    assertTrue(createTopologyLoaderSet instanceof StatsCreateTopologyLoaderSet);
    assertEquals("${ETLDATA_DIR}/test/raw/", createTopologyLoaderSet.getTopologyDirectory("test"));
  }

  @Test
  public void checkThatCreateTopologyLoaderSetIsCreatedWhenTechPackNameIsNULL() throws SQLException {
    techPackName = null;

    final CreateTopologyLoaderSet createTopologyLoaderSet = CreateTopologyLoaderSetFactory.createTopologyLoaderSet(
        TEMPLATE_DIR, LOADER_SET, setVersion, versionId, dwhrep, etlrep, techPackID, techPackName, isScheduling);

    assertTrue(createTopologyLoaderSet instanceof StatsCreateTopologyLoaderSet);
    assertEquals("${ETLDATA_DIR}/test123/raw/", createTopologyLoaderSet.getTopologyDirectory("test123"));
  }

  @Test
  public void checkThatEventsCreateTopologyLoaderSetIsCreatedWhenTechPackNameStartsWithEVENT_E() throws SQLException {
    techPackName = "DIM_E_SGEH";

    final CreateTopologyLoaderSet createTopologyLoaderSet = CreateTopologyLoaderSetFactory.createTopologyLoaderSet(
        TEMPLATE_DIR, LOADER_SET, setVersion, versionId, dwhrep, etlrep, techPackID, techPackName, isScheduling);

    assertTrue(createTopologyLoaderSet instanceof EventsCreateTopologyLoaderSet);
    assertEquals("/eniq/data/etldata_/00/test123/raw/", createTopologyLoaderSet.getTopologyDirectory("test123"));
  }
}
