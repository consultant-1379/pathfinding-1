package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;

public class CreateLoaderSetFactoryTest {

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
  public void checkThatStatsCreateLoaderSetIsCreated() throws SQLException {
    techPackName = "DC_E_BSS";

    final CreateLoaderSet createLoaderSet = CreateLoaderSetFactory.createLoaderSet(TEMPLATE_DIR, LOADER_SET,
        setVersion, versionId, dwhrep, etlrep, techPackID, techPackName, isScheduling);

    assertTrue(createLoaderSet instanceof StatsCreateLoaderSet);
  }

  @Test
  public void checkThatStatsCreateLoaderSetIsCreatedWhenTechPackNameIsNULL() throws SQLException {
    techPackName = null;

    final CreateLoaderSet createLoaderSet = CreateLoaderSetFactory.createLoaderSet(TEMPLATE_DIR, LOADER_SET,
        setVersion, versionId, dwhrep, etlrep, techPackID, techPackName, isScheduling);

    assertTrue(createLoaderSet instanceof StatsCreateLoaderSet);
  }

  @Test
  public void checkThatEventsCreateLoaderSetIsCreatedWhenTechPackNameStartsWithEVENT_E() throws SQLException {
    techPackName = "EVENT_E_SGEH_TEST";

    final CreateLoaderSet createLoaderSet = CreateLoaderSetFactory.createLoaderSet(TEMPLATE_DIR, LOADER_SET,
        setVersion, versionId, dwhrep, etlrep, techPackID, techPackName, isScheduling);

    assertTrue(createLoaderSet instanceof EventsCreateLoaderSet);
  }
}
