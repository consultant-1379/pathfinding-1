package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.ericsson.eniq.common.Constants;

public class CreateTPDirCheckerSetFactoryTest {

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
  public void checkThatStatsCreateTPDirCheckerSetIsCreated() throws Exception {
    final String type = "PM";
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, type, dwhrep, etlrep, 123, "topologyName");

    assertTrue(createTPDirCheckerSet instanceof StatsCreateTPDirCheckerSet);
  }

  @Test
  public void checkThatStatsCreateTPDirCheckerSetIsCreatedWhenTypeIsNULL() throws Exception {
    final String type = null;
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, type, dwhrep, etlrep, 123, "topologyName");

    assertTrue(createTPDirCheckerSet instanceof StatsCreateTPDirCheckerSet);
  }

  @Test
  public void checkThatEventsCreateTPDirCheckerSetIsCreatedWhenTechPackNameStartsWithEVENT_E() throws Exception {
    final String type = Constants.ENIQ_EVENT;
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, type, dwhrep, etlrep, 123, "topologyName");

    assertTrue(createTPDirCheckerSet instanceof EventsCreateTPDirCheckerSet);
    //Check that it is not a SONV techpack
    assertFalse(createTPDirCheckerSet instanceof SonvEventsCreateTPDirCheckerSet);
  }
  
  @Test
  public void checkThatSonvTPDirCheckerSetIsCreatedWhenTpTypeIsENIQ_EVENTAndTpNameContainsSONV() throws Exception {
    final String type = Constants.ENIQ_EVENT;
    final CreateTPDirCheckerSet createTPDirCheckerSet = CreateTPDirCheckerSetFactory.createTPDirCheckerSet(setName,
        setVersion, versionId, type, dwhrep, etlrep, 123, "DC_E_LTE_SONV");

    assertTrue(createTPDirCheckerSet instanceof SonvEventsCreateTPDirCheckerSet);
  }
}
