package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;



public class CreateIDirCheckSetFactoryTest {

  private static final String setVersion = "((12))";

  private static String setName = null;

  private static RockFactory etlrep = null;

  private static final int techPackID = 123;

  private final String objType = "reference";
  boolean isScheduling = true;  private final String parsertype = null;

  @BeforeClass
  public static void setUp() throws Exception {
    StaticProperties.giveProperties(new Properties());
    etlrep = CreateRockFactoryHelper.createEtlRepRockFactory();
  }

  @AfterClass
  public static void tearDownAfterClass() throws SQLException {
    CreateRockFactoryHelper.cleanUpRockFactory();
    etlrep = null;
  }

  @Test
  public void checkThatCreateIDirCheckerSetIsCreated() throws Exception {
    setName = "INTF_DIM_E_RAN_TEST";

    final CreateIDirCheckerSet createIDirCheckerSet = CreateIDirCheckerSetFactory.createIDirCheckerSet(objType,
        setVersion, etlrep, techPackID, setName, null,parsertype);

    assertTrue(createIDirCheckerSet instanceof StatsCreateIDirCheckerSet);
    assertEquals("${OSS}", createIDirCheckerSet.getIBaseDirectory());
  }

  @Test
  public void checkThatCreateIDirCheckSetIsCreatedWhenTechPackNameIsNULL() throws Exception {
    setName = null;


    final CreateIDirCheckerSet createIDirCheckerSet = CreateIDirCheckerSetFactory.createIDirCheckerSet(objType,
        setVersion, etlrep, techPackID, setName, null, parsertype);

    assertTrue(createIDirCheckerSet instanceof StatsCreateIDirCheckerSet);
    assertEquals("${OSS}", createIDirCheckerSet.getIBaseDirectory());
  }

  @Test
  public void checkThatEventsCreateIDirCheckSetIsCreatedWhenSetNameContainsDIM_E_SGEH() throws Exception {
    setName = "INTF_DIM_E_SGEH_TEST";

    final CreateIDirCheckerSet createIDirCheckerSet = CreateIDirCheckerSetFactory.createIDirCheckerSet(objType,
        setVersion, etlrep, techPackID, setName, null ,parsertype);

    assertTrue(createIDirCheckerSet instanceof EventsCreateIDirCheckerSet);
    assertEquals("eniq_events_topology", createIDirCheckerSet.getIBaseDirectory());
  }
}
