package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;



public class CreateIDiskmanagerSetFactoryTest {

  private static final String setVersion = "((12))";

  private static String setName = null;

  private static RockFactory etlrep = null;

  private static final int techPackID = 123;

  private final String objType = "reference";
  boolean isScheduling = true;

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
  public void checkThatCreateIDiskmanagerSetIsCreated() throws Exception {
    setName = "INTF_DIM_E_RAN_TEST";

    final CreateIDiskmanagerSet createIDiskmanagerSet = CreateIDiskmanagerSetFactory.createIDiskmanagerSet(objType,
        setVersion, etlrep, techPackID, setName, null);

    assertTrue(createIDiskmanagerSet instanceof StatsCreateIDiskmanagerSet);
    assertEquals("${OSS}", createIDiskmanagerSet.getIBaseDirectory());
  }

  @Test
  public void checkThatCreateIDiskmanagerSetIsCreatedWhenTechPackNameIsNULL() throws Exception {
    setName = null;


    final CreateIDiskmanagerSet createIDiskmanagerSet = CreateIDiskmanagerSetFactory.createIDiskmanagerSet(objType,
        setVersion, etlrep, techPackID, setName, null);

    assertTrue(createIDiskmanagerSet instanceof StatsCreateIDiskmanagerSet);
    assertEquals("${OSS}", createIDiskmanagerSet.getIBaseDirectory());
  }

  @Test
  public void checkThatEventsCreateIDiskmanagerSetIsCreatedWhenSetNameContainsDIM_E_SGEH() throws Exception {
    setName = "INTF_DIM_E_SGEH_TEST";

    final CreateIDiskmanagerSet createIDiskmanagerSet = CreateIDiskmanagerSetFactory.createIDiskmanagerSet(objType,
        setVersion, etlrep, techPackID, setName, null);

    assertTrue(createIDiskmanagerSet instanceof EventsCreateIDiskmanagerSet);
    assertEquals("eniq_events_topology", createIDiskmanagerSet.getIBaseDirectory());
  }
}
