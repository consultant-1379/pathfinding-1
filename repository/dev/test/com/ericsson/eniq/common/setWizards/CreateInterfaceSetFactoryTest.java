package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;

public class CreateInterfaceSetFactoryTest {

  private static final String setVersion = "((12))";

  private static String setName = null;

  private static RockFactory etlrep = null;
  
  private static RockFactory dwhrep = null;

  private static final int techPackID = 123;

  private final String objType = "reference";

  private final String templateDir = null;

  private final String adapterType = null;

  String connectionID = null;

  String dirName = null;

  String adapterName = null;

  String interfaceVersion = null;

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
  public void checkThatCreateInterfaceSetIsCreated() throws Exception {
    setName = "INTF_DIM_E_RAN_TEST";

    final CreateInterfaceSet createInterfaceSet = CreateInterfaceSetFactory.createInterfaceSet(objType, templateDir,
        setVersion, setVersion, dwhrep, etlrep, techPackID, setName, interfaceVersion, adapterType, adapterName,
        dirName, connectionID, null);

    assertTrue(createInterfaceSet instanceof StatsCreateInterfaceSet);
  }

  @Test
  public void checkThatCreateInterfaceSetIsCreatedWhenTechPackNameIsNULL() throws Exception {
    setName = null;

    final CreateInterfaceSet createInterfaceSet = CreateInterfaceSetFactory.createInterfaceSet(objType, templateDir,
        setVersion, setVersion, dwhrep, etlrep, techPackID, setName, interfaceVersion, adapterType, adapterName,
        dirName, connectionID, null);
    assertTrue(createInterfaceSet instanceof StatsCreateInterfaceSet);    
  }

  @Test
  public void checkThatEventsCreateInterfaceSetIsCreatedWhenSetNameContainsDIM_E_SGEH() throws Exception {
    setName = "INTF_DIM_E_SGEH_TEST";

    final CreateInterfaceSet createInterfaceSet = CreateInterfaceSetFactory.createInterfaceSet(objType, templateDir,
        setVersion, setVersion, dwhrep, etlrep, techPackID, setName, interfaceVersion, adapterType, adapterName,
        dirName, connectionID, null);
    assertTrue(createInterfaceSet instanceof EventsCreateInterfaceSet);
  }
}
