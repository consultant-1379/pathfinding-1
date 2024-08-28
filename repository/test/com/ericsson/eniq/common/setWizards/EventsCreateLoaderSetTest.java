package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import com.ericsson.eniq.common.Utils;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;

public class EventsCreateLoaderSetTest {

  private final boolean isScheduling = true;

  private final int techpackId = 123;

  private static final String LOADER_SET_NAME = "Loader_Set";

  private static final String TEMPLATE_DIR = "5.2";

  private static final String setVersion = "((12))";

  private static String techPackName = null;

  private static final String versionId = techPackName + ":" + setVersion;

  private static RockFactory dwhrep = null;

  private static RockFactory etlrep = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
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
  public void checkThatSQLTemplateIsNotEmptyWhenTechPackNameIsNULL() throws Exception {
    techPackName = null;

    final EventsCreateLoaderSet createLoaderSet = new EventsCreateLoaderSet(TEMPLATE_DIR, LOADER_SET_NAME, setVersion,
        versionId, dwhrep, etlrep, techpackId, techPackName, isScheduling);

    assertFalse(createLoaderSet.isSqlTemplateGoingToBeEmpty());
  }

  @Test
  public void checkThatSQLTemplateIsNOTEmptyWhenTechPackNameIsNOTEVENT_E_SGEH() throws Exception {
    techPackName = "TEST";

    final EventsCreateLoaderSet createLoaderSet = new EventsCreateLoaderSet(TEMPLATE_DIR, LOADER_SET_NAME, setVersion,
        versionId, dwhrep, etlrep, techpackId, techPackName, isScheduling);

    assertFalse(createLoaderSet.isSqlTemplateGoingToBeEmpty());
  }

  @Test
  public void checkThatSQLTemplateIsEmptyWhenTechPackNameIsEVENT_E_SGEH() throws Exception {
    techPackName = "EVENT_E_SGEH";

    final EventsCreateLoaderSet createLoaderSet = new EventsCreateLoaderSet(TEMPLATE_DIR, LOADER_SET_NAME, setVersion,
        versionId, dwhrep, etlrep, techpackId, techPackName, isScheduling);

    assertTrue(createLoaderSet.isSqlTemplateGoingToBeEmpty());
  }

  @Test
  public void checkThatWhereClauseIsCreatedForMetaTransferActions() throws Exception {
    final String dateformat = "dateformat=yyyy-MM-dd";
    final String taildir = "taildir=raw";
    final String techpack = "techpack=EVENT_E_SGEH";
    final String tablename = "tablename=MT1";
    // versiondir should be equal to default value of 1
    final String versiondir = "versiondir=" + Utils.getEventsProperty("versiondir", "1");

    techPackName = "EVENT_E_SGEH";

    final EventsCreateLoaderSet createLoaderSet = new EventsCreateLoaderSet(TEMPLATE_DIR, LOADER_SET_NAME, setVersion,
        versionId, dwhrep, etlrep, techpackId, techPackName, isScheduling);

    final String whereClause = createLoaderSet.createWhereClauseForMetaTransferActions("MT1", "raw");

    assertTrue("\nwhereClause should contain dateformat = \n" + dateformat
        + ". \n\nwhereClause actually contained = \n" + whereClause, whereClause.contains(dateformat));
    assertTrue("\nwhereClause should contain taildir = \n" + taildir + ". \n\nwhereClause actually contained = \n"
        + whereClause, whereClause.contains(taildir));
    assertTrue("\nwhereClause should contain techpack = \n" + techpack + ". \n\nwhereClause actually contained = \n"
        + whereClause, whereClause.contains(techpack));
    assertTrue("\nwhereClause should contain dateformat = \n" + dateformat
        + ". \n\nwhereClause actually contained = \n" + whereClause, whereClause.contains(dateformat));
    assertTrue("\nwhereClause should contain tablename = \n" + tablename + ". \n\nwhereClause actually contained = \n"
        + whereClause, whereClause.contains(tablename));
    assertTrue("\nwhereClause should contain versiondir = \n" + versiondir
        + ". \n\nwhereClause actually contained = \n" + whereClause, whereClause.contains(versiondir));
  }

  @Test
  public void checkThatGetLoaderActionTypeNameIsCorrect() throws SQLException {
    techPackName = "EVENT_E_SGEH";

    final EventsCreateLoaderSet createLoaderSet = new EventsCreateLoaderSet(TEMPLATE_DIR, LOADER_SET_NAME, setVersion,
        versionId, dwhrep, etlrep, techpackId, techPackName, isScheduling);
    assertTrue(createLoaderSet.getLoaderActionTypeName().equals("EventLoader"));
  }
}
