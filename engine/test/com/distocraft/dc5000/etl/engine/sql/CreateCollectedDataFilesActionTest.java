package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;

import com.ericsson.eniq.common.testutilities.DirectoryHelper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.sql.Date;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.testutilities.BaseUnitTestX;

public class CreateCollectedDataFilesActionTest extends BaseUnitTestX {

  private static RockFactory rockFact = null;

  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "CreateCollectedDataFilesActionTest");

  private final static String PATH_TO_COLLECTED_DATA_FILES = TMP_DIR.getPath();

  private final static Date dateId_1 = new Date(System.currentTimeMillis());
  private final static String dateId = new Date(System.currentTimeMillis()) + " 00:00:00";
  
  @BeforeClass
  public static void init() throws Exception {
    DirectoryHelper.mkdirs(TMP_DIR);
    StaticProperties.giveProperties(new Properties());
    try {
      Class.forName("org.hsqldb.jdbcDriver");
      rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
      final Statement stmt = rockFact.getConnection().createStatement();
      try {
    	  stmt.execute("drop table Meta_collection_sets");
      }
      catch(Exception e){}
      stmt
          .execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(31), COLLECTION_SET_NAME VARCHAR(31), "
              + "DESCRIPTION VARCHAR(31), VERSION_NUMBER VARCHAR(31), ENABLED_FLAG VARCHAR(31), TYPE VARCHAR(31))");
      stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES ('1', 'set_name', 'description', '2', 'Y', 'type')");
      try {
    	  stmt.execute("drop table META_DATABASES");
      }
      catch(Exception e){}
      stmt
          .execute("CREATE TABLE Meta_databases (USERNAME varchar(30), VERSION_NUMBER varchar(32), TYPE_NAME varchar(15), CONNECTION_ID numeric(31), CONNECTION_NAME varchar(30), CONNECTION_STRING varchar(200), PASSWORD varchar(30), DESCRIPTION varchar(32000), DRIVER_NAME varchar(100), DB_LINK_NAME varchar(128))");
      stmt
          .executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 1, 'dwh', 'jdbc:hsqldb:mem:testdb', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver', null)");
      stmt
          .executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 2, 'dwh', 'jdbc:hsqldb:mem:testdb', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver', null)");
      try {
    	  stmt.execute("drop table TEMP123");
      }
      catch(Exception e){}
      
      System.out.println("dateId is :" +dateId);
      stmt.execute("CREATE TABLE TEMP123 (ROP_STARTTIME timestamp, Source varchar(20), TYPENAME varchar(20))");
      stmt.executeUpdate("insert into TEMP123 values('" + dateId + "', 'SGEH', 'SGEH')");

      stmt.close();
      StaticProperties.giveProperties(new Properties());

    } catch (Exception e) {
      e.printStackTrace();
      fail("init() failed: " + e.getMessage());
    }
    System.setProperty("PATH_TO_COLLECTED_DATA_FILES", PATH_TO_COLLECTED_DATA_FILES + File.separator);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    DirectoryHelper.delete(TMP_DIR);
	  final Statement stmt = rockFact.getConnection().createStatement();
	  stmt.execute("drop table Meta_collection_sets");  
    rockFact.getConnection().close();

    System.clearProperty("PATH_TO_COLLECTED_DATA_FILES");
  }

  @Test
  public void checkThatCollectedDataFileIsCreated() throws Exception {
    boolean isFileCreated = false;

    PhysicalTableCache.testInit("TEST_LOG_SESSION_COLLECTED_DATA:PLAIN", "TEST_LOG_SESSION_COLLECTED_DATA_01", 0L,
        86400000L, "ACTIVE");

    final Meta_collections metaCollections = new Meta_collections(rockFact);

    final Properties where_cond = new Properties();

    where_cond.setProperty("sourceType", "TEST_LOG_SESSION_COLLECTED_DATA:PLAIN");

    final ByteArrayOutputStream baoss = new ByteArrayOutputStream();
    where_cond.store(baoss, "");

    final Meta_transfer_actions metaTransferActions = new Meta_transfer_actions(rockFact);
    metaTransferActions.setWhere_clause(baoss.toString());
    metaTransferActions
        .setAction_contents("#set ($listOfHours = [\"00\", \"01\", \"02\", \"03\", \"04\", \"05\", \"06\", \"07\", \"08\", \"09\", \"10\", \"11\", \"12\", \"13\", \"14\", \"15\", \"16\", \"17\", \"18\", \"19\", \"20\", \"21\", \"22\", \"23\"])"
            + "#set ($listOfMinutes = [\"00\", \"01\", \"02\", \"03\", \"04\", \"05\", \"06\", \"07\", \"08\", \"09\", \"10\", \"11\", \"12\", \"13\", \"14\", \"15\", \"16\", \"17\", \"18\", \"19\", \"20\", \"21\", \"22\", \"23\", \"24\", \"25\", \"26\", \"27\", \"28\", \"29\", \"30\", \"31\", \"32\", \"33\", \"34\", \"35\", \"36\", \"37\", \"38\", \"39\", \"40\", \"41\", \"42\", \"43\", \"44\", \"45\", \"46\", \"47\", \"48\", \"49\", \"50\", \"51\", \"52\", \"53\", \"54\", \"55\", \"56\", \"57\", \"58\", \"59\"])"
            + "#foreach( $hour in $listOfHours )#foreach( $minute in $listOfMinutes )$typeName $source $date $date $hour:$minute 0 "
            + "#end#end");

    isFileCreated = checkIfCollectedDataFileExists();
    assertFalse(isFileCreated);

    final StubbedCreateCollectedDataFilesAction pa = new StubbedCreateCollectedDataFilesAction(new Meta_versions(
        rockFact),
        new Long(1), metaCollections, new Long(1), new Long(1), new Long(1), rockFact, new ConnectionPool(rockFact),
        metaTransferActions, Logger.getLogger("log"));

    pa.getSourcesNotInLOG_COLLECTED_DATA = "select ROP_STARTTIME, Source, TYPENAME from TEMP123 where ROP_STARTTIME >=?";
    pa.execute();

    isFileCreated = checkIfCollectedDataFileExists();
    assertTrue(isFileCreated);
  }

  @Test
  public void checkThatExceptionIsThrownWhenVelocityTemplateUndefined() {
    final Meta_collections coll = new Meta_collections(rockFact);
    final Meta_transfer_actions act = new Meta_transfer_actions(rockFact);

    try {
      new CreateCollectedDataFilesAction(new Meta_versions(rockFact), new Long(1), coll, new Long(1), new Long(1),
          new Long(1), rockFact, new ConnectionPool(rockFact), act, Logger.getLogger("log"));
      fail("Should not have got here. Should have thrown an EngineMetaDataException");

    } catch (EngineMetaDataException me) {
      assertEquals("Velocity template not set", me.getMessage());
    }
  }

  private class StubbedCreateCollectedDataFilesAction extends CreateCollectedDataFilesAction {

    public StubbedCreateCollectedDataFilesAction(Meta_versions version, Long collectionSetId,
        Meta_collections collection, Long transferActionId, Long transferBatchId, Long connectionId,
        RockFactory etlRepRockFact, ConnectionPool connectionPool, Meta_transfer_actions trActions, Logger parentLogger)
        throws EngineMetaDataException {
      super(version, collectionSetId, collection, transferActionId, transferBatchId, connectionId, etlRepRockFact,
          connectionPool, trActions, parentLogger);
    }
  }

  private boolean checkIfCollectedDataFileExists() {
    boolean isFileCreated = false;
    File dir = new File(PATH_TO_COLLECTED_DATA_FILES + File.separator);
    System.out.println("dir is :" + dir);
    for (File file : dir.listFiles()) {
        if (file.getName().startsWith("COLLECTED_DATA") && file.getName().endsWith(dateId_1.toString())) {
    	isFileCreated = true;
        file.delete();
        break;
      }
    }
    return isFileCreated;
  }
}
