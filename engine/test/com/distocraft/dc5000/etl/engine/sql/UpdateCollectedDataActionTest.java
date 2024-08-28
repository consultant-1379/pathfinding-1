package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;

public class UpdateCollectedDataActionTest {

  private static RockFactory rockFact = null;

  private static final int MS_IN_DAY = 1000 * 60 * 60 * 24;

  @BeforeClass
  public static void init() throws Exception {

    StaticProperties.giveProperties(new Properties());
    try {

      Class.forName("org.hsqldb.jdbcDriver");
      rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
      final Statement stmt = rockFact.getConnection().createStatement();
      stmt.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(31), COLLECTION_SET_NAME VARCHAR(31), "
          + "DESCRIPTION VARCHAR(31), VERSION_NUMBER VARCHAR(31), ENABLED_FLAG VARCHAR(31), TYPE VARCHAR(31))");
      stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES ('1', 'set_name', 'description', '2', 'Y', 'type')");
      stmt.execute("CREATE TABLE Meta_databases (USERNAME varchar(30), VERSION_NUMBER varchar(32), TYPE_NAME varchar(15), CONNECTION_ID numeric(31), CONNECTION_NAME varchar(30), CONNECTION_STRING varchar(200), PASSWORD varchar(30), DESCRIPTION varchar(32000), DRIVER_NAME varchar(100), DB_LINK_NAME varchar(128))");
      stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 1, 'dwh', 'jdbc:hsqldb:mem:testdb', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver', null)");
      stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 2, 'dwh', 'jdbc:hsqldb:mem:testdb', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver', null)");
      stmt.execute("CREATE TABLE TEST_LOG_SESSION_COLLECTED_DATA_01 (TYPENAME varchar(255), SOURCE  varchar(255), DATE_ID date, DATETIME_ID timestamp, COLLECTED bit)");
      stmt.execute("CREATE TABLE TEST_LOG_SESSION_ADAPTER_01 (ROP_STARTTIME timestamp, ROP_ENDTIME timestamp, SOURCE varchar(255))");
      stmt.execute("INSERT INTO TEST_LOG_SESSION_ADAPTER_01 values ('2999-01-01 09:41:11', '2999-01-01 09:41:11', 'SGEH')");
      stmt.execute("INSERT INTO TEST_LOG_SESSION_ADAPTER_01 values ('2999-01-01 09:43:11', '2999-01-01 09:43:11', 'SGEH')");
      stmt.executeUpdate("INSERT INTO TEST_LOG_SESSION_COLLECTED_DATA_01 VALUES ('SGEH', 'SGSN01', '2999-01-01','2999-01-01 09:41:11', 0)");
      stmt.executeUpdate("INSERT INTO TEST_LOG_SESSION_COLLECTED_DATA_01 VALUES ('SGEH', 'SGSN01', '2999-01-01','2999-01-01 09:43:11', 0)");

      stmt.close();

      StaticProperties.giveProperties(new Properties());

    } catch (Exception e) {
      e.printStackTrace();
      fail("init() failed: " + e.getMessage());
    }
  }

  @AfterClass
  public static void cleanup() throws Exception {
    rockFact.getConnection().close();
  }

  @Test
  public void checkThatTestTableIsUpdated() throws Exception {
    PhysicalTableCache.testInit("TEST_LOG_SESSION_COLLECTED_DATA:PLAIN", "TEST_LOG_SESSION_COLLECTED_DATA_01", 0L,
        System.currentTimeMillis() + MS_IN_DAY, "ACTIVE");
    PhysicalTableCache.testInit("TEST_LOG_SESSION_ADAPTER:PLAIN", "TEST_LOG_SESSION_ADAPTER_01", 0L,
        System.currentTimeMillis() + MS_IN_DAY, "ACTIVE");

    final Meta_collections metaCollections = new Meta_collections(rockFact);

    final Properties where_cond = new Properties();

    where_cond.setProperty("sourceType", "TEST_LOG_SESSION_ADAPTER:PLAIN");
    where_cond.setProperty("targetType", "TEST_LOG_SESSION_COLLECTED_DATA:PLAIN");

    final ByteArrayOutputStream baoss = new ByteArrayOutputStream();
    where_cond.store(baoss, "");

    final Meta_transfer_actions metaTransferActions = new Meta_transfer_actions(rockFact);
    metaTransferActions.setWhere_clause(baoss.toString());
    metaTransferActions
        .setAction_contents("#foreach( $target in $targetTables ) "
            + "#foreach( $source in $sourceTables ) update $target trg set trg.COLLECTED = 1 where trg.DATETIME_ID in (select ROP_STARTTIME from $source); "
            + "#end commit; #end");

    final UpdateCollectedDataAction pa = new UpdateCollectedDataAction(new Meta_versions(rockFact), new Long(1),
        metaCollections, new Long(1), new Long(1), new Long(1), rockFact, new ConnectionPool(rockFact),
        metaTransferActions, Logger.getLogger("log"));

    pa.execute();

    final Connection c = rockFact.getConnection();
    Statement st = c.createStatement();
    final ResultSet rs = st.executeQuery("Select * from TEST_LOG_SESSION_COLLECTED_DATA_01 where COLLECTED = 1");
    int i = 0;
    while (rs.next()) {
      i++;
    }
    rs.close();
    st.close();

    if (i != 2) {
      fail("Should return two rows");
    }
  }

  @Test
  public void checkThatExceptionIsThrownWhenVelocityTemplateUndefined() {
    PhysicalTableCache.testInit("TARGET:1MIN", "TARGET_1MIN_01", 0L, 86400000L, "ACTIVE");

    final Meta_collections coll = new Meta_collections(rockFact);
    final Meta_transfer_actions act = new Meta_transfer_actions(rockFact);

    try {
      new UpdateCollectedDataAction(new Meta_versions(rockFact), new Long(1), coll, new Long(1), new Long(1), new Long(
          1), rockFact, new ConnectionPool(rockFact), act, Logger.getLogger("log"));
      fail("Should not have got here. Should have thrown an EngineMetaDataException");

    } catch (EngineMetaDataException me) {
      assertEquals("Velocity template not set", me.getMessage());
    }
  }

  @Test
  public void checkThatExceptionIsThrownWhenSourceTypeUndefined() throws Exception {
    PhysicalTableCache.testInit("TEST_LOG_SESSION_COLLECTED_DATA:PLAIN", "TEST_LOG_SESSION_COLLECTED_DATA_01", 0L,
        86400000L, "ACTIVE");

    final Meta_collections metaCollections = new Meta_collections(rockFact);

    final Properties where_cond = new Properties();

    final ByteArrayOutputStream baoss = new ByteArrayOutputStream();
    where_cond.store(baoss, "");

    final Meta_transfer_actions metaTransferActions = new Meta_transfer_actions(rockFact);
    metaTransferActions.setWhere_clause(baoss.toString());
    metaTransferActions
        .setAction_contents("#foreach( $source in $sourceTables ) update $source set COLLECTED = 1; commit; #end");

    try {
      final UpdateCollectedDataAction pa = new UpdateCollectedDataAction(new Meta_versions(rockFact), new Long(1),
          metaCollections, new Long(1), new Long(1), new Long(1), rockFact, new ConnectionPool(rockFact),
          metaTransferActions, Logger.getLogger("log"));

      pa.execute();
      fail("Should not have got here. Should have thrown a EngineException");
    } catch (EngineException me) {
      assertEquals("storage ID not found", me.getMessage());
    } catch (EngineMetaDataException me) {
      fail("Should not have got here. Should have thrown a EngineException");
    }

  }
}
