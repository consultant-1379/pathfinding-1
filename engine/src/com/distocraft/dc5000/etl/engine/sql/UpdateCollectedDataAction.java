/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.sql;

import java.io.StringWriter;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.VelocityPool;

/**
 * Updates the COLLECTED column in the tables, LOG_SESSION_COLLECTED_DATA. The
 * COLLECTED column update is done based a lookup on LOG_SESSION_ADAPTER.
 * 
 * @author epaujor
 * 
 */
public class UpdateCollectedDataAction extends SQLOperation {

  private static final int MS_IN_DAY = 1000 * 60 * 60 * 24;

  private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

  final SimpleDateFormat dbUTCDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault());

  protected Logger log;

  /**
   * Updates the COLLECTED column in the tables, LOG_SESSION_COLLECTED_DATA so
   * that it is known if files were collected or not. The information in this
   * table will be used by the adminUI to display files COLLECTED/NOT COLLECTED
   * from the nodes.
   * 
   * @param version
   * @param collectionSetId
   * @param collection
   * @param transferActionId
   * @param transferBatchId
   * @param connectionId
   * @param etlRepRockFact
   * @param connectionPool
   * @param trActions
   * @param parentLogger
   * @throws EngineMetaDataException
   */
  public UpdateCollectedDataAction(final Meta_versions version, final Long collectionSetId,
      final Meta_collections collection, final Long transferActionId, final Long transferBatchId,
      final Long connectionId, final RockFactory etlRepRockFact, final ConnectionPool connectionPool,
      final Meta_transfer_actions trActions, final Logger parentLogger) throws EngineMetaDataException {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectionId, etlRepRockFact,
        connectionPool, trActions);

    final String logname = parentLogger.getName() + ".UpdateCollectedDataAction";
    log = Logger.getLogger(logname);

    if (this.getTrActions().getAction_contents() == null || this.getTrActions().getAction_contents().length() <= 0) {
      throw new EngineMetaDataException("Velocity template not set", new Exception(), "init");
    }
  }

  @Override
  public void execute() throws EngineException, EngineMetaDataException {
    String sqlClause;

    final StringWriter writer = new StringWriter();
    VelocityEngine vEngine = null;

    final VelocityContext context = populateContext();

    if (context != null) {
      try {

        vEngine = VelocityPool.reserveEngine();
        vEngine.evaluate(context, writer, "", this.getTrActions().getAction_contents());

        sqlClause = writer.toString();
        log.finer("Evaluated SQL statement" + sqlClause);

        executeUpdate(sqlClause);

      } catch (final EngineException ee) {
        log.info("Template evaluation failed");
        throw ee;
      } catch (final Exception e) {
        log.log(Level.WARNING, "Template evaluation failure on:\n" + this.getTrActions().getAction_contents(), e);
      } finally {
        VelocityPool.releaseEngine(vEngine);
      }
    }

  }

  private void executeUpdate(final String sqlClause) throws EngineException {
    log.finer("Parsed sql:" + sqlClause);

    Statement stmt = null; // NOPMD - see close() method in finally block

    try {
      stmt = getConnection().getConnection().createStatement();
      stmt.executeUpdate(sqlClause);
      stmt.getConnection().commit();
    } catch (Exception e) {
      log.log(Level.WARNING, "Statement failed: " + sqlClause, e);
      throw new EngineException("UpdateCollectedDataAction statement failed", e, this, "execute", "ERROR");
    } finally {
      close(null, stmt, log);
    }
  }

  private VelocityContext populateContext() throws EngineException, EngineMetaDataException {
    dbUTCDateTimeFormat.setTimeZone(UTC_TIME_ZONE);
    VelocityContext context = new VelocityContext();

    final Properties whereClause = stringToProperties(getTrActions().getWhere_clause());

    final long oldestRopStartTimeInDays = Long.valueOf(StaticProperties.getProperty("oldestRopStartTimeInDays", "1"));
    final long currentTime = System.currentTimeMillis();
    final long oldestTime = currentTime - (MS_IN_DAY * oldestRopStartTimeInDays);
    context.put("currentTime", dbUTCDateTimeFormat.format(currentTime));
    context.put("oldestTime", dbUTCDateTimeFormat.format(oldestTime));

    // StorageID of source type. Example LOG_SESSION_ADAPTER:PLAIN
    final String sourceStorageID = whereClause.getProperty("sourceType");
    final List<String> sourceTables = getListOfTables(sourceStorageID, oldestTime, currentTime);

    // StorageID of target type. Example LOG_SESSION_COLLECTED_DATA:PLAIN
    final String targetStorageID = whereClause.getProperty("targetType");
    final List<String> targetTables = getListOfTables(targetStorageID, oldestTime, currentTime);

    if (sourceTables.isEmpty() || targetTables.isEmpty()) {
      context = null;
      log.log(Level.INFO, "No source/target tables available between " + oldestTime + " and " + currentTime);
    } else {
      context.put("sourceTables", sourceTables);
      context.put("targetTables", targetTables);
    }

    return context;

  }

  private List<String> getListOfTables(final String storageId, final long oldestTime, final long currentTime)
      throws EngineException {
    log.finest("storageID = " + storageId);

    if (storageId == null) {
      throw new EngineException("storage ID not found", null, this, "populateContext", "");
    }
    return PhysicalTableCache.getCache().getTableName(storageId, oldestTime, currentTime);
  }
}
