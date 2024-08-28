/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.sql;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
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
import com.ericsson.eniq.common.VelocityPool;

/**
 * Creates the files that will be loaded into the tables, LOG_SESSION_COLLECTED_DATA.
 * 
 * These files are generated based on a lookup on LOG_SESSION_ADAPTER.
 * 
 * @author epaujor
 * 
 */
public class CreateCollectedDataFilesAction extends SQLOperation {

  private static final int MS_IN_DAY = 1000 * 60 * 60 * 24;

  private transient final SimpleDateFormat fileSDF = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault());

  private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

  protected final Logger log;

  private static final String COLLECTED_DATA = "COLLECTED_DATA";

  // Used ROP_STARTTIME instead of DATE_ID in LOG_SESSION_ADAPATER because backlog of data could come in at the one time
  // for several days.
  protected transient String getSourcesNotInLOG_COLLECTED_DATA = "select DATEFORMAT(ROP_STARTTIME, 'YYYY-MM-DD') as dateID, SOURCE, TYPENAME "
      + "from LOG_SESSION_ADAPTER where dateID||SOURCE||TYPENAME not in (select distinct DATE_ID||SOURCE||TYPENAME from LOG_SESSION_COLLECTED_DATA)"
      + "and ROP_STARTTIME >= ? group by SOURCE, dateID, TYPENAME";

  private transient final String PATH_TO_COLLECTED_DATA_FILES = System
      .getProperty("PATH_TO_COLLECTED_DATA_FILES", System.getProperty("ETLDATA_DIR") + File.separator + "session"
          + File.separator + COLLECTED_DATA + File.separator);

  /**
   * Creates the files that will be loaded into the tables, LOG_SESSION_COLLECTED_DATA
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
  public CreateCollectedDataFilesAction(final Meta_versions version, final Long collectionSetId,
      final Meta_collections collection, final Long transferActionId, final Long transferBatchId,
      final Long connectionId, final RockFactory etlRepRockFact, final ConnectionPool connectionPool,
      final Meta_transfer_actions trActions, final Logger parentLogger)
      throws EngineMetaDataException {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectionId, etlRepRockFact,
        connectionPool, trActions);

    final String logname = parentLogger.getName() + ".CreateCollectedDataFilesAction";
    log = Logger.getLogger(logname);

    if (this.getTrActions().getAction_contents() == null || this.getTrActions().getAction_contents().length() <= 0) {
      throw new EngineMetaDataException("Velocity template not set", new Exception(), "init");
    }
  }

  /*
   * Runs the action to create the files that will be loaded into the tables, LOG_SESSION_COLLECTED_DATA
   * 
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.structure.TransferActionBase#execute()
   */
  @Override
  public void execute() throws SQLException {

    final RockFactory dwhRockFactory = this.getConnection();

    ResultSet result = null; // NOPMD - see close() method in finally block
    PreparedStatement preparedStatement = null;
    try {
      preparedStatement = dwhRockFactory.getConnection().prepareStatement(getSourcesNotInLOG_COLLECTED_DATA);
      preparedStatement.setString(1, getOldestRopStartTime());
      result = preparedStatement.executeQuery();

      while (result.next()) {
        final Date dateId = result.getDate(1);
        final String source = result.getString(2);
        final String typeName = result.getString(3);

        final String fileContent = getCollectedDataFileContent(dateId, source, typeName);
        createCollectedDataFile(dateId, fileContent);
      }
    } catch (final SQLException e) {
      log.log(Level.WARNING, "Template evaluation failure on:\n" + e.getMessage(), e);
      throw e;
    } finally {
      close(result, preparedStatement, log);
    }
  }

  private String getOldestRopStartTime() {
    final SimpleDateFormat dbUTCDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault());
    dbUTCDateTimeFormat.setTimeZone(UTC_TIME_ZONE);
    final long oldestRopStartTimeInDays = Long.valueOf(StaticProperties.getProperty("oldestRopStartTimeInDays", "1"));
    final long oldestRopStartTime = System.currentTimeMillis() - (MS_IN_DAY * oldestRopStartTimeInDays);

    return dbUTCDateTimeFormat.format(oldestRopStartTime);
  }

  private String getCollectedDataFileContent(final Date dateId, final String source, final String typeName) {
    String fileContent = null;
    final StringWriter writer = new StringWriter();
    VelocityEngine vEngine = null;

    try {
      vEngine = VelocityPool.reserveEngine();
      final VelocityContext context = populateContext(dateId, source, typeName);
      vEngine.evaluate(context, writer, "", this.getTrActions().getAction_contents());
      fileContent = writer.toString();

      log.finer("Evaluated SQL statement" + fileContent);

    } catch (final EngineException ee) {
      log.info("Template evaluation failed");
    } catch (final Exception e) {
      log.log(Level.WARNING, "Template evaluation failure on:\n" + this.getTrActions().getAction_contents(), e);
    } finally {
      VelocityPool.releaseEngine(vEngine);
    }
    return fileContent;
  }

  /**
   * The file name has to match a certain format, e.g. <COLLECTED_DATA>.timeInMilliSeconds.yyyy-MM-dd
   * 
   * @param dateId
   * @param fileContent
   */
  private void createCollectedDataFile(final Date dateId, final String fileContent) {
    final String collectedDatafileName = COLLECTED_DATA + "_" + System.currentTimeMillis() + "."
        + fileSDF.format(dateId);

    // Initially end file name with ".unfinished" so that loader will not pick it up straight away.
    // Remove this after file has been written to.
    final File collectedDataFile = new File(PATH_TO_COLLECTED_DATA_FILES + collectedDatafileName + ".unfinished");
    try {
      collectedDataFile.createNewFile();
      final FileOutputStream fileOutputStream = new FileOutputStream(collectedDataFile);
      fileOutputStream.write(fileContent.getBytes());
      fileOutputStream.flush();
      fileOutputStream.close();

      collectedDataFile.renameTo(new File(PATH_TO_COLLECTED_DATA_FILES + collectedDatafileName));
    } catch (IOException e) {
      log.log(Level.WARNING, "IOException failure on:\n" + collectedDataFile, e);
    }
  }

  /**
   * Populates VelocityContext object for the file template that will be loaded into the tables,
   * LOG_SESSION_COLLECTED_DATA
   */
  private VelocityContext populateContext(final Date dateId, final String source, final String typeName)
      throws EngineException {

    final VelocityContext context = new VelocityContext();

    if (dateId == null || source == null) {
      throw new EngineException("DateId or source cannot be found", null, this, "populateContext", "");
    }
    context.put("typeName", typeName);
    context.put("source", source);
    context.put("date", dateId);

    return context;

  }
}
