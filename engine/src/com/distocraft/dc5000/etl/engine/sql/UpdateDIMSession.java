package com.distocraft.dc5000.etl.engine.sql;

import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * TODO intro <br>
 * TODO usage <br>
 * TODO used databases/tables <br>
 * TODO used properties <br>
 * <br>
 * Copyright Distocraft 2005 <br>
 * <br>
 * $id$
 * 
 * @author lemminkainen
 */
public class UpdateDIMSession extends SQLOperation {

	private final Logger log;
	private final Logger sqlLog;

	private final SetContext sctx;

	private String elem = "";

	private final boolean useROWSTATUS;

	public UpdateDIMSession(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final SetContext sctx)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.sctx = sctx;

		try {

			final Meta_collection_sets whereCollSet = new Meta_collection_sets(rockFact);
			whereCollSet.setEnabled_flag("Y");
			whereCollSet.setCollection_set_id(collectionSetId);
			final Meta_collection_sets collSet = new Meta_collection_sets(rockFact, whereCollSet);

			final String techPack = collSet.getCollection_set_name();
			final String set_type = collection.getSettype();
			final String set_name = collection.getCollection_name();
			final String logName = techPack + "." + set_type + "." + set_name;

			final Properties prop = TransferActionBase.stringToProperties(trActions.getWhere_clause());

			this.elem = prop.getProperty("element", "");
			this.useROWSTATUS = "true".equalsIgnoreCase(prop.getProperty("useRAWSTATUS", "false"));

			this.log = Logger.getLogger("etl." + logName + ".loader.UpdateDIMSession");
			this.sqlLog = Logger.getLogger("sql." + logName + ".loader.UpdateDIMSession");

		} catch (Exception e) {
			throw new EngineMetaDataException("UpdateDIMSession initialization error", e, "init");
		}

	}

  public void execute() throws EngineException {

    final RockFactory r = this.getConnection();
    Statement s = null;

    try {

      log.fine("Executing...");

      final SimpleDateFormat sdfIN = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      final SimpleDateFormat sdfOUT = new SimpleDateFormat("yyyy-MM-dd");

      final String measType = (String) sctx.get("MeasType");

      s = r.getConnection().createStatement();

      final String sessionEndTime = String.valueOf(System.currentTimeMillis());

      if (elem != null && elem.length() > 0) {
        elem = ", " + elem;
      }

      String sqlClause;
      
      final List tableList = (List)sctx.get("tableList");
      log.fine("printing table list size---->"+tableList.size());

      for (Object aTableList : tableList) {
        String tableName = (String) aTableList;
        log.fine("printing table list name---->" + tableName);

        if (useROWSTATUS) {

          sqlClause = "SELECT COUNT(*) AS ROWCOUNT, SESSION_ID, BATCH_ID, TIMELEVEL, DATETIME_ID " + elem + " FROM "
            + tableName + " WHERE ROWSTATUS IS NULL OR ROWSTATUS = '' " + " GROUP BY SESSION_ID, BATCH_ID,  TIMELEVEL,  DATETIME_ID " + elem;

        } else {

          sqlClause = "SELECT COUNT(*) AS ROWCOUNT, SESSION_ID, BATCH_ID, TIMELEVEL, DATETIME_ID " + elem + " FROM "
            + tableName + " WHERE YEAR_ID IS NULL " + " GROUP BY SESSION_ID, BATCH_ID,  TIMELEVEL,  DATETIME_ID " + elem;

        }

        //log.info("Getting session information from raw table view " + rawView);
        log.info("Getting session information from the raw table " + tableName);
        sqlLog.finer(sqlClause);
        log.info("sqlClause in updateDimSession-->" + sqlClause);

        final ResultSet resultSet = s.executeQuery(sqlClause);

        int totalRowCount;
        //Create the new collection...
        final Collection<Map<String, Object>> collection = new ArrayList<Map<String, Object>>();

        while (resultSet.next()) {

          totalRowCount = resultSet.getInt(1);

          if (totalRowCount <= 0) { // No ROWS???
            return;
          }

          final String element;
          final String sessionID = resultSet.getString(2);
          final String batchID = resultSet.getString(3);
          final String timeLevel = resultSet.getString(4);
          final String dateTimeID = resultSet.getString(5);
          if (elem == null || elem.equalsIgnoreCase("")) {
            element = null;
          } else {
            element = resultSet.getString(6);
          }

          log.finest("sessionID: " + sessionID);
          log.finest("batchID: " + batchID);
          log.finest("rowCount: " + totalRowCount);
          log.finest("dateTimeID: " + dateTimeID);
          log.finest("timeLevel: " + timeLevel);
          log.finest("source: " + element);

          // Date time formatting needs this null checking so null values won't stop overall processing
          final String formattedDatetime = (dateTimeID == null ? null : sdfOUT.format(sdfIN.parse(dateTimeID)));

          final Map sessionLogEntry = (Map) sctx.get("sessionLogEntry");

          sessionLogEntry.put("SESSION_ID", sessionID);
          sessionLogEntry.put("BATCH_ID", batchID);
          sessionLogEntry.put("DATE_ID", formattedDatetime);
          sessionLogEntry.put("TIMELEVEL", timeLevel);
          sessionLogEntry.put("DATATIME", dateTimeID);
          sessionLogEntry.put("DATADATE", formattedDatetime);
          sessionLogEntry.put("ROWCOUNT", String.valueOf(totalRowCount));
          sessionLogEntry.put("SESSIONENDTIME", sessionEndTime);
          if(isBeforeRestore(sdfIN,dateTimeID)){
        	  sessionLogEntry.put("STATUS", "RESTORED");
          }
          else{
        	  sessionLogEntry.put("STATUS", "OK");
          }
          sessionLogEntry.put("TYPENAME", measType);
          sessionLogEntry.put("SOURCE", element);

          //Need to dump what we have at this point to file.
          //This is to avoid any memory problems...
          final int bulkLimit = SessionHandler.getBulkLimit();
          if (collection.size() >= bulkLimit) {
            log.finest("The number of session is currently > " + bulkLimit + ". Performing write to SessionLog.");
            SessionHandler.bulkLog("LOADER", collection);
            log.finest("Finished Writing to the SessionLog.");
            //Now clear the collection...
            collection.clear();
          }
          //Write the session to the collection for bulk logging.
          collection.add(new HashMap<String, Object>(sessionLogEntry));
          log.fine("Saving session information. SessionID: " + sessionID + " BatchID: " + batchID);
        }
        log.finest("Writing to the SessionLog...");
        SessionHandler.bulkLog("LOADER", collection);
        log.finest("Finished Writing to the SessionLog.");

        resultSet.close();

        if ("TABLE_NOT_FOUND".equals(tableName)) {
          continue;
        }

        if (useROWSTATUS) {

          sqlClause = "UPDATE " + tableName + " SET ROWSTATUS = 'LOADED' WHERE ROWSTATUS IS NULL or ROWSTATUS = ''";
          log.info("Updating ROWSTATUS from raw table " + tableName);

        } else {

          sqlClause = "UPDATE " + tableName + " SET YEAR_ID = datepart(yy, date_id) WHERE YEAR_ID IS NULL";
          log.info("Updating YEAR_IDs from raw table " + tableName);

        }

        sqlLog.finer(sqlClause);
        s.executeUpdate(sqlClause);

      } // end while

      log.fine("Succesfully updated...");

    } catch (Exception e) {
      log.log(Level.WARNING, "Update DIM session failed", e);
    } finally {

      if (s != null) {
        try {
          s.close();
        } catch (Exception e) {
          log.log(Level.WARNING, "error closing statement", e);
        }
      }

      try {
        r.getConnection().commit();
      } catch (Exception e) {
        log.log(Level.WARNING, "error finally committing", e);
      }

    }

  }

/**
 * Check if datetime_id of row is less than restore time
 * If restore is enabled
 * @param sdfIN 
 * 
 * @param dateTimeID
 * @return true 
 * if data loaded in restore period
 */
  private boolean isBeforeRestore(SimpleDateFormat sdfIN, String dateTimeID) {
	  boolean isRestoreEnabled = (boolean) sctx.get("RESTORE_FLAG");
	  if(isRestoreEnabled){
		  try {
			  long restoreTime = (long) sctx.get("RESTORE_TIMESTAMP");
			  long dateTimeMil = sdfIN.parse(dateTimeID).getTime();
			  if(dateTimeMil < restoreTime){
				  log.finer("Row loaded is from a restored file");
				  return true;
			  }
		  }
		  catch (ParseException e) {
			  log.warning("Unable to parse " + dateTimeID + "to milliseconds" + e);
		  }
	  }
	  return false;
  }
}
