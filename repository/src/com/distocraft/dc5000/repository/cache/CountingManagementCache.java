/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.repository.cache;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Countingmanagement;

/**
 * Maintains and provides access to a shared static cache of volume partition
 * counting management info. </br> This is organised by primarily by storageID,
 * and secondarily by partition tablename. </br> The counting
 * (Countingmanagement) info contains the last loaded aggregation row, or 0 if
 * never aggregated, or after table truncation. </br> The cache is loaded
 * dynamically, as each storageID is accessed. </br> To avoid cache
 * synchronization issues, all aggregation info updates/access must be executed
 * via this cache.
 * 
 * @author efinian
 * 
 */
public class CountingManagementCache {

  private static Logger log = Logger.getLogger(CountingManagementCache.class.getSimpleName());

  // Map<StorageID,<TableName,Countingmanagement>
  private static Map<String, Map<String, Countingmanagement>> cache = new HashMap<String, Map<String, Countingmanagement>>();

  private static RockFactory dwhRepRockFactory = null;

  // The right join ensures a row is returned in the result set for all
  // partitions named in the DWHpartition table,
  // even when there is no corresponding row in the the CountingManagement for
  // this table.
  private static final String LOAD_AGG_INFO_FOR_STORAGE_ID = "select d.tablename, agg.lastaggregatedrow "
      + "from countingmanagement agg " + "right join "
      + "	(select tablename from dwhpartition where storageid=?) d on (agg.tablename=d.tablename)";

  private static final String DELETE_AGG_INFO_FOR_STORAGE_ID = "delete from countingmanagement where storageid = ?";

  private static String etlrepUser;

  private static String etlrepPassword;

  private static String etlrepUrl;

  private static String etlrepDriver;

  private static Lock lock = new ReentrantLock(true);

  private CountingManagementCache() { /**/
  }

  /**
   * Initialize the cache, setting up a dwhrep connection. Also, allows
   * re-initialzation with a new connection.
   * 
   * @param etlRepRockFactory
   *          DB factory for the ETLREP (metadata) repository DB.
   */
  public static synchronized void initializeCache(final RockFactory etlRepRockFactory) {
    lock.lock();
    try {
      if (dwhRepRockFactory != null) {
        clearCache();
        // close existing connection
        dwhRepRockFactory.getConnection().close();
        dwhRepRockFactory = null;
      }

      // Saving connection info for future retries, if required.
      etlrepUser = etlRepRockFactory.getUserName();
      etlrepPassword = etlRepRockFactory.getPassword();
      etlrepUrl = etlRepRockFactory.getDbURL();
      etlrepDriver = etlRepRockFactory.getDriverName();
      dwhRepRockFactory = DBUtils.createPrivateRockFactory(etlRepRockFactory, DBUtils.DWHREP_CONNECTION_NAME);

    } catch (final SQLException e) {
      log.warning("Problem closing existing " + CountingManagementCache.class.getSimpleName() + "connection: " + e);
    } finally {
      lock.unlock();
    }

  }

  /**
   * Clear the cache of all counter management info for a specific storage id.
   * This will force a refresh of cache on next access for this storage id.
   * 
   * @param storageId
   */
  public static synchronized void clearCache(final String storageId) {
    final Map<String, Countingmanagement> removedValue = cache.remove(storageId);
    if (removedValue == null) {
      log.warning("Attempt to remove an inactive storage id from the counting managment cache: " + storageId);
    }
  }

  /**
   * Retrieve CountingManagement from the cache based on the storageID. Load the
   * data from the DB if it's not already in the cache.
   * CountingManagement.Lastaggregatedrow is defaulted to 0.
   * 
   * @param storageID
   *          A storageID for the measurement table partitions.
   * @return The map (by tablename) of CountingManagement (lastAggregatedRow)
   *         for all partitions (of the given storageID)
   * @throws IllegalStateException
   *           if the storageID does not exist, or s SQLException occurs.
   */
  public static Map<String, Countingmanagement> getCountingManagementData(final String storageID) {
    Map<String, Countingmanagement> aggregationInfo = cache.get(storageID);

    try {
      if (aggregationInfo == null) {
        // Not in cache-memory, so load from DB
        aggregationInfo = loadPartitionInfoFromDB(storageID);
        if (aggregationInfo == null) {
          throw new IllegalStateException("No database aggregations possible for (unknown) storageID: " + storageID);
        }
        cache.put(storageID, aggregationInfo);
      }

    } catch (final SQLException e) {
      log.warning("SQLException: Error during load of aggregation info from DB for storage id: " + e.getMessage());
      throw new IllegalStateException("Error during load of aggregation info from DB for storage id: " + storageID, e);
    } catch (RockException e) {
      log.warning("RockException: Error during load of aggregation info from DB for storage id: " + e.getMessage());
      throw new IllegalStateException("Error during load of aggregation info from DB for storage id: " + storageID, e);
    }

    return aggregationInfo;

  }

  /**
   * Update the details (lastAggregatedRow) for a list of CountingManagement
   * from a specific storageID.
   * 
   * @param storageID
   *          A storageID for the measurement table partitions
   * @param aggregationInfoList
   *          The list of CountingManagement which require updates.
   * @throws IllegalStateException
   *           If no cacheEntry/DB Partition can be found for the
   *           CountingManagement table, or SQL error.
   */

  public static void updateCountingManagementData(final String storageID,
      final List<Countingmanagement> aggregationInfoList) {
    // Find a row in the cache and update cache and DB.
    final Map<String, Countingmanagement> cacheMap = getCountingManagementData(storageID);
    Countingmanagement cacheEntry = null;

    try {
      checkDwhRepRockFactory();
      for (final Countingmanagement aggregationInfo : aggregationInfoList) {
        cacheEntry = cacheMap.get(aggregationInfo.getTablename());
        if (cacheEntry == null) {
          // Should not happen, as partition name should always be valid.
          throw new IllegalStateException("Could not find entry in cache or db for table: "
              + aggregationInfo.getTablename());
        }
        cacheEntry.setLastaggregatedrow(aggregationInfo.getLastaggregatedrow());
        cacheEntry.saveDB();
      }
    } catch (final SQLException e) {
      log.warning("SQLException: Error during update of CountingManagement cache: " + e.getMessage());
      throw new IllegalStateException(e);
    } catch (final RockException e) {
      log.warning("RockException: Error during update of CountingManagement cache: " + e.getMessage());
      throw new IllegalStateException(e);
    }
  }

  /**
   * Resets (in cache & DB) the CountingManagement for the given partionName
   * (for a storageID)
   * 
   * @param dwhRepRockFactory
   *          A connectionFactory for the dwhRep database
   * @param storageID
   *          A storageID for the measurement table partitions
   * @param partitionName
   *          A partition table name
   * @throws IllegalStateException
   *           If no cacheEntry/DB Partition can be found for the partitionName,
   *           or SQL error.
   */
  public static void clearPartitionInfo(final String storageID, final String partitionName) {

    final Map<String, Countingmanagement> cacheMap = getCountingManagementData(storageID);

    final Countingmanagement cacheEntry = cacheMap.get(partitionName);
    if (cacheEntry == null) {
      throw new IllegalStateException("Could not find table in cache or db for table: " + partitionName);
    }

    try {
      cacheEntry.setLastaggregatedrow((long) 0);
      cacheEntry.saveDB();
    } catch (final SQLException e) {
      throw new IllegalStateException(e);
    } catch (final RockException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Removes (in cache & DB) the CountingManagement for the given partionName
   * (for a storageID)
   * 
   * @param dwhRepRockFactory
   *          A connectionFactory for the dwhRep database
   * @param storageID
   *          A storageID for the measurement table partitions
   * @param partitionName
   *          A partition table name
   * @throws IllegalStateException
   *           If no cacheEntry/DB Partition can be found for the partitionName,
   *           or SQL error.
   */
  public static void removePartitionInfo(final String storageID, final String partitionName) {

    final Map<String, Countingmanagement> cacheMap = getCountingManagementData(storageID);

    final Countingmanagement cacheEntry = cacheMap.get(partitionName);
    if (cacheEntry == null) {
      throw new IllegalStateException("Could not find table in cache or db for table: " + partitionName);
    }

    try {
      cacheEntry.deleteDB();
      cacheMap.remove(partitionName);
    } catch (final SQLException e) {
      throw new IllegalStateException(e);
    } catch (final RockException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Delete all Volume loader aggregation info from the cache & DB for a given
   * storageID.
   * 
   * @param storageID
   *          A storageID for the measurement table partitions
   * @throws IllegalStateException
   *           on SQL error.
   */
  public static void deleteStorageDetails(final String storageID) {
    cache.remove(storageID);

    PreparedStatement deleteAggregationInfoStatement = null;
    int deletedRowCount = 0;

    try {
      checkDwhRepRockFactory();
      deleteAggregationInfoStatement = dwhRepRockFactory.getConnection().prepareStatement(
          DELETE_AGG_INFO_FOR_STORAGE_ID);
      deleteAggregationInfoStatement.setString(1, storageID);
      deletedRowCount = deleteAggregationInfoStatement.executeUpdate();
      if (deletedRowCount < 1) {
        log.warning("Unexpected: No aggregation info rows in DB to delete for storageID: " + storageID);
      }
    } catch (final SQLException e) {
      throw new IllegalStateException(e);
    } catch (final RockException e) {
      throw new IllegalStateException(e);
    } finally {
      try {
        if (deleteAggregationInfoStatement != null) {
          deleteAggregationInfoStatement.close();
        }
      } catch (final SQLException e) {
        log.warning("Cleanup error - " + e.toString());
      }
    }

  }

  /**
   * Load CountingManagement from the database. An entry is retrieved for all
   * partitions for the given storageID. <br/>
   * Partitions without an explicit lastAggregationRow value are flagged as new
   * to control insert/update behaviour during a subsequent update.
   * 
   * @param storageID
   *          A storageID for the measurement table partitions
   * @return A map by tablename of CountingManagement for all partitions of the
   *         storageID.
   * @throws SQLException
   * @throws RockException
   */
  private static Map<String, Countingmanagement> loadPartitionInfoFromDB(final String storageID) throws SQLException,
      RockException {
    String tableName = null;
    long lastRowId = -1;
    boolean newRow = false;
    final Map<String, Countingmanagement> aggInfoMap = new Hashtable<String, Countingmanagement>();

    log.info(String.format("Loading CountingManagment Info from DB for storageID [%s]", storageID));

    PreparedStatement getAggregationInfoStatement = null;
    ResultSet result = null;
    try {
      checkDwhRepRockFactory();
      getAggregationInfoStatement = dwhRepRockFactory.getConnection().prepareStatement(LOAD_AGG_INFO_FOR_STORAGE_ID);
      getAggregationInfoStatement.setString(1, storageID);
      result = getAggregationInfoStatement.executeQuery();
      while (result.next()) {
        tableName = result.getString(1);
        lastRowId = result.getLong(2); // Null is returned as 0
        newRow = result.wasNull(); // Null indicates that the last row value
                                   // does not exist in the DB.

        final Countingmanagement aggInfo = new Countingmanagement(dwhRepRockFactory); // NOPMD
                                                                                      // by
                                                                                      // efinian
                                                                                      // on
                                                                                      // 07/05/10
                                                                                      // 11:36
                                                                                      // (cache
                                                                                      // entry)
        aggInfo.setStorageid(storageID);
        aggInfo.setTablename(tableName);
        aggInfo.setLastaggregatedrow(lastRowId);
        aggInfo.setNewItem(newRow);
        aggInfoMap.put(tableName, aggInfo);

        log.fine(String.format("CountingManagment Detail: storageId[%s],tableName[%s],lastRow[%d],new[%b]", storageID,
            tableName, lastRowId, newRow));
      }
    } finally {
      if (result != null) {
        result.close();
      }
      try {
        if (getAggregationInfoStatement != null) {
          getAggregationInfoStatement.close();
        }
      } catch (final Exception e) {
        log.warning("Cleanup error - " + e.toString());
      }
    }
    return aggInfoMap;
  }

  private static void checkDwhRepRockFactory() throws SQLException, RockException {
    lock.lock();
    try {
      if (dwhRepRockFactory == null || dwhRepRockFactory.getConnection().isClosed()) {
        clearCache();
        dwhRepRockFactory = null;
        RockFactory privateEtlRockObject = null;
        try {
          privateEtlRockObject = new RockFactory(etlrepUrl, etlrepUser, etlrepPassword, etlrepDriver,
              "ForCountingManagementCacheRetry", true);
          dwhRepRockFactory = DBUtils.createPrivateRockFactory(privateEtlRockObject, DBUtils.DWHREP_CONNECTION_NAME);
        } finally {
          try {
            if (privateEtlRockObject != null) {
              privateEtlRockObject.getConnection().close();
              privateEtlRockObject = null;
              log.fine("Closed CountingManagementCache.privateEtlRockObject.");
            }
          } catch (Exception e1) {
            log.fine("Exception encountered while closing privateEtlRockObject.");
          }
        }
      }
    } finally {
      lock.unlock();
    }

  }

  private static void clearCache() {
    // clean out existing cache
    for (final String key : cache.keySet()) {
      cache.remove(key);
    }
  }
}
