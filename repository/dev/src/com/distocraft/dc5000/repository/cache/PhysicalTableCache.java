/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2010 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.repository.cache;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;

/**
 * In-memory cache for accessing data in following repository tables.
 * <ul>
 * <li>dwhrep.DWHPartition</li>
 * <li>dwhrep.DWHType</li>
 * <li>dwhrep.TPActivation</li>
 * <li>dwhrep.TypeActivation</li>
 * <li>dwhrep.PartitionPlan</li>
 * </ul>
 * 
 * @author etuolem
 */
public class PhysicalTableCache {

    private static final Logger LOG = Logger.getLogger("etlengine.repository.PhysicalTableCache");

    private static final String GET_ACTIVE_TABLES = "SELECT dwhp.storageid, dwhp.tablename, dwhp.starttime, dwhp.endtime, dwhp.status, dwht.partitionsize, plan.defaultpartitionsize, dwhp.loadorder "
            + "FROM DWHPartition dwhp, DWHType dwht, TPActivation tpa, TypeActivation ta, PartitionPlan plan "
            + "WHERE dwht.storageid = dwhp.storageid AND dwht.techpack_name = tpa.techpack_name AND dwht.techpack_name = ta.techpack_name AND "
            + "dwht.typename = ta.typename AND dwht.tablelevel = ta.tablelevel AND dwht.partitionplan = plan.partitionplan AND "
            + "tpa.status = 'ACTIVE' AND ta.status='ACTIVE'";

    private static final String ACTIVE = "ACTIVE";

    private static final String USER = "USER";

    // date: 9999-01-01 00:00:00
    private static final long ENDOFTHEWORLD = 253370757600000L;

    private String dburl = null;
    private String dbdrv = null;
    private String dbusr = null;
    private String dbpwd = null;
    private String dwhdburl = null;
    private String dwhdbdrv = null;
    private String dwhdbusr = null;
    private String dwhdbpwd = null;

    private volatile Map<String, List<PTableEntry>> tableMap = null;

    private static PhysicalTableCache ptc = null;

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

    private static final Calendar utcCalendar = Calendar.getInstance(UTC_TIME_ZONE);

    private final SimpleDateFormat dbUTCDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault());

    /**
     * Empty private constructor to prevent external initialisation. Use method <code>getCache()</code> to get reference to this singleton object.
     */
    private PhysicalTableCache() {
        dbUTCDateTimeFormat.setTimeZone(UTC_TIME_ZONE);
    }

    /**
     * Initialise this singleton object. Etlrep reference is only used to lookup dwhrep connection details. Initialize will create independent
     * connection to dwhrep database.
     * 
     * @param etlrep
     *            Connection to etlrep schema
     */
    public static void initialize(final RockFactory etlrep) {

        ptc = new PhysicalTableCache();

        try {

            LOG.fine("Initializing...");

            final Meta_databases repdb = ptc.getMetaDBInfoForConName(etlrep, "dwhrep");

            ptc.dburl = repdb.getConnection_string();
            ptc.dbdrv = repdb.getDriver_name();
            ptc.dbusr = repdb.getUsername();
            ptc.dbpwd = repdb.getPassword();

            LOG.config("Repository: " + ptc.dburl);

            final Meta_databases dwh = ptc.getMetaDBInfoForConName(etlrep, "dwh_coor");
            ptc.dwhdburl = dwh.getConnection_string();
            ptc.dwhdbdrv = dwh.getDriver_name();
            ptc.dwhdbusr = dwh.getUsername();
            ptc.dwhdbpwd = dwh.getPassword();

            ptc.revalidate();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Fatal initialization error", e);
        }

    }

    /**
     * Initialise this singleton object. Rock reference is only used to lookup database driver name. Initialize will create independent connection to
     * dwhrep database.
     * 
     * @param rock
     *            Rock Object to get the driver from
     * @param dburl
     *            REPDB url
     * @param dbusr
     *            REPDB user
     * @param dbpwd
     *            REPDB password
     * @param dwhdburl
     *            DWH url
     * @param dwhdbusr
     *            DWH user
     * @param dwhdbpwd
     *            DWH password
     */
    public static void initialize(final RockFactory rock, final String dburl, final String dbusr, final String dbpwd, final String dwhdburl,
                                  final String dwhdbusr, final String dwhdbpwd) {

        ptc = new PhysicalTableCache();

        try {
            ptc.dburl = dburl;
            ptc.dbdrv = rock.getDriverName();
            ptc.dbusr = dbusr;
            ptc.dbpwd = dbpwd;

            LOG.config("Repository: " + ptc.dburl);

            ptc.dwhdburl = dwhdburl;
            ptc.dwhdbdrv = rock.getDriverName();
            ptc.dwhdbusr = dwhdbusr;
            ptc.dwhdbpwd = dwhdbpwd;

            ptc.revalidate();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Fatal initialization error", e);
        }

    }

    /**
     * Returns a list containing the names of tables that are active for specified storageId. Empty list is returned if storageID not found or no
     * active tables found.
     * 
     * @param storageID
     *            The StorageID to look up.
     * @return The active dwh tables for the StorageID
     */
    public List<String> getActiveTables(final String storageID) {
        final List<String> ret = new ArrayList<String>();

        final List<PTableEntry> ptes = tableMap.get(storageID);

        if (ptes != null) {
            for (PTableEntry pte : ptes) {
                if (pte.status.equalsIgnoreCase("ACTIVE")) {
                    ret.add(pte.tableName);
                }
            }
        }

        return ret;
    }

    /**
     * Returns endTime for specified table. -1 is returned if table is not found.
     * 
     * @param tablename
     *            The dwh table to look up
     * @return The end time of the active dwh table, -1 if the table cant be found
     */
    public long getEndTime(final String tablename) {
        for (String key : tableMap.keySet()) {
            for (PTableEntry pte : tableMap.get(key)) {
                if (pte.status.equalsIgnoreCase(ACTIVE) && pte.tableName.equalsIgnoreCase(tablename)) {
                    return pte.endTime;
                }
            }
        }

        return -1;
    }

    /**
     * Returns startTime for specified table. -1 is returned if table is not found.
     * 
     * @param tablename
     *            The dwh table to look up
     * @return The start time of the active dwh table, -1 if the table cant be found
     */
    public long getStartTime(final String tablename) {

        for (String key : tableMap.keySet()) {
            for (PTableEntry pte : tableMap.get(key)) {
                if (pte.status.equalsIgnoreCase(ACTIVE) && pte.tableName.equalsIgnoreCase(tablename)) {
                    return pte.startTime;
                }
            }
        }

        return -1;
    }

    /**
     * Returns tableName for specified storageID and specified time. If storageID is not found or specified time not defined in partitioning null is
     * returned.
     * 
     * @param storageID
     *            The StorageID to look up.
     * @param dataTime
     *            the time the data exists
     * @return The active dwh tables for the StorageID where start>DataTime<endTime
     */
    public String getTableName(final String storageID, final long dataTime) {
        final List<PTableEntry> ptes = tableMap.get(storageID);

        String ret = null;

        if (ptes != null) {
            for (PTableEntry pte : ptes) {
                if (pte.status.equalsIgnoreCase(ACTIVE) && pte.endTime > dataTime && pte.startTime <= dataTime) {
                    ret = pte.tableName;
                }
            }

        }
        return ret;

    }

    /**
     * Returns a list of tablenames for specified storageID for specified time range. Only <code>ACTIVE</code> partitions are returned.
     * 
     * @param storageID
     *            The StorageID to look up.
     * @param startMs
     *            Data greater than
     * @param endMs
     *            Data less than
     * @return Active tables in dwh storing data with data dates between startMs and endMs
     */
    public List<String> getTableName(final String storageID, final long startMs, final long endMs) {
        return getTableName(ACTIVE, storageID, startMs, endMs);
    }

    /**
     * Returns a list of tablenames for specified storageID, and specified statuses for specified time range.
     * 
     * @param statuses
     *            Comma separated list of statuses to return
     * @param storageID
     *            The StorageID to look up.
     * @param startMs
     *            Data greater than
     * @param endMs
     *            Data less than
     * @return Active tables in dwh storing data with data dates between startMs and endMs
     */
    public List<String> getTableName(final String statuses, final String storageID, final long startMs, final long endMs) {

        final List<String> result = new ArrayList<String>();
        final List<PTableEntry> tables = tableMap.get(storageID);

        if (tables != null && statuses != null) {
            final List<String> statusList = new ArrayList<String>();
            final String[] statusArr = statuses.split(",");
            for (String status : statusArr) {
                if (status.length() > 0) {
                    statusList.add(status);
                }
            }

            for (PTableEntry pte : tables) {
                final long startTime = pte.startTime;
                final long endTime = (pte.endTime == 0L) ? ENDOFTHEWORLD : pte.endTime;

                if (statusList.contains(pte.status) && startMs < endTime && endMs >= startTime) {
                    result.add(pte.tableName);
                }
            }
        }

        return result;
    }

    /**
     * Returns a list of RAW Event tables that are ACTIVE for a specified storageID in the specified time range. This will not return RAW Event tables
     * if the startTime and endTime are equal to zero.
     * 
     * Based on UTC time.
     * 
     * @param storageID
     *            The StorageID to look up.
     * @param startMs
     *            Data greater than
     * @param endMs
     *            Data less than
     * @return Active tables in dwh storing data with data dates between startMs and endMs
     */
    public List<String> getTableNamesForRawEvents(final String storageID, final long startMs, final long endMs) {
        final List<String> result = new ArrayList<String>();
        final List<PTableEntry> ptes = tableMap.get(storageID);

        LOG.finest("getTableNamesForRawEvents(): Finding tables for " + storageID + " between the times "
                + dbUTCDateTimeFormat.format(new Date(startMs)) + "(UTC) and " + dbUTCDateTimeFormat.format(new Date(endMs)) + "(UTC)");
        if (ptes != null) {
            for (PTableEntry pte : ptes) {
                if (pte.endTime != 0L && pte.startTime != 0L) {
                    if (pte.status.equalsIgnoreCase(ACTIVE) && startMs <= pte.endTime && endMs >= pte.startTime) {
                        result.add(pte.tableName);
                        LOG.finest("getTableNamesForRawEvents(): Table " + pte.storageID + " -> " + pte.tableName + " "
                                + dbUTCDateTimeFormat.format(new Date(pte.startTime)) + "(UTC)..."
                                + dbUTCDateTimeFormat.format(new Date(pte.endTime)) + "(UTC)");
                    }
                }

            }
        }
        return result;
    }

    /**
     * Gets the latest table name for the RAW Event tables
     * 
     * @param storageID
     *            The StorageID to look up.
     * @return latest table being used to store RAW Events
     */
    public String getLatestTableNameForRawEvents(final String storageID) {
        String tableName = "";
        final List<PTableEntry> ptes = tableMap.get(storageID);
        long latestTimeInMs = 0;
        LOG.finest("getLatestTableNameForRawEvents(): Finding latest tables for " + storageID);
        if (ptes != null) {
            for (PTableEntry pte : ptes) {
                if (pte.status.equalsIgnoreCase(ACTIVE) && latestTimeInMs <= pte.endTime) {
                    latestTimeInMs = pte.endTime;
                    tableName = pte.tableName;
                }
            }
        }

        LOG.finest("getLatestTableNameForRawEvents(): Table " + storageID + " -> " + tableName + " endTime="
                + dbUTCDateTimeFormat.format(new Date(latestTimeInMs)) + "(UTC)");
        return tableName;
    }

    /**
     * Gets the earliest time for the RAW Event tables
     * 
     * @param storageID
     *            The StorageID to look up.
     * @return earlies table being used to store RAW Events
     */
    public long getEarliestTimeForRawEvents(final String storageID) {
        long earliestTime = System.currentTimeMillis();
        final List<PTableEntry> ptes = tableMap.get(storageID);
        if (ptes != null) {
            for (PTableEntry pte : ptes) {
                if (pte.endTime != 0L && pte.startTime != 0L) {
                    if (pte.startTime < earliestTime) {
                        earliestTime = pte.startTime;
                    }
                }
            }
        }
        return earliestTime;
    }

    /**
     * Returns the reference to this Singleton instance.
     * 
     * @return The cache
     */
    public static PhysicalTableCache getCache() {
        return ptc;
    }

    public static void setCache(final PhysicalTableCache cache) {
        ptc = cache;
    }

    /**
     * Revalidates the in-memory cache in case that something is changed in database.
     * 
     * @throws ClassNotFoundException
     *             Failed to load sql driver
     * @throws ssc.rockfactory.RockException
     *             Error connection to db
     * @throws java.sql.SQLException
     *             SQL Errors
     */
    @SuppressWarnings("PMD.CloseResource")
    // eeipca : closed in the cleanup call
    public void revalidate() throws RockException, SQLException, ClassNotFoundException {

        LOG.fine("Revalidating...");

        //  tableMap = new HashMap<String, List<PTableEntry>>();

        Connection con = null;
        Statement statement = null;
        ResultSet result = null;
        RockFactory rock = null;

        try {

            rock = new RockFactory(dburl, dbusr, dbpwd, dbdrv, "PhysicalTableCache", true);
            con = rock.getConnection();

            statement = con.createStatement();
            result = statement.executeQuery(GET_ACTIVE_TABLES);

            final Map<String, List<PTableEntry>> si_map2 = new HashMap<String, List<PTableEntry>>();

            while (result.next()) {
                final PTableEntry pte = new PTableEntry();

                pte.storageID = result.getString(1);
                pte.tableName = result.getString(2);

                final Timestamp sts = result.getTimestamp(3);
                if (sts == null) {
                    LOG.finest("Start time not defined -> Partition " + result.getString(2) + " not in use");
                    pte.startTime = -1;
                    continue;
                } else {
                    pte.startTime = sts.getTime();
                }

                final Timestamp ets = result.getTimestamp(4);
                if (ets == null) {
                    pte.endTime = ENDOFTHEWORLD;
                } else {
                    pte.endTime = ets.getTime();
                }

                pte.status = result.getString(5);
                pte.partitionsize = result.getLong(6);
                pte.defaultpartitionsize = result.getLong(7);

                if (pte.partitionsize == -1) {
                    pte.partitionsize = pte.defaultpartitionsize;
                }

                pte.loadOrder = result.getInt(8);

                List<PTableEntry> entries = si_map2.get(pte.storageID);
                if (entries == null) {
                    entries = new ArrayList<PTableEntry>(10);
                    si_map2.put(pte.storageID, entries);
                }

                LOG.finest("Table " + pte.storageID + " -> " + pte.tableName + " " + pte.startTime + "..." + pte.endTime);

                entries.add(pte);

                Collections.sort(entries);

            }

            revalidateCacheForRawEvents(si_map2);
            // Reference assignment is atomic
            tableMap = si_map2;
            LOG.info("Revalidation succesfully performed. " + tableMap.size() + " tables found");

        } finally {
            cleanup(con, statement, result);
            rock.getConnection().close();
        }
    }

    /**
     * Updates the order specified table in cache and repdb. Also partition start time is updated to current time.
     * 
     * @param storageID
     *            The StorageID to look up.
     * @param tableName
     *            The dwh table name
     * @param loadOrder
     *            the new load order
     * 
     * @throws java.sql.SQLException
     *             SQL Errors
     * @throws ssc.rockfactory.RockException
     *             Error connection to db
     */
    public void updateLoadOrder(final String storageID, final String tableName, final int loadOrder) throws RockException, SQLException {
        final List<PTableEntry> ptes = tableMap.get(storageID);

        for (PTableEntry pte : ptes) {
            if (pte.tableName.equals(tableName)) {

                RockFactory rock = null;
                try {
                    rock = new RockFactory(dburl, dbusr, dbpwd, dbdrv, "PhysicalTableCache", true);
                    final Dwhpartition part = new Dwhpartition(rock, pte.tableName);
                    part.setLoadorder(loadOrder);
                    part.setStarttime(new Timestamp(System.currentTimeMillis()));
                    part.updateDB();
                    pte.loadOrder = loadOrder;
                    Collections.sort(ptes);
                } finally {
                    if (rock != null) {
                        rock.getConnection().close();
                    }
                }
                break;
            }
        }

    }

    /**
     * Re-validate the start and end times for the RAW EVENT tables in the cache
     * 
     * @throws SQLException
     *             SQL Errors
     * @throws RockException
     *             Error connection to db
     * @throws ClassNotFoundException
     *             Failed to load sql driver
     */
    @SuppressWarnings("PMD.CloseResource")
    // eeipca : closed in the cleanup call
    protected void revalidateCacheForRawEvents(Map<String, List<PTableEntry>> mapTable) throws SQLException, RockException, ClassNotFoundException {

        LOG.fine("Revalidating cache for Raw Event tables...");
        Connection con = null;
        Statement statement = null;
        ResultSet resultSet = null;
        RockFactory dwhRock = null;

        try {
            dwhRock = new RockFactory(dwhdburl, dwhdbusr, dwhdbpwd, dwhdbdrv, "PhysicalTableCache_dwh", true);
            con = dwhRock.getConnection();

            statement = con.createStatement();
            resultSet = statement // Added sys. for hsql testing
                    .executeQuery("select tab.table_name from sys.systable tab, sys.sysuser suser where table_type='VIEW' "
                            + "and tab.creator = suser.user_id and suser.user_name = 'dc' and table_name like '%_RAW_TIMERANGE'");

            while (resultSet.next()) {
                final String viewName = resultSet.getString(1).trim();

                // Change EVENT_E_SGEH_SUC_RAW_TIMERANGE (viewName) to EVENT_E_SGEH_SUC:RAW (storageId)
                final String storageId = viewName.replace("_RAW_TIMERANGE", ":RAW");
                LOG.finest("storageId: " + storageId + ", viewName: " + viewName);

                final List<PTableEntry> ptes = mapTable.get(storageId);
                if (ptes != null) {
                    updateCacheWithDBValuesForRawEvents(con, viewName, ptes);
                }
            }
            LOG.fine("Revalidation finished successfully for Raw Event tables.");
        } finally {
            cleanup(con, statement, resultSet);
            dwhRock.getConnection().close();
        }
    }

    /**
     * Gets the latest startTime and endTime for each RAW event table from the database. This should only be read from database as PhysicalTableCache
     * is being created. After this, the cache should be updated with the correct startTime and endTime (there is no need to query databse again until
     * PhysicalTableCache is being re-created).
     * 
     * @param con
     *            Db connection
     * @param viewName
     *            the view name
     * @param ptes
     *            the entries to search for
     * @throws SQLException
     *             SQL Errors
     */
    private void updateCacheWithDBValuesForRawEvents(final Connection con, final String viewName, final List<PTableEntry> ptes) throws SQLException {
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            statement = con.createStatement();
            resultSet = statement.executeQuery("select MIN_DATE, MAX_DATE, TABLENAME from " + viewName);

            while (resultSet.next()) {
                final Timestamp minDateTime = resultSet.getTimestamp(1, utcCalendar);
                final Timestamp maxDateTime = resultSet.getTimestamp(2, utcCalendar);
                final String tableName = resultSet.getString(3);

                for (PTableEntry pte : ptes) {
                    if (tableName.equalsIgnoreCase(pte.tableName)) {
                        pte.startTime = getTimeInMilliSeconds(minDateTime);
                        pte.endTime = getTimeInMilliSeconds(maxDateTime);
                        LOG.finest("updateCacheWithDBValuesForRawEvents(): Table " + pte.storageID + " -> " + pte.tableName + " "
                                + dbUTCDateTimeFormat.format(new Date(pte.startTime)) + "(UTC)..."
                                + dbUTCDateTimeFormat.format(new Date(pte.endTime)) + "(UTC)");
                        break;
                    }
                }
            }
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Cleanup error", e);
            }

            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "Cleanup error", e);
                }
            }
        }
    }

    private long getTimeInMilliSeconds(final Timestamp timeStamp) {
        long time = 0L;
        if (timeStamp != null) {
            time = timeStamp.getTime();
        }
        return time;
    }

    /**
     * Updates the cache for the RAW Event tables. There is no need to update database. Database values are only read on creation of
     * PhysicalTableCache.
     * 
     * If startRow is zero, then partition roll-around has been detected.
     * 
     * @param storageID
     *            The storage ID
     * @param tableName
     *            the dwh table name
     * @param startTime
     *            the start time for the data
     * @param endTime
     *            the end time for hte data
     * @param startRow
     *            the data start row
     */
    public void updateCacheForRawEvents(final String storageID, final String tableName, final long startTime, final long endTime, final long startRow) {
        final List<PTableEntry> ptes = tableMap.get(storageID);

        if (ptes != null) {
            for (PTableEntry pte : ptes) {
                if (tableName.equalsIgnoreCase(pte.tableName)) {

                    // startRow = 0 means partition roll-around detected. Reset startTime and endTime
                    if (pte.startTime > startTime || startRow == 0) {
                        pte.startTime = startTime;
                    }

                    if (pte.endTime < endTime || startRow == 0 || pte.endTime == ENDOFTHEWORLD) {
                        pte.endTime = endTime;
                    }
                    LOG.finest("updateCacheForRawEvents(): Table " + pte.storageID + " -> " + pte.tableName + " "
                            + dbUTCDateTimeFormat.format(new Date(pte.startTime)) + "(UTC)..." + dbUTCDateTimeFormat.format(new Date(pte.endTime))
                            + "(UTC)");
                    break;
                }
            }
        }
    }

    /**
     * Get the Meta database info for a particular connection name
     * 
     * @param etlrep
     *            Db connection
     * @param conName
     *            connection name
     * @throws SQLException
     *             SQL Errors
     * @throws RockException
     *             Error connection to db
     * @throws ClassNotFoundException
     *             Failed to load sql driver
     * @return Connecion details to use to connect as <code>conName</code>
     */
    private Meta_databases getMetaDBInfoForConName(final RockFactory etlrep, final String conName) throws SQLException, RockException,
            ClassNotFoundException {
        final Meta_databases dbCond = new Meta_databases(etlrep);
        dbCond.setConnection_name(conName);
        dbCond.setType_name(USER);

        final Meta_databasesFactory dbFact = new Meta_databasesFactory(etlrep, dbCond);

        final List<Meta_databases> dbs = dbFact.get(); // NOPMD

        if (dbs == null || dbs.size() != 1) {
            LOG.severe(conName + " database not correctly defined in etlrep.Meta_databases.");
            throw new SQLException(conName + " database not correctly defined in etlrep.Meta_databases.");
        }

        final Meta_databases metaDatabase = dbs.get(0);

        Class.forName(metaDatabase.getDriver_name());
        return metaDatabase;
    }

    /**
     * Ensure Connection, Statement and ResultSet are closed correctly
     * 
     * @param con
     *            Db connection to close
     * @param statement
     *            the SQL Statement to close
     * @param result
     *            result set to close
     */
    private void cleanup(final Connection con, final Statement statement, final ResultSet result) {
        if (result != null) {
            try {
                result.close();
            } catch (Throwable e) {
                LOG.log(Level.WARNING, "Cleanup error", e);
            }
        }

        if (statement != null) {
            try {
                statement.close();
            } catch (Throwable e) {
                LOG.log(Level.WARNING, "Cleanup error", e);
            }
        }

        if (con != null) {
            try {
                con.close();
            } catch (Throwable e) {
                LOG.log(Level.WARNING, "Cleanup error", e);
            }
        }
    }

    /**
     * Returns all entries in cache internal format for specified storageID. ATTN: Do not change entries as changes are not automatically relayed to
     * database.
     * 
     * @param storageID
     *            the StorageId to lookup
     * @return List of entries for the StorageID
     */
    public List<PTableEntry> getEntries(final String storageID) {
        return tableMap.get(storageID);
    }

    /**
     * Internal class for representing configuration details of a partition.
     */
    public class PTableEntry implements Comparable<PTableEntry> {

        public String storageID;

        public String tableName;

        public long startTime;

        public long endTime;

        public String status;

        public long partitionsize;

        public long defaultpartitionsize;

        public Integer loadOrder;

        public PTableEntry() {

        }

        public PTableEntry(final PTableEntry pTableEntry) {
            storageID = pTableEntry.storageID;
            tableName = pTableEntry.tableName;
            startTime = pTableEntry.startTime;
            endTime = pTableEntry.endTime;
            status = pTableEntry.status;

            partitionsize = pTableEntry.partitionsize;
            defaultpartitionsize = pTableEntry.defaultpartitionsize;
            loadOrder = pTableEntry.loadOrder;
        }

        /**
         * Order PTableEntries based on loadOrder or tablename.
         */
        @Override
        public int compareTo(final PTableEntry cmp) {

            int ret = cmp.loadOrder.compareTo(this.loadOrder);

            if (ret == 0) {
                ret = cmp.tableName.compareTo(this.tableName);
            }

            return ret;
        }
    }

    /**
     * Resets the cache. Used for testing
     */
    public static void resetCache() {
        if (ptc != null) {
            ptc = null;
            ptc = new PhysicalTableCache();
        }
    }

    /**
     * Test method to initialise whole cache.
     * 
     * @param tableMap
     *            Test data
     */
    public static void testInit(final Map<String, List<PTableEntry>> tableMap) {
        if (ptc == null) {
            ptc = new PhysicalTableCache();
        }

        if (tableMap != null) {
            for (List<PTableEntry> pTableEntries : tableMap.values()) {
                Collections.sort(pTableEntries);
            }
        }
        ptc.tableMap = tableMap;
    }

    /**
     * A initialisation method for unit testing only, where this class is difficult because it has static accessor only.
     * 
     * @param storageID
     *            storage id
     * @param tableName
     *            table name
     * @param startTime
     *            start date
     * @param endTime
     *            end date
     * @param status
     *            status
     */
    public static void testInit(final String storageID, final String tableName, final long startTime, final long endTime, final String status) {
        if (ptc == null) {
            ptc = new PhysicalTableCache();
        }

        if (ptc.tableMap == null) {
            ptc.tableMap = new HashMap<String, List<PTableEntry>>();
        }

        List<PTableEntry> ptes = ptc.tableMap.get(storageID);
        if (ptes == null) {
            ptes = new ArrayList<PTableEntry>();
            ptc.tableMap.put(storageID, ptes);
        }

        final PTableEntry entry = ptc.new PTableEntry();
        entry.storageID = storageID;
        entry.tableName = tableName;
        entry.startTime = startTime;
        entry.endTime = endTime;
        entry.status = status;

        ptes.add(entry);

    }

}
