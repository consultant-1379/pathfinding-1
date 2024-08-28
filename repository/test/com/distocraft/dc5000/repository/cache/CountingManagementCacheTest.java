/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.repository.cache;

import static com.ericsson.eniq.common.testutilities.RockDatabaseHelper.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.distocraft.dc5000.repository.dwhrep.Countingmanagement;
import com.ericsson.eniq.common.testutilities.RockDatabaseHelper;

/**
 * @author efinian
 * 
 */
public class CountingManagementCacheTest {

  private static final String storageID = "EVENT_E_SGEH_SUC:RAW";
  private static final String table_1 = "EVENT_E_SGEH_SUC_RAW_01";
  private static final String table_2 = "EVENT_E_SGEH_SUC_RAW_02";
  private static Statement dwhrep_stmt = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    RockDatabaseHelper.setUpBeforeClass();

    dwhrep_stmt = dwhRepConnection.createStatement();

    dwhrep_stmt
        .execute("create table DWHPartition (STORAGEID varchar(255), TABLENAME varchar(255), STARTTIME timestamp, ENDTIME timestamp, STATUS varchar(10), loadstatus varchar(10) )");
    dwhrep_stmt
        .executeUpdate("insert into DWHPartition (STORAGEID, TABLENAME, STARTTIME, ENDTIME, STATUS, loadstatus) values ('EVENT_E_SGEH_SUC:RAW', 'EVENT_E_SGEH_SUC_RAW_01', '2010-03-14 00:00:00.0', '2010-03-15 00:00:00.0', 'ACTIVE', 'EMPTY')");
    dwhrep_stmt
        .executeUpdate("insert into DWHPartition (STORAGEID, TABLENAME, STARTTIME, ENDTIME, STATUS, loadstatus) values ('EVENT_E_SGEH_SUC:RAW', 'EVENT_E_SGEH_SUC_RAW_02', '2010-03-14 00:00:00.0', '2010-03-15 00:00:00.0', 'ACTIVE', 'EMPTY')");

    dwhrep_stmt
        .execute("create table CountingManagement (STORAGEID varchar(255), TABLENAME varchar(255), LASTAGGREGATEDROW integer)");

    CountingManagementCache.initializeCache(etlrepRock);

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    dwhrep_stmt.execute("DROP TABLE Countingmanagement");
    dwhrep_stmt.execute("DROP TABLE DWHPartition");

    RockDatabaseHelper.tearDownAfterClass();
  }

  @Test
  public void queryAndInitializeTest() throws Exception {
    final Map<String, Countingmanagement> currentInfo = CountingManagementCache.getCountingManagementData(storageID);
    final Collection<Countingmanagement> currentInfoList = currentInfo.values();

    assertTrue("Expecting 2 partitions, got " + currentInfoList.size(), currentInfoList.size() == 2);
    for (final Countingmanagement info : currentInfoList) {
      assertTrue("Expecting row " + info, info.getLastaggregatedrow().intValue() == 0);
    }
  }

  @Test
  public void updateTest() throws Exception {
    final List<Countingmanagement> latestRowInfo = new ArrayList<Countingmanagement>();
    latestRowInfo.add(createAggInfo(table_1, new Integer(100)));

    CountingManagementCache.updateCountingManagementData(storageID, latestRowInfo);

    // Check the cache
    final Map<String, Countingmanagement> currentInfo = CountingManagementCache.getCountingManagementData(storageID);
    final Collection<Countingmanagement> currentInfoList = currentInfo.values();

    assertTrue("Expecting 2 partitions, got " + currentInfoList.size(), currentInfoList.size() == 2);
    for (final Countingmanagement info : currentInfoList) {
      if (table_1.equals(info.getTablename())) {
        assertTrue("Expecting 100 rows for " + table_1 + " got, " + info.getLastaggregatedrow(), new Long(100)
            .equals(info.getLastaggregatedrow()));
      } else {
        assertTrue("Expecting empty row " + info, info.getLastaggregatedrow().intValue() == 0);
      }
    }

    // Also, check in the DB.
    checkAggregationRowInDB(table_1, 100);
    checkAggregationRowNotInDB(table_2);
  }

  @Test
  public void partitionRollaround() throws Exception {
    // First update the row count
    final List<Countingmanagement> latestRowInfo = new ArrayList<Countingmanagement>();
    latestRowInfo.add(createAggInfo(table_1, new Integer(200)));
    latestRowInfo.add(createAggInfo(table_2, new Integer(100)));
    CountingManagementCache.updateCountingManagementData(storageID, latestRowInfo);

    // Not simulate roll-around by clearing row status on table_1
    CountingManagementCache.clearPartitionInfo(storageID, table_1);

    // Read results and validate
    final Map<String, Countingmanagement> currentInfo = CountingManagementCache.getCountingManagementData(storageID);
    final Collection<Countingmanagement> currentInfoList = currentInfo.values();

    assertTrue("Expecting 2 partitions, got " + currentInfoList.size(), currentInfoList.size() == 2);
    for (final Countingmanagement info : currentInfoList) {
      if (table_1.equals(info.getTablename())) {
        assertTrue("Expecting 0 rows for " + table_1 + " got, " + info.getLastaggregatedrow(), new Long(0).equals(info
            .getLastaggregatedrow()));
      } else if (table_2.equals(info.getTablename())) {
        assertTrue("Expecting 100 rows for " + table_2 + " got, " + info.getLastaggregatedrow(), new Long(100)
            .equals(info.getLastaggregatedrow()));
      } else {
        assertTrue("Unexpected row " + info.getTablename(), false);
      }
    }

    // Also, check in the DB.
    checkAggregationRowInDB(table_1, 0);
    checkAggregationRowInDB(table_2, 100);
  }

  @Test
  public void testClearCache() throws Exception {
    // Load cache
    final Map<String, Countingmanagement> currentInfo = CountingManagementCache.getCountingManagementData(storageID);
    final List<Countingmanagement> currentInfoList = new ArrayList<Countingmanagement>(currentInfo.values());

    currentInfoList.add(createAggInfo(table_2, new Integer(300)));
    CountingManagementCache.updateCountingManagementData(storageID, currentInfoList);

    // Clear cache
    CountingManagementCache.clearCache(storageID);

    // Change value in the DB
    setCountingManagementValueinDB(table_2, 999);

    // Check new value is picked up.
    final Map<String, Countingmanagement> currentInfoRes = CountingManagementCache.getCountingManagementData(storageID);
    Countingmanagement info = currentInfoRes.get(table_2);
    assertThat("table_2 count", info.getLastaggregatedrow(), is(999l));
  }

  @Test
  public void partitionRemove() throws Exception {
    // First update the row count
    final List<Countingmanagement> latestRowInfo = new ArrayList<Countingmanagement>();
    latestRowInfo.add(createAggInfo(table_1, new Integer(200)));
    latestRowInfo.add(createAggInfo(table_2, new Integer(100)));
    CountingManagementCache.updateCountingManagementData(storageID, latestRowInfo);

    // Not simulate roll-around by clearing row status on table_1
    CountingManagementCache.removePartitionInfo(storageID, table_1);

    // Read results and validate
    final Map<String, Countingmanagement> currentInfo = CountingManagementCache.getCountingManagementData(storageID);

    Countingmanagement countingInfo = currentInfo.get(table_1);
    assertTrue("table_1 should not be in cache, but it is.", countingInfo == null);

    // Also, check in the DB.
    checkAggregationRowNotInDB(table_1);
  }

  @Test
  public void deactivateTechPack() throws Exception {
    // Load cache
    final Map<String, Countingmanagement> currentInfo = CountingManagementCache.getCountingManagementData(storageID);
    final List<Countingmanagement> currentInfoList = new ArrayList<Countingmanagement>(currentInfo.values());

    currentInfoList.add(createAggInfo(table_2, new Integer(300)));
    CountingManagementCache.updateCountingManagementData(storageID, currentInfoList);

    // Delete cache & DB entries
    CountingManagementCache.deleteStorageDetails(storageID);

    // Check in DB
    checkAggregationRowNotInDB(table_1);
    checkAggregationRowNotInDB(table_2);
  }

  private void checkAggregationRowNotInDB(final String tableName) throws Exception {
    final ResultSet rs = dwhrep_stmt
        .executeQuery("select LASTAGGREGATEDROW from Countingmanagement where TABLENAME = '" + tableName + "'");
    if (rs.next()) {
      assertTrue("Found row in DB, Unexpectedly" + tableName, false);
    }
  }

  private void setCountingManagementValueinDB(final String tableName, int rowValue) throws Exception {
    final int updatedRows = dwhrep_stmt.executeUpdate("update Countingmanagement set  LASTAGGREGATEDROW = " + rowValue
        + " where TABLENAME = '" + tableName + "'");
    if (updatedRows == 0) {
      fail("Could not update row for countingmanagment table:" + tableName);
    }
  }

  private void checkAggregationRowInDB(final String tableName, final int expectedLastRow) throws Exception {
    final ResultSet rs = dwhrep_stmt
        .executeQuery("select LASTAGGREGATEDROW from Countingmanagement where TABLENAME = '" + tableName + "'");
    if (rs.next()) {
      final int currentRow = rs.getInt(1);
      assertTrue("Expecting " + expectedLastRow + " last row, got " + currentRow + " from db, for " + tableName,
          currentRow == expectedLastRow);
    } else {
      assertTrue("Missing expected row in DB: " + tableName, false);
    }
  }

  private Countingmanagement createAggInfo(final String tableName, final long lastRowId) {
    final Countingmanagement aggInfo = new Countingmanagement(dwhrepRock);
    aggInfo.setStorageid(storageID);
    aggInfo.setTablename(tableName);
    aggInfo.setLastaggregatedrow(lastRowId);
    aggInfo.setNewItem(false);
    return aggInfo;
  }

}
