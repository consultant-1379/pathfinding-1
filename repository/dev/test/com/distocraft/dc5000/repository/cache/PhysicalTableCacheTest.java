package com.distocraft.dc5000.repository.cache;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

import org.junit.*;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;

public class PhysicalTableCacheTest {

    private static RockFactory rockFact;

    private static Calendar minDateForTableRaw00;

    private static Calendar maxDateForTableRaw00;

    private static Calendar minDateForTableRaw01;

    private static Calendar maxDateForTableRaw01;

    private static Calendar minDateForTableRaw02;

    private static Calendar maxDateForTableRaw02;

    @BeforeClass
    public static void beforeClass() throws Exception {
        rockFact = DatabaseTestUtils.getTestDbConnection();
        DatabaseTestUtils.loadSetup(rockFact, "ptc");
        init_events_data();
        PhysicalTableCache.initialize(rockFact);
    }

    @AfterClass
    public static void afterClass() {
        DatabaseTestUtils.close(rockFact);
    }

    public static void init_events_data() throws Exception {
        minDateForTableRaw00 = getCalendar(2010, 1, 1, 0, 0);
        maxDateForTableRaw00 = getCalendar(2010, 2, 1, 0, 0);
        minDateForTableRaw01 = getCalendar(2010, 3, 1, 0, 0);
        maxDateForTableRaw01 = getCalendar(2010, 4, 1, 0, 0);
        minDateForTableRaw02 = getCalendar(2010, 4, 1, 0, 0);
        maxDateForTableRaw02 = getCalendar(2010, 5, 1, 0, 0);

        final Statement stmt = rockFact.getConnection().createStatement();

        stmt.execute("CREATE TABLE sys.systable (table_name varchar(32), creator varchar(12), table_type varchar(12))");
        stmt.execute("CREATE TABLE sys.sysuser (user_id varchar(12), user_name varchar(12))");
        stmt.execute("CREATE TABLE table_RAW_TIMERANGE (MIN_DATE timestamp, MAX_DATE timestamp, TABLENAME varchar(12))");

        stmt.executeUpdate("insert into DWHPartition VALUES('table:RAW', 'table_RAW_00', '" + new Timestamp(minDateForTableRaw00.getTimeInMillis())
                + "', '" + new Timestamp(maxDateForTableRaw00.getTimeInMillis()) + "', 'ACTIVE', 3)");
        stmt.executeUpdate("insert into DWHPartition VALUES('table:RAW', 'table_RAW_01', '" + new Timestamp(minDateForTableRaw01.getTimeInMillis())
                + "', '" + new Timestamp(maxDateForTableRaw01.getTimeInMillis()) + "', 'ACTIVE', 2)");
        stmt.executeUpdate("insert into DWHPartition VALUES('table:RAW', 'table_RAW_02', '" + new Timestamp(minDateForTableRaw02.getTimeInMillis())
                + "', '" + new Timestamp(maxDateForTableRaw02.getTimeInMillis()) + "', 'ACTIVE', 1)");

        stmt.executeUpdate("insert into sys.systable VALUES ('table_RAW_TIMERANGE', '1', 'VIEW')");
        stmt.executeUpdate("insert into sys.sysuser VALUES ('1', 'dc')");

        insertValuesIntoTimeRangeTable(stmt, new Timestamp(minDateForTableRaw00.getTimeInMillis()),
                new Timestamp(maxDateForTableRaw00.getTimeInMillis()), new Timestamp(minDateForTableRaw01.getTimeInMillis()), new Timestamp(
                        maxDateForTableRaw01.getTimeInMillis()), new Timestamp(minDateForTableRaw02.getTimeInMillis()), new Timestamp(
                        maxDateForTableRaw02.getTimeInMillis()));
        stmt.close();
    }

    @Test
    public void test_initialize2_Stats() {
        PhysicalTableCache.initialize(rockFact, DatabaseTestUtils.getTestDbUrl(), "SA", "", DatabaseTestUtils.getTestDbUrl(), "SA", "");
        final long endTime_1 = PhysicalTableCache.getCache().getEndTime("DC_E_SASN_TCP_CHARGING_RAW_01");
        final Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(endTime_1);
        int year = cal.get(Calendar.YEAR);
        assertEquals("Endtime is not correct for table DC_E_SASN_TCP_CHARGING_RAW_01", 1924, year);
    }

    @Test
    public void test_getActiveTables_Stats() {
        final String typeName = "DC_E_SASN_TCP_CHARGING";
        final String level = "RAW";
        final int tableCount = 3;
        final String storageId = typeName + ":" + level;

        final List<String> tables = PhysicalTableCache.getCache().getActiveTables(storageId);
        assertNotNull("Returned list should not be null", tables);
        assertFalse("Returned List should not be empty", tables.isEmpty());
        assertEquals("Table List is the wrong size", tableCount, tables.size());

        for (int i = 1; i <= tableCount; i++) {
            final String eTableName = typeName + "_" + level + "_0" + i;
            assertTrue("Table '" + eTableName + "' not found in returned list", tables.contains(eTableName));
        }
    }

    @Test
    public void test_getEndTime_Stats() {
        final long endTime_1 = PhysicalTableCache.getCache().getEndTime("DC_E_SASN_TCP_CHARGING_RAW_01");

        final Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(endTime_1);
        int year = cal.get(Calendar.YEAR);
        assertEquals("Endtime is not correct for table DC_E_SASN_TCP_CHARGING_RAW_01", 1924, year);

        final long endTime_3 = PhysicalTableCache.getCache().getEndTime("DC_E_SASN_TCP_CHARGING_RAW_03");
        cal.setTimeInMillis(endTime_3);
        year = cal.get(Calendar.YEAR);
        assertEquals("Endtime is not correct for table DC_E_SASN_TCP_CHARGING_RAW_03", 2099, year);

        final String ENDOFTHEWORLD = "ENDOFTHEWORLD";
        long endOfTime = -1;
        try {
            final Field eotw = PhysicalTableCache.getCache().getClass().getDeclaredField(ENDOFTHEWORLD);
            eotw.setAccessible(true);
            endOfTime = (Long) eotw.get(PhysicalTableCache.getCache());
        } catch (NoSuchFieldException e) {
            fail("Could not get ENDOFTHEWORLD value from PhysicalTableCache.class: Field " + ENDOFTHEWORLD + " not found in PhysicalTableCache ");
        } catch (IllegalAccessException e) {
            fail("Could not get ENDOFTHEWORLD value from PhysicalTableCache.class : " + e.toString());
        }
        final long endTime_2 = PhysicalTableCache.getCache().getEndTime("DC_E_SASN_TCP_CHARGING_RAW_02");
        assertEquals("Endtime is not correct for table DC_E_SASN_TCP_CHARGING_RAW_02", endOfTime, endTime_2);

        final long tnd = PhysicalTableCache.getCache().getEndTime("DOES_NOT_EXIST");
        assertEquals("Endtime is not correct for non existing table", -1, tnd);

    }

    @Test
    public void test_getStartTime_Stats() {
        final long startTime_1 = PhysicalTableCache.getCache().getStartTime("DC_E_SASN_TCP_CHARGING_RAW_01");
        final Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(startTime_1);
        int year = cal.get(Calendar.YEAR);
        assertEquals("Starttime is not correct for table DC_E_SASN_TCP_CHARGING_RAW_01", 1923, year);

        final long startTime_2 = PhysicalTableCache.getCache().getStartTime("DC_E_SASN_TCP_CHARGING_RAW_04");
        assertEquals("No Starttime should be defined for table DC_E_SASN_TCP_CHARGING_RAW_04", -1, startTime_2);
    }

    @Test
    public void test_getTableName_1_Stats() {
        final String typeName = "DC_E_SASN_TCP_CHARGING";
        final String level = "RAW";
        final String storageId = typeName + ":" + level;

        final Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 1923);
        String tableName = PhysicalTableCache.getCache().getTableName(storageId, cal.getTimeInMillis());
        assertEquals("Table name found was not correct", "DC_E_SASN_TCP_CHARGING_RAW_01", tableName);

        cal.set(Calendar.YEAR, 2000);
        tableName = PhysicalTableCache.getCache().getTableName(storageId, cal.getTimeInMillis());
        assertNull("No Table name should have been found", tableName);
    }

    @Test
    public void test_getTableNamesForRawEvents_Events() {
        final String typeName = "DC_E_SASN_TCP_CHARGING";
        final String level = "RAW";
        final String storageId = typeName + ":" + level;

        final Calendar start = Calendar.getInstance();
        start.set(Calendar.YEAR, 2000);
        final Calendar end = Calendar.getInstance();

        final List<String> tableNames = PhysicalTableCache.getCache().getTableNamesForRawEvents(storageId, start.getTimeInMillis(),
                end.getTimeInMillis());
        assertNotNull("Returned List should never be null", tableNames);
        assertEquals("Returned List is the wrong size", 1, tableNames.size());
        assertEquals("Wrong table returned in list", "DC_E_SASN_TCP_CHARGING_RAW_02", tableNames.get(0));

    }

    @Test
    public void test_resetCache_Events() {
        PhysicalTableCache.resetCache();
        try {
            PhysicalTableCache.getCache().getEndTime("DC_E_SASN_TCP_CHARGING_RAW_03");
            fail();
        } catch (NullPointerException e) {
            // OK
        }
        PhysicalTableCache.initialize(rockFact);
        final long endTime_3 = PhysicalTableCache.getCache().getEndTime("DC_E_SASN_TCP_CHARGING_RAW_03");
        final Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(endTime_3);
        final int year = cal.get(Calendar.YEAR);
        assertEquals("Endtime is not correct for table DC_E_SASN_TCP_CHARGING_RAW_03", 2099, year);
    }

    @Test
    public void test_getEarliestTimeForRawEvents_Events() {
        // events / stats : all the same w.r.t the cache
        final String typeName = "DC_E_SASN_TCP_CHARGING";
        final String level = "RAW";
        final String storageId = typeName + ":" + level;
        final long time = PhysicalTableCache.getCache().getEarliestTimeForRawEvents(storageId);
        final Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        assertEquals("Earliest YEAR value is wrong", 1923, cal.get(Calendar.YEAR));
        assertEquals("Earliest MONTH value is wrong", 1, cal.get(Calendar.MONTH) + 1);
        assertEquals("Earliest DAY_OF_MONTH value is wrong", 1, cal.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    public void test_updateLoadOrder_Events() throws RockException, SQLException {
        final String storageId = "DC_E_SASN_TCP_CHARGING:RAW";
        final String tableName = "DC_E_SASN_TCP_CHARGING_RAW_02";

        List<PhysicalTableCache.PTableEntry> partitions = PhysicalTableCache.getCache().getEntries(storageId);
        for (PhysicalTableCache.PTableEntry pte : partitions) {
            if (tableName.equals(pte.tableName)) {
                assertEquals("PreTest LOADORDER value is not correct", 0, pte.loadOrder.intValue());
                break;
            }
        }
        final int loadOrder = -45;
        PhysicalTableCache.getCache().updateLoadOrder(storageId, tableName, loadOrder);

        partitions = PhysicalTableCache.getCache().getEntries(storageId);
        for (PhysicalTableCache.PTableEntry pte : partitions) {
            if (tableName.equals(pte.tableName)) {
                assertEquals("Updated LOADORDER value is not correct", loadOrder, pte.loadOrder.intValue());
                break;
            }
        }
    }

    @Test
    public void test_getTableName_2_Stats() {
        final String typeName = "DC_E_SASN_TCP_CHARGING";
        final String level = "RAW";
        final String storageId = typeName + ":" + level;

        final Calendar start = Calendar.getInstance();
        start.set(Calendar.YEAR, 2000);
        final Calendar end = Calendar.getInstance();

        List<String> tableNames = PhysicalTableCache.getCache().getTableName(storageId, start.getTimeInMillis(), end.getTimeInMillis());
        assertNotNull("Returned List should never be null", tableNames);
        assertEquals("Returned List is the wrong size", 1, tableNames.size());
        assertEquals("Wrong table returned in list", "DC_E_SASN_TCP_CHARGING_RAW_02", tableNames.get(0));

        start.set(Calendar.YEAR, 1924);
        end.set(Calendar.YEAR, 1924);
        tableNames = PhysicalTableCache.getCache().getTableName(storageId, start.getTimeInMillis(), end.getTimeInMillis());
        assertNotNull("Returned List should neve be null", tableNames);
        assertEquals("Returned List is the wrong size", 1, tableNames.size());
        assertEquals("Wrong table returned in list", "DC_E_SASN_TCP_CHARGING_RAW_01", tableNames.get(0));

        start.set(Calendar.YEAR, 1920);
        end.set(Calendar.YEAR, 2100);
        tableNames = PhysicalTableCache.getCache().getTableName(storageId, start.getTimeInMillis(), end.getTimeInMillis());
        assertNotNull("Returned List should neve be null", tableNames);
        assertEquals("Returned List is the wrong size", 3, tableNames.size());
        for (int i = 1; i <= 3; i++) {
            final String eTableName = typeName + "_" + level + "_0" + i;
            assertTrue("Table '" + eTableName + "' not found in returned list", tableNames.contains(eTableName));
        }
    }

    @Test
    public void test_checkThatInitializeCacheForRawEventsUpdatesStartTimeAndEndTime_Events() throws SQLException, RockException,
            ClassNotFoundException {
        PhysicalTableCache.initialize(rockFact);
        final PhysicalTableCache ptc = PhysicalTableCache.getCache();

        final List<PhysicalTableCache.PTableEntry> list = createListOfEntriesForPhysicalTableCache(ptc);
        final Map<String, List<PhysicalTableCache.PTableEntry>> mp = new HashMap<String, List<PhysicalTableCache.PTableEntry>>();

        mp.put("table:RAW", list);
        PhysicalTableCache.testInit(mp);
        ptc.revalidateCacheForRawEvents(mp);

        // Check that startTime and endTime have been updated
        final PhysicalTableCache.PTableEntry tableRaw00 = list.get(0);
        assertEquals(tableRaw00.tableName, "table_RAW_00");
        assertEquals(tableRaw00.startTime, minDateForTableRaw00.getTimeInMillis());
        assertEquals(tableRaw00.endTime, maxDateForTableRaw00.getTimeInMillis());

        final PhysicalTableCache.PTableEntry tableRaw01 = list.get(1);
        assertEquals(tableRaw01.tableName, "table_RAW_01");
        assertEquals(tableRaw01.startTime, minDateForTableRaw01.getTimeInMillis());
        assertEquals(tableRaw01.endTime, maxDateForTableRaw01.getTimeInMillis());

        final PhysicalTableCache.PTableEntry tableRaw02 = list.get(2);
        assertEquals(tableRaw02.tableName, "table_RAW_02");
        assertEquals(tableRaw02.startTime, minDateForTableRaw02.getTimeInMillis());
        assertEquals(tableRaw02.endTime, maxDateForTableRaw02.getTimeInMillis());
    }

    @Test
    public void test_checkThatStartAndEndTimeEqualsZeroWhenValuesNULLInTimeRange_Events() throws SQLException, RockException, ClassNotFoundException {
        // Insert NULLs into TIMERANGE table
        final Statement stmt = rockFact.getConnection().createStatement();
        stmt.executeUpdate("insert into table_RAW_TIMERANGE VALUES(null, null, 'table_RAW_00')");
        stmt.executeUpdate("insert into table_RAW_TIMERANGE VALUES(null, null, 'table_RAW_01')");
        stmt.executeUpdate("insert into table_RAW_TIMERANGE VALUES(null, null, 'table_RAW_02')");
        stmt.close();

        PhysicalTableCache.initialize(rockFact);
        final PhysicalTableCache ptc = PhysicalTableCache.getCache();

        final List<PhysicalTableCache.PTableEntry> list = createListOfEntriesForPhysicalTableCache(ptc);
        final Map<String, List<PhysicalTableCache.PTableEntry>> mp = new HashMap<String, List<PhysicalTableCache.PTableEntry>>();

        mp.put("table:RAW", list);
        PhysicalTableCache.testInit(mp);
        ptc.revalidateCacheForRawEvents(mp);

        // Check that startTime and endTime are equal to zero
        final PhysicalTableCache.PTableEntry tableRaw00 = list.get(0);
        assertEquals(tableRaw00.tableName, "table_RAW_00");
        assertEquals(tableRaw00.startTime, 0);
        assertEquals(tableRaw00.endTime, 0);

        final PhysicalTableCache.PTableEntry tableRaw01 = list.get(1);
        assertEquals(tableRaw01.tableName, "table_RAW_01");
        assertEquals(tableRaw01.startTime, 0);
        assertEquals(tableRaw01.endTime, 0);

        final PhysicalTableCache.PTableEntry tableRaw02 = list.get(2);
        assertEquals(tableRaw02.tableName, "table_RAW_02");
        assertEquals(tableRaw02.startTime, 0);
        assertEquals(tableRaw02.endTime, 0);
    }

    @Test
    public void test_checkThatCacheIsUpdated_Events() throws SQLException, RockException, ClassNotFoundException {
        final int startRow = 123;
        PhysicalTableCache.initialize(rockFact);
        final PhysicalTableCache ptc = PhysicalTableCache.getCache();

        final List<PhysicalTableCache.PTableEntry> list = createListOfEntriesForPhysicalTableCache(ptc);
        final Map<String, List<PhysicalTableCache.PTableEntry>> mp = new HashMap<String, List<PhysicalTableCache.PTableEntry>>();

        mp.put("table:RAW", list);
        PhysicalTableCache.testInit(mp);

        final PhysicalTableCache.PTableEntry tableRaw00 = list.get(0);

        // Check that startTime and endTime are updated in cache
        ptc.updateCacheForRawEvents("table:RAW", "table_RAW_00", minDateForTableRaw00.getTimeInMillis(), maxDateForTableRaw00.getTimeInMillis(),
                startRow);
        assertEquals(tableRaw00.tableName, "table_RAW_00");
        assertEquals(tableRaw00.startTime, 0);
        assertEquals(tableRaw00.endTime, maxDateForTableRaw00.getTimeInMillis());

        // Check that startTime and endTime are always updated in cache when zero is passed in for startRow
        // Zero for startRow means partition has rolled over.
        ptc.updateCacheForRawEvents("table:RAW", "table_RAW_00", minDateForTableRaw00.getTimeInMillis(), maxDateForTableRaw00.getTimeInMillis(), 0);
        assertEquals(tableRaw00.tableName, "table_RAW_00");
        assertEquals(tableRaw00.startTime, minDateForTableRaw00.getTimeInMillis());
        assertEquals(tableRaw00.endTime, maxDateForTableRaw00.getTimeInMillis());

        final long newerMinDateForTableRaw00 = minDateForTableRaw00.getTimeInMillis() + 1000000;
        final long olderMaxDateForTableRaw00 = maxDateForTableRaw00.getTimeInMillis() + 1000000;
        // Check that startTime and endTime are updated in cache when:
        // 1. zero is passed in for startRow.
        // 2. startTime is newer than original startTime
        // 3. endTime is older then original endTime
        ptc.updateCacheForRawEvents("table:RAW", "table_RAW_00", newerMinDateForTableRaw00, olderMaxDateForTableRaw00, 0);
        assertEquals(tableRaw00.tableName, "table_RAW_00");
        assertEquals(tableRaw00.startTime, newerMinDateForTableRaw00);
        assertEquals(tableRaw00.endTime, olderMaxDateForTableRaw00);
    }

    @Test
    public void test_checkThatGetLatestTableNameForRawEventsIsCorrect_Events() {
        String storageId = "EVENT_E_SGEH_ERR:RAW";
        String tableName1 = "EVENT_E_SGEH_ERR_RAW_01";
        String tableName2 = "EVENT_E_SGEH_ERR_RAW_02";
        String tableName3 = "EVENT_E_SGEH_ERR_RAW_03";
        String tableName4 = "EVENT_E_SGEH_ERR_RAW_04";
        PhysicalTableCache.testInit(storageId, tableName1, 0L, 1L, "ACTIVE");
        // Should ignore DEACTIVE tables
        PhysicalTableCache.testInit(storageId, tableName2, 0L, 5L, "DEACTIVE");
        PhysicalTableCache.testInit(storageId, tableName3, 0L, 4L, "ACTIVE");
        PhysicalTableCache.testInit(storageId, tableName4, 0L, 2L, "ACTIVE");

        assertEquals(tableName3, PhysicalTableCache.getCache().getLatestTableNameForRawEvents(storageId));
    }

    /*****************************************************************************************************************/
    /******************************************** PRIVATE METHODS ****************************************************/
    /**
     * *************************************************************************************************************
     * 
     * @param ptc
     *            Stubbed Cache
     * @return Values
     */
    private List<PhysicalTableCache.PTableEntry> createListOfEntriesForPhysicalTableCache(final PhysicalTableCache ptc) {
        final List<PhysicalTableCache.PTableEntry> list = new ArrayList<PhysicalTableCache.PTableEntry>();
        for (int i = 0; i < 3; i++) {
            final PhysicalTableCache.PTableEntry entry1 = ptc.new PTableEntry();
            entry1.storageID = "table:RAW";
            entry1.tableName = "table_RAW_0" + i;
            entry1.status = "ACTIVE";
            entry1.partitionsize = 1000;
            entry1.defaultpartitionsize = 1000;
            entry1.loadOrder = 3 - i;
            assertEquals(entry1.startTime, 0);
            assertEquals(entry1.endTime, 0);
            list.add(entry1);
        }
        return list;
    }

    protected static Calendar getCalendar(final int year, final int month, final int day, final int hour, final int min) {
        final Calendar calendar = new GregorianCalendar();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, day);
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, min);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    private static void insertValuesIntoTimeRangeTable(final Statement stmt, final Timestamp minTimeStamp00, final Timestamp maxTimeStamp00,
                                                       final Timestamp minTimeStamp01, final Timestamp maxTimeStamp01,
                                                       final Timestamp minTimeStamp02, final Timestamp maxTimeStamp02) throws SQLException {
        stmt.executeUpdate("insert into table_RAW_TIMERANGE VALUES('" + minTimeStamp00 + "', '" + maxTimeStamp00 + "', 'table_RAW_00')");
        stmt.executeUpdate("insert into table_RAW_TIMERANGE VALUES('" + minTimeStamp01 + "', '" + maxTimeStamp01 + "', 'table_RAW_01')");
        stmt.executeUpdate("insert into table_RAW_TIMERANGE VALUES('" + minTimeStamp02 + "', '" + maxTimeStamp02 + "', 'table_RAW_02')");
    }
}
