package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;

import java.io.*;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import org.junit.*;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.*;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.ericsson.eniq.common.HashIdCreator;
import com.ericsson.eniq.common.testutilities.DirectoryHelper;

public class UpdateHashIdsActionTest {

    private static final String PARTITION_5 = "EVENT_E_TEST_RAW_05";

    private static final String PARTITION_4 = "EVENT_E_TEST_RAW_04";

    private static final String PARTITION_3 = "EVENT_E_TEST_RAW_03";

    private static final String PARTITION_2 = "EVENT_E_TEST_RAW_02";

    private static final String PARTITION_1 = "EVENT_E_TEST_RAW_01";

    private static final String VENDOR_VALUE = "Ericsson";

    private static final String RAT_VALUE = "0";

    private static final String HIER1_VALUE = "CELL55";

    private static final String HIER2_VALUE = null;

    private static final String HIER3_VALUE = "BSC123";

    private static final String HIER321_ID_VALUE = null;

    private static final String HIER321_ID = "HIER321_ID";

    private static final String VENDOR = "VENDOR";

    private static final String HIERARCHY_1 = "HIERARCHY_1";

    private static final String HIERARCHY_2 = "HIERARCHY_2";

    private static final String HIERARCHY_3 = "HIERARCHY_3";

    private static final String RAT = "RAT";

    private static final String STRING_OF_COLUMNS = RAT + ", " + HIERARCHY_3 + ", " + HIERARCHY_2 + ", " + HIERARCHY_1 + ", " + VENDOR + ", "
            + HIER321_ID;

    private static final String TARGET_STORAGE_ID = "EVENT_E_TEST:RAW";

    private static RockFactory rockFact = null;

    private static final List<String> hashIdCols = new ArrayList<String>();

    private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "UpdateHashIdsActionTest");

    @BeforeClass
    public static void init() throws Exception {
        DirectoryHelper.mkdirs(TMP_DIR);
        Properties props = new Properties();
        props.put("updateHashIdsAction.loader.dir", TMP_DIR.getPath());
        props.put("updateHashIdsAction.loader.dir", TMP_DIR.getPath());
        props.put("updateHashIdsAction.temp.option", "");
        StaticProperties.giveProperties(props);
        Class.forName("org.hsqldb.jdbcDriver");
        rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
        final Statement stmt = rockFact.getConnection().createStatement();
        stmt.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID bigint, COLLECTION_SET_NAME VARCHAR(31), "
                + "DESCRIPTION VARCHAR(31), VERSION_NUMBER VARCHAR(31), ENABLED_FLAG VARCHAR(31), TYPE VARCHAR(31))");
        stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES ('1', 'TestTP', 'Description', '1', 'Y', 'type')");

        stmt.execute("create table META_DATABASES (USERNAME varchar(30), VERSION_NUMBER varchar(32), TYPE_NAME varchar(31), CONNECTION_ID numeric(38), "
                + "CONNECTION_NAME varchar(30), CONNECTION_STRING varchar(200), PASSWORD varchar(30), DESCRIPTION varchar(32000), "
                + "DRIVER_NAME varchar(100), DB_LINK_NAME varchar(128))");
        stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 1, 'dwh', 'jdbc:hsqldb:mem:testdbdwh', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver', null)");
        stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 2, 'dwhrep', 'jdbc:hsqldb:mem:testdb', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver', null)");
        stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'DBA', 1, 'dwh', 'jdbc:hsqldb:mem:testdbdwh', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver', null)");
        stmt.execute("create table dwhColumn (storageid varchar(31), dataname varchar(31))");
        stmt.execute("create table dwhPartition (storageid varchar(31), tableName varchar(31), starttime timestamp, endtime timestamp, status varchar(31), loadorder int)");
        stmt.close();
        hashIdCols.add(HIER321_ID);
    }

    @Test
    public void checkExceptionIsThrownWhenWhereClauseIsEmpty() {
        final Meta_transfer_actions act = new Meta_transfer_actions(rockFact);
        try {
            new UpdateHashIdsAction(new Meta_versions(rockFact), new Long(1), new Meta_collections(rockFact), new Long(1), new Long(1), new Long(1),
                    rockFact, act, Logger.getLogger("log"), new ConnectionPool(rockFact));
            fail("Should not get here.");
        } catch (Exception e) {
            assertTrue(e instanceof EngineMetaDataException);
            assertEquals("Target type not set", e.getMessage());
        }
    }

    @Test
    public void checkExceptionIsThrownWhenTargetStorageIdEmpty() {
        try {
            final Properties where_cond = new Properties();
            where_cond.setProperty("TEST", "test123");

            final ByteArrayOutputStream baoss = new ByteArrayOutputStream();
            where_cond.store(baoss, "");

            final Meta_transfer_actions act = new Meta_transfer_actions(rockFact);
            act.setWhere_clause(baoss.toString());

            new UpdateHashIdsAction(new Meta_versions(rockFact), new Long(1), new Meta_collections(rockFact), new Long(1), new Long(1), new Long(1),
                    rockFact, act, Logger.getLogger("log"), new ConnectionPool(rockFact));
            fail("Should not get here.");
        } catch (Exception e) {
            assertTrue(e instanceof EngineMetaDataException);
            assertEquals("Target Storage Id not set", e.getMessage());
        }
    }

    @Test
    public void checkGetStorageIdColumnsReturnsNull() throws Exception {
        final Meta_transfer_actions act = getMetaTransferActions();

        UpdateHashIdsAction hashIdAction = new UpdateHashIdsAction(new Meta_versions(rockFact), new Long(1), new Meta_collections(rockFact),
                new Long(1), new Long(1), new Long(1), rockFact, act, Logger.getLogger("log"), new ConnectionPool(rockFact));

        final Class<? extends UpdateHashIdsAction> bcdtClass = hashIdAction.getClass();
        final Method method = bcdtClass.getDeclaredMethod("getStorageIdColumns", new Class[] {});
        method.setAccessible(true);
        final String actual = (String) method.invoke(hashIdAction, new Object[] {});

        assertNull(actual);
    }

    @Test
    public void checkGetStorageIdColumnsReturnsCols() throws Exception {
        final Meta_transfer_actions act = getMetaTransferActions();
        insertDataIntoDwhColumn();

        UpdateHashIdsAction hashIdAction = new UpdateHashIdsAction(new Meta_versions(rockFact), new Long(1), new Meta_collections(rockFact),
                new Long(1), new Long(1), new Long(1), rockFact, act, Logger.getLogger("log"), new ConnectionPool(rockFact));

        final Class<? extends UpdateHashIdsAction> bcdtClass = hashIdAction.getClass();
        final Method method = bcdtClass.getDeclaredMethod("getStorageIdColumns", new Class[] {});
        method.setAccessible(true);
        final String actual = (String) method.invoke(hashIdAction, new Object[] {});

        assertEquals(STRING_OF_COLUMNS, actual);
    }

    @Test
    public void checkGetListOfPartitionsReturnsPartition() throws Exception {
        final Meta_transfer_actions act = getMetaTransferActions();
        insertDataIntoDwhPartition();

        UpdateHashIdsAction hashIdAction = new UpdateHashIdsAction(new Meta_versions(rockFact), new Long(1), new Meta_collections(rockFact),
                new Long(1), new Long(1), new Long(1), rockFact, act, Logger.getLogger("log"), new ConnectionPool(rockFact));

        final Class<? extends UpdateHashIdsAction> bcdtClass = hashIdAction.getClass();
        final Method method = bcdtClass.getDeclaredMethod("getListOfPartitions", new Class[] {});
        method.setAccessible(true);
        final List<Dwhpartition> actual = (List<Dwhpartition>) method.invoke(hashIdAction, new Object[] {});

        assertTrue(actual.size() == 5);
        assertEquals(PARTITION_1, actual.get(0).getTablename());
        assertEquals(PARTITION_2, actual.get(1).getTablename());
        assertEquals(PARTITION_3, actual.get(2).getTablename());
        assertEquals(PARTITION_4, actual.get(3).getTablename());
        assertEquals(PARTITION_5, actual.get(4).getTablename());
    }

    @Test
    public void checkGetHashIdColumnsReturnsCorrectCols() throws Exception {
        final Meta_transfer_actions act = getMetaTransferActions();
        insertDataIntoDwhColumn();

        UpdateHashIdsAction hashIdAction = new UpdateHashIdsAction(new Meta_versions(rockFact), new Long(1), new Meta_collections(rockFact),
                new Long(1), new Long(1), new Long(1), rockFact, act, Logger.getLogger("log"), new ConnectionPool(rockFact));

        final Class<? extends UpdateHashIdsAction> bcdtClass = hashIdAction.getClass();
        final Method method = bcdtClass.getDeclaredMethod("getHashIdColumns", new Class[] { String.class });
        method.setAccessible(true);
        final List<String> actual = (List<String>) method.invoke(hashIdAction, new Object[] { STRING_OF_COLUMNS });

        assertTrue(actual.size() == 1);
        assertEquals(HIER321_ID, actual.get(0));
    }

    @Test
    public void checkCorrectHashIdsArePutIntoTheFile() throws Exception {
        final Meta_transfer_actions act = getMetaTransferActions();

        UpdateHashIdsAction hashIdAction = new UpdateHashIdsAction(new Meta_versions(rockFact), new Long(1), new Meta_collections(rockFact),
                new Long(1), new Long(1), new Long(1), rockFact, act, Logger.getLogger("log"), new ConnectionPool(rockFact));
        createTargetTable(hashIdAction);
        final Class<? extends UpdateHashIdsAction> bcdtClass = hashIdAction.getClass();
        final Method method = bcdtClass.getDeclaredMethod("createFileWithNewHashIds", new Class[] { String.class, List.class, String.class });
        method.setAccessible(true);
        final File hashIdFile = (File) method.invoke(hashIdAction, new Object[] { STRING_OF_COLUMNS, hashIdCols, PARTITION_1 });
        String fileContent = getFileContentAsString(hashIdFile);
        hashIdFile.delete();

        // Check that column values are in the file
        String expected = RAT_VALUE + "|" + HIER3_VALUE + "|" + HIER2_VALUE + "|" + HIER1_VALUE + "|" + VENDOR_VALUE;
        assertTrue(fileContent.startsWith(expected));

        // Check that hashID value is in file and it correct
        long hashId = new HashIdCreator().hashStringToLongId(RAT_VALUE + "|" + HIER3_VALUE + "||" + HIER1_VALUE + "|" + VENDOR_VALUE);
        assertTrue(fileContent.endsWith(String.valueOf(hashId) + "|\n"));
        dropTargetTable(hashIdAction);
    }

    @Test
    @Ignore
    public void checkThatRowWithNoHashIsDeleted() throws Exception {
        final Meta_transfer_actions act = getMetaTransferActions();

        UpdateHashIdsAction hashIdAction = new UpdateHashIdsAction(new Meta_versions(rockFact), new Long(1), new Meta_collections(rockFact),
                new Long(1), new Long(1), new Long(1), rockFact, act, Logger.getLogger("log"), new ConnectionPool(rockFact));
        createTargetTable(hashIdAction);
        final Class<? extends UpdateHashIdsAction> bcdtClass = hashIdAction.getClass();
        final Method method = bcdtClass.getDeclaredMethod("deleteRowWithNoHashIds", new Class[] { List.class, String.class });
        method.setAccessible(true);
        method.invoke(hashIdAction, new Object[] { hashIdCols, PARTITION_1 });

        final Statement stmt = hashIdAction.getConnection().getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from " + PARTITION_1);
        // ResultSet should be empty
        assertFalse(rs.next());
        rs.close();
        stmt.close();

        dropTargetTable(hashIdAction);
    }

    private String getFileContentAsString(final File file) throws FileNotFoundException, IOException {
        final InputStream in = new FileInputStream(file);
        StringBuilder strBuilder = new StringBuilder();

        int ch;
        while ((ch = in.read()) > 0) {
            strBuilder.append((char) ch);
        }
        return strBuilder.toString();
    }

    private void createTargetTable(UpdateHashIdsAction hashIdAction) throws SQLException {
        final Statement stmt = hashIdAction.getConnection().getConnection().createStatement();
        try {
            stmt.execute("create table " + PARTITION_1 + " (RAT tinyint, " + HIERARCHY_3 + " varchar(128), " + HIERARCHY_2 + " varchar(128), "
                    + HIERARCHY_1 + " varchar(128), " + VENDOR + " varchar(50), " + HIER321_ID + " bigint)");
        } catch (Exception e) {
        }
        stmt.executeUpdate("insert into " + PARTITION_1 + " VALUES (" + RAT_VALUE + ", '" + HIER3_VALUE + "', " + HIER2_VALUE + ", '" + HIER1_VALUE
                + "', '" + VENDOR_VALUE + "', " + HIER321_ID_VALUE + ")");
        stmt.close();
    }

    private void dropTargetTable(UpdateHashIdsAction hashIdAction) throws SQLException {
        final Statement stmt = hashIdAction.getConnection().getConnection().createStatement();
        stmt.execute("drop table " + PARTITION_1);
        stmt.close();
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        final Meta_transfer_actions act = getMetaTransferActions();
        UpdateHashIdsAction hashIdAction = new UpdateHashIdsAction(new Meta_versions(rockFact), new Long(1), new Meta_collections(rockFact),
                new Long(1), new Long(1), new Long(1), rockFact, act, Logger.getLogger("log"), new ConnectionPool(rockFact));
        final Statement stmt = hashIdAction.getConnection().getConnection().createStatement();
        try {
            stmt.execute("drop table " + PARTITION_1);
        } catch (Exception e) {
        } finally {
            stmt.close();
        }
        DirectoryHelper.delete(TMP_DIR);
    }

    private void insertDataIntoDwhColumn() throws SQLException {
        final Statement stmt = rockFact.getConnection().createStatement();
        stmt.executeUpdate("insert into dwhColumn (storageid, dataname) values ('" + TARGET_STORAGE_ID + "', '" + RAT + "')");
        stmt.executeUpdate("insert into DWHColumn (storageid, dataname) values ('" + TARGET_STORAGE_ID + "', '" + HIERARCHY_3 + "')");
        stmt.executeUpdate("insert into DWHColumn (storageid, dataname) values ('" + TARGET_STORAGE_ID + "', '" + HIERARCHY_2 + "')");
        stmt.executeUpdate("insert into DWHColumn (storageid, dataname) values ('" + TARGET_STORAGE_ID + "', '" + HIERARCHY_1 + "')");
        stmt.executeUpdate("insert into DWHColumn (storageid, dataname) values ('" + TARGET_STORAGE_ID + "', '" + VENDOR + "')");
        stmt.executeUpdate("insert into DWHColumn (storageid, dataname) values ('" + TARGET_STORAGE_ID + "', '" + HIER321_ID + "')");
        stmt.close();
    }

    private void insertDataIntoDwhPartition() throws SQLException {
        final Statement stmt = rockFact.getConnection().createStatement();
        stmt.executeUpdate("insert into dwhPartition (storageid, tablename) values ('" + TARGET_STORAGE_ID + "', '" + PARTITION_1 + "')");
        stmt.executeUpdate("insert into dwhPartition (storageid, tablename) values ('" + TARGET_STORAGE_ID + "', '" + PARTITION_2 + "')");
        stmt.executeUpdate("insert into dwhPartition (storageid, tablename) values ('" + TARGET_STORAGE_ID + "', '" + PARTITION_3 + "')");
        stmt.executeUpdate("insert into dwhPartition (storageid, tablename) values ('" + TARGET_STORAGE_ID + "', '" + PARTITION_4 + "')");
        stmt.executeUpdate("insert into dwhPartition (storageid, tablename) values ('" + TARGET_STORAGE_ID + "', '" + PARTITION_5 + "')");
        stmt.close();
    }

    private static Meta_transfer_actions getMetaTransferActions() throws IOException {
        final Properties where_cond = new Properties();
        where_cond.setProperty("targetType", TARGET_STORAGE_ID);

        final ByteArrayOutputStream baoss = new ByteArrayOutputStream();
        where_cond.store(baoss, "");

        final Meta_transfer_actions act = new Meta_transfer_actions(rockFact);
        act.setWhere_clause(baoss.toString());
        return act;
    }
}
