package com.distocraft.dc5000.etl.engine.structure;

import static org.junit.Assert.*;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;

import org.junit.*;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.diskmanager.DirectoryDiskManagerAction;
import com.distocraft.dc5000.etl.engine.common.*;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.file.SQLInputFromFile;
import com.distocraft.dc5000.etl.engine.file.SQLOutputToFile;
import com.distocraft.dc5000.etl.engine.plugin.*;
import com.distocraft.dc5000.etl.engine.priorityqueue.PriorityQueue;
import com.distocraft.dc5000.etl.engine.sql.*;
import com.distocraft.dc5000.etl.engine.system.*;
import com.distocraft.dc5000.etl.mediation.MediationAction;
import com.distocraft.dc5000.etl.mediation.jdbc.JDBCMediationAction;
import com.distocraft.dc5000.etl.mediation.smtp.SMTPMediationAction;
import com.distocraft.dc5000.etl.monitoring.*;
import com.distocraft.dc5000.etl.rock.*;
import com.distocraft.dc5000.repository.cache.*;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
import com.ericsson.eniq.exception.ConfigurationException;

public class TransferActionTest extends UnitDatabaseTestCase {

    private static RockFactory etlrep = null;

    private static Meta_versions meta_versions = null;

    private static Meta_collections meta_collections = null;

    private static Meta_collection_sets meta_collection_sets = null;

    private static ConnectionPool connectionPool = null;

    private static Meta_transfer_actions metaTransferActions = null;

    private static PluginLoader pluginLoader = null;

    private static EngineCom engineCom = null;

    static long collectionSetId = 1;

    static long collectionId = 2;

    static long transferBatchId = 0;

    Long connectId = null;

    String batchColumnName = "";

    private final SetContext sctx = new SetContext();

    private final Logger clog = Logger.getLogger("TransferActionTest");

    private static final String testPluginName = "TestPlugin";

    private Map<String, Long> connectionMapping = null;

    @BeforeClass
    public static void beforeClass() throws IOException, ConfigurationException, NoSuchFieldException {
        setup(TestType.unit);
        etlrep = getRockFactory(Schema.etlrep);
        final Properties props = new Properties();
        final File sp = new File(System.getProperty("java.io.tmpdir"), "static.properties");
        if (!sp.exists() && !sp.createNewFile()) {
            Assert.fail("Failed to setup testcases, error creating file " + sp.getPath());
        }
        sp.deleteOnExit();
        props.put("SessionHandling.storageFile", sp.getPath());
        props.put("SessionHandling.log.types", "");
        StaticProperties.giveProperties(props);
        SessionHandler.init();
        connectionPool = new ConnectionPool(etlrep);
        meta_collections = new Meta_collections(etlrep);
        meta_collections.setCollection_set_id(collectionSetId);
        meta_collections.setCollection_id(collectionId);

        meta_collection_sets = new Meta_collection_sets(etlrep);
        meta_collection_sets.setCollection_set_id(collectionSetId);
        meta_collection_sets.setCollection_set_name("DC_E_TEST");

        meta_versions = new Meta_versions(etlrep);
        meta_versions.setVersion_number("0");
        pluginLoader = new PluginLoader("/") {
            @Override
            public Class<?> loadClass(final String className) throws ClassNotFoundException {
                if (testPluginName.equals(className)) {
                    return super.loadClass(TestPlugin.class.getName());
                } else {
                    return super.loadClass(className);
                }
            }
        };
        engineCom = new EngineCom();
        setupDataFormatCache();
    }

    @Before
    public void before() {
        // Stop ExecutionProfileAction from reloading config from file (if called)
        SlotRebuilder.setReloadConfig(false);

        connectionMapping = truncateSchemaTables(Schema.etlrep, true);
        loadSqlFile("setupSQL/TransferActionTest/etlrep.sql", Schema.etlrep);
    }

    @Test
    public void test_create_PartitionedLoader() throws Exception {
        checkActionCreation("Loader", PartitionedLoader.class);
        checkActionCreation("Partitioned Loader", PartitionedLoader.class);
    }

    @Test
    public void test_create_UnPartitionedLoader() throws Exception {
        checkActionCreation("UnPartitioned Loader", UnPartitionedLoader.class);
    }

    @Test
    public void test_create_UpdateDIMSession() throws Exception {
        checkActionCreation("UpdateDimSession", UpdateDIMSession.class);
    }

    @Test
    public void test_create_Parse() throws Exception {
        checkActionCreation("Parse", Parse.class);
    }

    @Test
    public void test_create_UncompressorAction() throws Exception {
        checkActionCreation("Uncompress", "com.distocraft.dc5000.uncompress.UncompressorAction");
    }

    @Test
    public void test_create_SystemCall() throws Exception {
        checkActionCreation("System Call", SystemCall.class);
        checkActionCreation("System call", SystemCall.class);
    }

    @Test
    public void test_create_LogSessionLoader() throws Exception {
        checkActionCreation("SessionLog Loader", LogSessionLoader.class);
    }

    @Test
    public void test_create_AggregationAction() throws Exception {
        checkActionCreation("Aggregator", AggregationAction.class);
        checkActionCreation("Aggregation", AggregationAction.class);
    }

    @Test
    public void test_create_UpdateMonitoringAction() throws Exception {
        checkActionCreation("UpdateMonitoring", UpdateMonitoringAction.class);
    }

    @Test
    public void test_create_AutomaticAggregationAction() throws Exception {
        checkActionCreation("AutomaticAggregation", AutomaticAggregationAction.class);
    }

    @Test
    public void test_create_AutomaticReAggregationAction() throws Exception {
        checkActionCreation("AutomaticREAggregati", AutomaticReAggregationAction.class);
    }

    @Test
    public void test_create_ManualReAggregationAction() throws Exception {
        checkActionCreation("ManualReAggregation", ManualReAggregationAction.class);
    }

    @Test
    public void test_create_SQLExecute() throws Exception {
        checkActionCreation("SQL Execute", SQLExecute.class);
    }

    @Test
    public void test_create_CreateDirAction() throws Exception {
        checkActionCreation("CreateDir", CreateDirAction.class);
    }

    @Test
    public void test_create_Distribute() throws Exception {
        checkActionCreation("Distribute", Distribute.class);
    }

    @Test
    public void test_create_JVMMonitorAction() throws Exception {
        checkActionCreation("JVMMonitor", JVMMonitorAction.class);
    }

    @Test
    public void test_create_DiskManagerAction() throws Exception {
        checkActionCreation("Diskmanager", "com.distocraft.dc5000.diskmanager.DiskManagerAction");
    }

    @Test
    public void test_create_DirectoryDiskManagerAction() throws Exception {
        checkActionCreation("DirectoryDiskmanager", DirectoryDiskManagerAction.class);
    }

    @Test
    public void test_create_TriggerAction() throws Exception {
        checkActionCreation("Trigger", TriggerAction.class);
    }

    @Test
    public void test_create_SetContextTriggerAction() throws Exception {
        checkActionCreation("SetContextTrigger", SetContextTriggerAction.class);
    }

    @Test
    public void test_create_TriggerSetListInSchedulerAction() throws Exception {
        checkActionCreation("TriggerScheduledSet", TriggerSetListInSchedulerAction.class);
    }

    @Test
    public void test_create_GateKeeperAction() throws Exception {
        checkActionCreation("GateKeeper", GateKeeperAction.class);
    }

    @Test
    public void test_create_BatchUpdateMonitoringAction() throws Exception {
        checkActionCreation("BatchUpdateMonitorin", BatchUpdateMonitoringAction.class);
    }

    @Test
    public void test_create_ImportAction() throws Exception {
        checkActionCreation("import", ImportAction.class);
    }

    @Test
    public void test_create_ExportAction() throws Exception {
        checkActionCreation("export", ExportAction.class);
    }

    @Test
    public void test_create_MediationAction() throws Exception {
        checkActionCreation("Mediation", MediationAction.class);
    }

    @Test
    public void test_create_JDBCMediationAction() throws Exception {
        checkActionCreation("JDBC Mediation", JDBCMediationAction.class);
    }

    @Test
    public void test_create_AlarmHandlerActionWrapper() throws Exception {
        checkActionCreation("AlarmHandler", AlarmHandlerActionWrapper.class);
    }

    @Test
    public void test_create_AlarmMarkupActionWrapper() throws Exception {
        checkActionCreation("AlarmMarkup", AlarmMarkupActionWrapper.class);
    }

    @Test
    public void test_create_DWHMUpdatePlanAction() throws Exception {
        checkActionCreation("UpdatePlan", DWHMUpdatePlanAction.class);
    }

    @Test
    public void test_create_SMTPMediationAction() throws Exception {
        checkActionCreation("SMTP Mediation", SMTPMediationAction.class);
    }

    @Test
    public void test_create_SystemMonitoringAction() throws Exception {
        checkActionCreation("System Monitor", "com.distocraft.dc5000.etl.monitoring.SystemMonitorAction");
    }

    @Test
    public void test_create_DuplicateCheckAction() throws Exception {
        checkActionCreation("DuplicateCheck", DuplicateCheckAction.class);
    }

    @Test
    public void test_create_SQLExtract() throws Exception {
        checkActionCreation("SQL Extract", SQLExtract.class);
    }

    @Test
    public void test_create_ChangeProfileAction() throws Exception {
        checkActionCreation("ChangeProfile", ChangeProfileAction.class);
    }

    @Test
    public void test_create_ReloadPropertiesAction() throws Exception {
        checkActionCreation("reloadProperties", ReloadPropertiesAction.class);
        checkActionCreation("ReloadProperties", ReloadPropertiesAction.class);
    }

    @Test
    public void test_create_DWHMPartitionAction() throws Exception {
        checkActionCreation("PartitionAction", DWHMPartitionAction.class);
    }

    @Test
    public void test_create_DWHMStorageTimeAction() throws Exception {
        checkActionCreation("StorageTimeAction", DWHMStorageTimeAction.class);
    }

    @Test
    public void test_create_DWHSanityCheckerAction() throws Exception {
        final String actionType = "SanityCheck";
        final Meta_transfer_actions action = insertAction(10, actionType);
        final Properties props = new Properties();
        props.put("mode", "ALL");
        action.setAction_contents(TransferActionBase.propertiesToString(props));
        action.saveToDB();
        checkActionCreation(actionType, action, DWHSanityCheckerAction.class);
    }

    @Test
    public void test_create_DWHMCreateViewsAction() throws Exception {
        loadDefaultTechpack(TechPack.stats, "v1");
        try {
            final Dwhtype type = new Dwhtype(getRockFactory(Schema.dwhrep));
            type.setTechpack_name("DC_E_TEST");
            type.setType("gg");
            type.setTablelevel("R");
            type.setTypename("f");
            type.setStorageid("sid");
            type.setPartitionsize((long) 1);
            type.setStatus("ACTIVE");
            type.setViewtemplate("");
            type.setCreatetemplate("");
            type.setBasetablename("t");
            type.saveDB();
            checkActionCreation("CreateViews", DWHMCreateViewsAction.class);
        } finally {
            truncateSchemaTables(Schema.dwhrep, false);
        }
    }

    @Test
    public void test_create_DWHMVersionUpdateAction() throws Exception {
        checkActionCreation("VersionUpdate", DWHMVersionUpdateAction.class);
    }

    @Test
    public void test_create_UpdateMonitoredTypes() throws Exception {
        checkActionCreation("UpdateMonitoredTypes", UpdateMonitoredTypes.class);
    }

    @Test
    public void test_create_TableCleaner() throws Exception {
        checkActionCreation("TableCleaner", TableCleaner.class);
    }

    @Test
    public void test_create_SetTypeTriggerAction() throws Exception {
        checkActionCreation("SetTypeTrigger", SetTypeTriggerAction.class);
    }

    @Test
    public void test_create_DWHMTableCheckAction() throws Exception {
        checkActionCreation("TableCheck", DWHMTableCheckAction.class);
    }

    @Test
    public void test_create_AggregationRuleCopy() throws Exception {
        checkActionCreation("AggregationRuleCopy", AggregationRuleCopy.class);
    }

    @Test
    public void test_create_PartitionedSQLExecute() throws Exception {
        final String actionType = "PartitionedSQLExec";
        final Meta_transfer_actions action = insertAction(10, actionType);
        final Properties props = new Properties();
        props.put("typeName", "wwwww");
        props.put("useOnlyLoadedPartitions", "1");
        action.setWhere_clause(TransferActionBase.propertiesToString(props));
        action.saveToDB();
        checkActionCreation(actionType, action, PartitionedSQLExecute.class);
    }

    @Test
    public void test_create_SQLLogResultSet() throws Exception {
        checkActionCreation("SQLLogResultSet", SQLLogResultSet.class);
    }

    @Test
    public void test_create_ReloadDBLookupsAction() throws Exception {
        checkActionCreation("ReloadDBLookups", ReloadDBLookupsAction.class);
    }

    @Test
    public void test_create_ReloadTransformationsAction() throws Exception {
        checkActionCreation("ReloadTransformation", ReloadTransformationsAction.class);
    }

    @Test
    public void test_create_ExecutionProfilerAction() throws Exception {
        checkActionCreation("ExecutionProfiler", ExecutionProfilerAction.class);
    }

    @Test
    public void test_create_InvalidateDBLookupCache() throws Exception {
        checkActionCreation("RefreshDBLookup", InvalidateDBLookupCache.class);
    }

    @Test
    public void test_create_SQLJoin() throws Exception {
        final String actionType = "SQLJoiner";
        final Meta_transfer_actions action = insertAction(10, actionType);
        final Properties props = new Properties();
        props.put("typeName", "wwwww");
        action.setWhere_clause(TransferActionBase.propertiesToString(props));
        action.saveToDB();

        loadDefaultTechpack(TechPack.stats, "v1");
        try {
            final Dwhtype type = new Dwhtype(getRockFactory(Schema.dwhrep));
            type.setTechpack_name("DC_E_TEST");
            type.setType("gg");
            type.setTablelevel("R");
            type.setTypename("f");
            type.setStorageid("sid");
            type.setPartitionsize((long) 1);
            type.setStatus("ACTIVE");
            type.setViewtemplate("");
            type.setCreatetemplate("");
            type.setBasetablename("wwwww");
            type.saveDB();
            checkActionCreation(actionType, action, SQLJoin.class);
        } finally {
            truncateSchemaTables(Schema.dwhrep, false);
        }
    }

    @Test
    public void test_create_EBSUpdateAction() throws Exception {
        checkActionCreation("EBSUpdate", EBSUpdateAction.class);
    }

    @Test
    public void test_create_EventLoader() throws Exception {
        checkActionCreation("EventLoader", "com.distocraft.dc5000.etl.engine.sql.EventLoader");
    }

    @Test
    public void test_create_StoreCountingManagementDataAction() throws Exception {
        checkActionCreation("StoreCountingData", "com.distocraft.dc5000.etl.engine.sql.StoreCountingManagementDataAction");
    }

    @Test
    public void test_create_CountingIntervalsActionclass() throws Exception {
        checkActionCreation("CountingIntervals", "com.distocraft.dc5000.etl.engine.sql.CountingIntervalsAction");
    }

    @Test
    public void test_create_CountingTrigger() throws Exception {
        final String actionType = "CountingTrigger";
        final Meta_transfer_actions action = insertAction(10, actionType, Schema.dwhrep);
        final Properties props = new Properties();
        props.put("setName", "wwwww");
        props.put("timelevels", "1");
        action.setWhere_clause(TransferActionBase.propertiesToString(props));
        action.saveToDB();
        checkActionCreation(actionType, action, "com.distocraft.dc5000.etl.engine.sql.CountingTrigger");
    }

    @Test
    public void test_create_CountingDayTrigger() throws Exception {
        final String actionType = "CountingDayTrigger";
        final Meta_transfer_actions action = insertAction(10, actionType, Schema.dwhrep);
        final Properties props = new Properties();
        props.put("setName", "wwwww");
        props.put("timelevels", "1");
        action.setWhere_clause(TransferActionBase.propertiesToString(props));
        action.saveToDB();
        checkActionCreation(actionType, action, "com.distocraft.dc5000.etl.engine.sql.CountingDayTrigger");
    }

    @Test
    public void test_create_UpdateCountIntervalsOnUpgrade() throws Exception {
        final String actionType = "UpdateCountIntervals";
        final Meta_transfer_actions action = insertAction(10, actionType, Schema.dwhrep);
        final Properties props = new Properties();
        props.put("targetType", "wwwww");
        action.setWhere_clause(TransferActionBase.propertiesToString(props));
        action.saveToDB();
        checkActionCreation(actionType, action, "com.distocraft.dc5000.etl.engine.sql.UpdateCountIntervalsOnUpgrade");
    }

    @Test
    public void test_create_CountingReAggAction() throws Exception {
        final String actionType = "CountReAggAction";
        final Meta_transfer_actions action = insertAction(10, actionType, Schema.dwhrep);
        final Properties props = new Properties();
        props.put("minTimestamp", "1234-01-01 01:01:01");
        props.put("maxTimestamp", "1234-01-01 01:01:01");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        checkActionCreation(actionType, action, "com.distocraft.dc5000.etl.engine.sql.CountingReAggAction");
    }

    @Test
    public void test_create_BackupTrigger() throws Exception {
        checkActionCreation("BackupTrigger", "com.distocraft.dc5000.etl.engine.sql.BackupTrigger");
    }

    @Test
    public void test_create_BackupCountDayTables() throws Exception {
        final Properties props = new Properties();
        props.put("lockTable", "lockTable");
        props.put("aggDate", "0");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);
        checkActionCreation("BackupCountDay", "com.ericsson.eniq.engine.backup.BackupCountDayTables");
    }

    @Test
    public void test_create_BackupCountDayTablesUsingFactory() throws Exception {
        final Properties props = new Properties();
        props.put("lockTable", "lockTable");
        props.put("aggDate", "0");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        //tablename ending with ":DAY" should return BackupCountDayTables
        metaTransferActions.setWhere_clause("tablename=EVENT_E_SGEH_TEST:DAY");
        metaTransferActions.setAction_type("BackupTables");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.ericsson.eniq.engine.backup.BackupCountDayTables"));
    }

    @Test
    public void test_create_BackupVolBasedTablesUsingFactory() throws Exception {
        final Properties props = new Properties();
        props.put("lockTable", "lockTable");
        props.put("aggDate", "0");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        //tablename ending with ":RAW" should return BackupVolBasedTables
        metaTransferActions.setWhere_clause("tablename=EVENT_E_SGEH_TEST:RAW\nisVolBasedPartition=true");
        metaTransferActions.setAction_type("BackupTables");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.ericsson.eniq.engine.backup.BackupVolBasedTables"));
    }

    @Test
    public void test_create_BackupFactoryThrowsExceptionForUnknownStorageId() throws Exception {
        final Properties props = new Properties();
        props.put("lockTable", "lockTable");
        props.put("aggDate", "0");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        metaTransferActions.setWhere_clause("tablename=EVENT_E_SGEH_TEST");
        metaTransferActions.setAction_type("BackupTables");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        try {
            new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                    metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                    sctx, clog, engineCom, null);
            fail("should not get here..");
        } catch (Exception e) {
            assertTrue(e instanceof EngineMetaDataException);
            assertEquals("Error initializing action: \"BackupTables\"", e.getMessage());
        }

    }

    @Test
    public void test_create_BackupLoaderUsingFactory() throws Exception {
        final Properties props = new Properties();
        props.put("lockTable", "lockTable");
        props.put("aggDate", "0");
        props.put("fromToDates", "2012:01:01, 2012:01:02");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        //tablename ending with ":DAY" should return BackupCountDayTables
        metaTransferActions.setWhere_clause("tablename=EVENT_E_SGEH_TEST:DAY");
        metaTransferActions.setAction_type("Restore");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.ericsson.eniq.engine.backup.BackupLoader"));
    }

    @Test
    public void test_create_BackupVolBasedLoaderUsingFactory() throws Exception {
        // Setup properties
        final String eniqConfDirectory = System.getenv().get("WORKSPACE").toLowerCase() + File.separator + "eniqConf";
        final String EVENTS_BACKUP_DIR = System.getenv().get("WORKSPACE").toLowerCase() + File.separator + "/eniq/backup";
        System.setProperty("dc5000.config.directory", eniqConfDirectory);
        createEmptyFile(eniqConfDirectory, "ETLCServer.properties");
        final File propertiesFile = createEmptyFile(eniqConfDirectory, "niq.rc");
        addPropertyToFile("EVENTS_BACKUP_DIR", EVENTS_BACKUP_DIR, propertiesFile);
        new File(EVENTS_BACKUP_DIR).mkdirs();

        final Properties props = new Properties();
        props.put("lockTable", "lockTable");
        props.put("aggDate", "0");
        props.put("fromToDates", "2012:01:01, 2012:01:02");

        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        //tablename ending with ":RAW" should return BackupVolBasedTables
        metaTransferActions.setWhere_clause("tablename=EVENT_E_SGEH_TEST:RAW\nisVolBasedPartition=true");
        metaTransferActions.setAction_type("Restore");
        metaTransferActions.setEnabled_flag("Y");

        PhysicalTableCache.testInit("EVENT_E_SGEH_TEST:RAW", "EVENT_E_SGEH_ERR_RAW_01", 0L, 0L, "ACTIVE");

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.ericsson.eniq.engine.backup.BackupVolBasedLoader"));
    }

    @Test
    public void test_create_BackupLoaderFactoryThrowsExceptionForUnknownStorageId() throws Exception {
        final Properties props = new Properties();
        props.put("lockTable", "lockTable");
        props.put("aggDate", "0");
        props.put("fromToDates", "2012:01:01, 2012:01:02");

        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        metaTransferActions.setWhere_clause("tablename=EVENT_E_SGEH_TEST");
        metaTransferActions.setAction_type("Restore");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        try {
            new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                    metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                    sctx, clog, engineCom, null);
            fail("should not get here..");
        } catch (Exception e) {
            assertTrue(e instanceof EngineMetaDataException);
            assertEquals("Error initializing action: \"Restore\"", e.getMessage());
        }

    }

    @Test
    public void test_create_BackupLoader() throws Exception {
        final Properties props = new Properties();
        props.put("lockTable", "lockTable_RAW");
        props.put("fromToDates", "2010:05:23, 2010:05:23");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        checkActionCreation("BackupLoader", "com.ericsson.eniq.engine.backup.BackupLoader");
    }

    @Test
    public void test_create_EventLoaderUsingFactory() throws Exception {
        final Properties props = new Properties();

        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        metaTransferActions.setAction_type("EventLoader");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.distocraft.dc5000.etl.engine.sql.EventLoader"));
    }

    @Test
    public void test_create_EventSnappyPipeLoaderUsingFactory() throws Exception {
        final Properties props = new Properties();
        props.put("useNamedPipe", "true");
        props.put("useSnappy", "true");
        props.put("isParallelLoadAllowed", "false");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(new Properties()));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        metaTransferActions.setWhere_clause(TransferActionBase.propertiesToString(props));
        metaTransferActions.setAction_type("EventLoader");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.distocraft.dc5000.etl.engine.sql.EventSnappyPipeLoader"));
    }

    @Test
    public void test_create_EventPipeLoaderUsingFactory() throws Exception {
        final Properties props = new Properties();
        props.put("useNamedPipe", "true");
        props.put("isParallelLoadAllowed", "false");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(new Properties()));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        metaTransferActions.setWhere_clause(TransferActionBase.propertiesToString(props));
        metaTransferActions.setAction_type("EventLoader");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.distocraft.dc5000.etl.engine.sql.EventPipeLoader"));
    }

    @Test
    public void test_create_EventParallelPipeLoaderUsingFactory() throws Exception {
        final Properties props = new Properties();
        props.put("useNamedPipe", "true");
        props.put("isParallelLoadAllowed", "true");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(new Properties()));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        metaTransferActions.setWhere_clause(TransferActionBase.propertiesToString(props));
        metaTransferActions.setAction_type("EventLoader");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.distocraft.dc5000.etl.engine.sql.EventParallelPipeLoader"));
    }

    @Test
    public void test_create_TimeBasePartitionLoaderUsingFactory() throws Exception {
        final Properties props = new Properties();

        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(props));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        metaTransferActions.setAction_type("TimeBase EventLoader");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.distocraft.dc5000.etl.engine.sql.TimeBasePartitionLoader"));
    }

    @Test
    public void test_create_TimeBasePartitionPipeLoaderUsingFactory() throws Exception {
        final Properties props = new Properties();
        props.put("useNamedPipe", "true");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(new Properties()));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        metaTransferActions.setWhere_clause(TransferActionBase.propertiesToString(props));
        metaTransferActions.setAction_type("TimeBase EventLoader");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.distocraft.dc5000.etl.engine.sql.TimeBasePartitionPipeLoader"));
    }

    @Test
    public void test_create_TimeBasePartitionSnappyPipeLoaderUsingFactory() throws Exception {
        final Properties props = new Properties();
        props.put("useNamedPipe", "true");
        props.put("useSnappy", "true");
        meta_collections.setScheduling_info(TransferActionBase.propertiesToString(new Properties()));
        metaTransferActions = new Meta_transfer_actions(etlrep);
        metaTransferActions.setWhere_clause(TransferActionBase.propertiesToString(props));
        metaTransferActions.setAction_type("TimeBase EventLoader");
        metaTransferActions.setEnabled_flag("Y");

        final Map<String, List<PhysicalTableCache.PTableEntry>> testCache = new HashMap<String, List<PhysicalTableCache.PTableEntry>>(1);
        PhysicalTableCache.testInit(testCache);

        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                metaTransferActions.getTransfer_action_id(), metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader,
                sctx, clog, engineCom, null);

        assertTrue(transferAction.trBaseAction.getClass() == Class.forName("com.distocraft.dc5000.etl.engine.sql.TimeBasePartitionSnappyPipeLoader"));
    }

    @Test
    public void test_create_CountingAction() throws Exception {
        checkActionCreation("CountingAction", "com.distocraft.dc5000.etl.engine.sql.CountingAction");
    }

    @Test
    public void test_create_CountingDayAction() throws Exception {
        checkActionCreation("CountingDayAction", "com.distocraft.dc5000.etl.engine.sql.CountingDayAction");
    }

    @Test
    public void test_create_CreateCollectedDataFilesAction() throws Exception {
        checkActionCreation("CreateCollectedData", CreateCollectedDataFilesAction.class);
    }

    @Test
    public void test_create_UpdateCollectedDataAction() throws Exception {
        checkActionCreation("UpdateCollectedData", UpdateCollectedDataAction.class);
    }

    @Test
    public void test_create_TopologySQLExecute() throws Exception {
        final String actionType = "TopologySqlExecute";
        final Meta_transfer_actions action = insertAction(10, actionType, Schema.dwhrep);
        final Properties props = new Properties();
        props.put("tableName", "wwwww");
        action.setWhere_clause(TransferActionBase.propertiesToString(props));
        action.saveToDB();
        checkActionCreation(actionType, action, "com.distocraft.dc5000.etl.engine.sql.TopologySQLExecute");
    }

    @Test
    public void test_create_HistorySQLExecute() throws Exception {
        final String actionType = "HistorySqlExecute";
        final Meta_transfer_actions action = insertAction(10, actionType, Schema.dwhrep);
        final Properties props = new Properties();
        props.put("tableName", "wwwww");
        action.setWhere_clause(TransferActionBase.propertiesToString(props));
        action.saveToDB();
        checkActionCreation(actionType, action, "com.distocraft.dc5000.etl.engine.sql.HistorySQLExecute");
    }

    @Test
    public void test_create_UnknownTopologySQLExecute() throws Exception {
        checkActionCreation("UnknownTopology", "com.distocraft.dc5000.etl.engine.sql.UnknownTopologySQLExecute");
    }

    @Test
    public void test_create_IMSItoIMEISQLExecute() throws Exception {
        checkActionCreation("IMSItoIMEI", "com.distocraft.dc5000.etl.engine.sql.IMSItoIMEISQLExecute");
    }

    @Test
    public void test_create_GateKeeperPropertyAction() throws Exception {
        checkActionCreation("GateKeeperProperty", "com.distocraft.dc5000.etl.engine.sql.GateKeeperPropertyAction");
    }

    @Test
    public void test_create_UpdateHashIdsAction() throws Exception {
        final String actionType = "UpdateHashIds";
        final Meta_transfer_actions action = insertAction(10, actionType);
        final Properties props = new Properties();
        props.put("targetType", "targetType");
        action.setWhere_clause(TransferActionBase.propertiesToString(props));
        checkActionCreation(actionType, action, UpdateHashIdsAction.class);
    }

    @Test
    public void test_create_SQLActionExecute() throws Exception {
        checkActionCreation("SQLActionExecute", SQLActionExecute.class);
    }

    @Test
    public void test_create_SQLInsert() throws Exception {
        final String actionType = "SQL Insert";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, SQLInsert.class);
    }

    @Test
    public void test_create_SQLUpdate() throws Exception {
        final String actionType = "SQL Update";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, SQLUpdate.class);
    }

    @Test
    public void test_create_SQLInsertAndUpdate() throws Exception {
        final String actionType = "SQL Update&Ins";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, SQLInsertAndUpdate.class);
    }

    @Test
    public void test_create_SQLSummary() throws Exception {
        final String actionType = "SQL Summary";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, SQLSummary.class);
    }

    @Test
    public void test_create_SQLDelete() throws Exception {
        final String actionType = "SQL Delete";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, SQLDelete.class);
    }

    @Test
    public void test_create_SQLCreateAsSelect() throws Exception {
        final String actionType = "SQL Create as Select";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, SQLCreateAsSelect.class);
    }

    @Test
    public void test_create_SQLOutputToFile() throws Exception {
        final String actionType = "DB -> File";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, SQLOutputToFile.class);
    }

    @Test
    public void test_create_SQLInputFromFile() throws Exception {
        final String actionType = "File -> DB";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, SQLInputFromFile.class);
    }

    @Test
    public void test_create_PluginToSql() throws Exception {
        final String actionType = "Plugin -> DB";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, PluginToSql.class);
    }

    @Test
    public void test_create_SqlToPlugin() throws Exception {
        final String actionType = "DB -> Plugin";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, SqlToPlugin.class);
    }

    @Test
    public void test_create_SQLLoad() throws Exception {
        final String actionType = "SQL Load";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, SQLLoad.class);
    }

    @Test
    public void test_create_Plugin() throws Exception {
        final String actionType = "Plugin";
        final Meta_transfer_actions action = setupSqlTasks(actionType);
        checkActionCreation(actionType, action, Plugin.class);
    }

    @Test
    public void test_create_Config() throws Exception {
        checkActionCreation("Config", Config.class);
    }

    @Test
    public void test_create_DirSetPermissions() throws Exception {
        checkActionCreation("DirSetPermissions", DirSetPermissions.class);
    }

    private void checkActionCreation(final String actionType, final String klass) throws Exception {
        final Class _klass = Class.forName(klass);
        checkActionCreation(actionType, _klass);
    }

    private void checkActionCreation(final String actionType, final Meta_transfer_actions action, final String klass) throws Exception {
        final Class _klass = Class.forName(klass);
        checkActionCreation(actionType, action, _klass);
    }

    private void checkActionCreation(final String actionType, final Class klass) throws Exception {
        checkActionCreation(actionType, insertAction(10, actionType), klass);
    }

    private void checkActionCreation(final String actionType, final Meta_transfer_actions action, final Class klass) throws Exception {
        truncateSchemaTables(Schema.etlrep, Arrays.asList("META_TRANSFER_ACTIONS"));
        final TransferAction transferAction = new TransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets, meta_collections,
                action.getTransfer_action_id(), action, transferBatchId, connectionPool, batchColumnName, pluginLoader, sctx, clog, engineCom, null);
        Assert.assertNotNull(actionType + " of type " + klass.getSimpleName() + " not created!", transferAction.trBaseAction);
        Assert.assertTrue("Wrong Action created E{" + klass.getName() + "} != A{" + transferAction.trBaseAction.getClass().getName() + "}",
                transferAction.trBaseAction.getClass() == klass);
    }

    //@Test
    public void checkThatPartitionedLoaderIsCreatedWhenTransferActionIsLoader() throws Exception {
        final long transferActionId = 3;
        metaTransferActions = new Meta_transfer_actions(etlrep, "versionnumber", transferActionId, collectionId, collectionSetId);

        final StubbedTransferAction transferAction = new StubbedTransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets,
                meta_collections, transferActionId, metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader, sctx, clog,
                engineCom, null);
        assertTrue(transferAction.trBaseAction instanceof PartitionedLoader);

    }

    //@Test
    public void checkThatCreateCollectedDataFilesActionIsCreatedWhenTransferActionIsCreateCollectedDataFiles() throws Exception {
        final long transferActionId = 4;
        metaTransferActions = new Meta_transfer_actions(etlrep, "versionnumber", transferActionId, collectionId, collectionSetId);

        final StubbedTransferAction transferAction = new StubbedTransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets,
                meta_collections, transferActionId, metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader, sctx, clog,
                engineCom, null);
        assertTrue(transferAction.trBaseAction instanceof CreateCollectedDataFilesAction);

    }

    //@Test
    public void checkThatUpdateCollectedDataActionIsCreatedWhenTransferActionIsUpdateCollectedData() throws Exception {
        final long transferActionId = 5;
        metaTransferActions = new Meta_transfer_actions(etlrep, "versionnumber", transferActionId, collectionId, collectionSetId);

        final StubbedTransferAction transferAction = new StubbedTransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets,
                meta_collections, transferActionId, metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader, sctx, clog,
                engineCom, null);
        assertTrue(transferAction.trBaseAction instanceof UpdateCollectedDataAction);

    }

    //@Test
    public void checkThatUpdateHashIdsActionIsCreatedWhenTransferActionIsUpdateHashIds() throws Exception {

        final Properties where_cond = new Properties();
        where_cond.setProperty("targetType", "EVENT_E_TEST:RAW");

        final ByteArrayOutputStream baoss = new ByteArrayOutputStream();
        where_cond.store(baoss, "");
        final long transferActionId = 6;

        metaTransferActions = new Meta_transfer_actions(etlrep, "versionnumber", transferActionId, collectionId, collectionSetId);
        metaTransferActions.setWhere_clause(baoss.toString());

        final StubbedTransferAction transferAction = new StubbedTransferAction(etlrep, meta_versions, collectionSetId, meta_collection_sets,
                meta_collections, transferActionId, metaTransferActions, transferBatchId, connectionPool, batchColumnName, pluginLoader, sctx, clog,
                engineCom, null);
        assertTrue(transferAction.trBaseAction instanceof UpdateHashIdsAction);
    }

    private class StubbedTransferAction extends TransferAction {

        public StubbedTransferAction(final RockFactory etlrep, final Meta_versions version, final long collectionSetId,
                                     final Meta_collection_sets collectionSets, final Meta_collections collection, final long transferActionId,
                                     final Meta_transfer_actions metaTransferActions, final long transferBatchId,
                                     final ConnectionPool connectionPool, final String batchColumnName, final PluginLoader pluginLoader,
                                     final SetContext sctx, final Logger clog, final EngineCom engineCom, final PriorityQueue priorityQueue)
                throws Exception {
            super(etlrep, version, collectionSetId, collectionSets, collection, transferActionId, metaTransferActions, transferBatchId,
                    connectionPool, batchColumnName, pluginLoader, sctx, clog, engineCom, priorityQueue);
        }

        @Override
        protected PartitionedLoader getPartitionedLoader(final SetContext setContext) throws EngineMetaDataException {
            return new StubbedPartitionedLoader(this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                    this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, setContext, clog);
        }

    }

    private class StubbedPartitionedLoader extends PartitionedLoader {

        public StubbedPartitionedLoader(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
                                        final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
                                        final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final SetContext sctx,
                                        final Logger clog) throws EngineMetaDataException {
            super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool, trActions, sctx, clog);
        }

        @Override
        protected long getSessionHandler() {
            return 10L;
        }

    }

    private static void setupDataFormatCache() {
        final Map<String, DFormat> it_map = new HashMap<String, DFormat>();
        final Map<String, List<DFormat>> id_map = new HashMap<String, List<DFormat>>();
        final Set<String> if_names = new HashSet<String>();
        final Map<String, DFormat> folder_map = new HashMap<String, DFormat>();

        final List<DItem> list = new ArrayList<DItem>();
        final DItem col1 = new DItem("col1", 1, "col1", "", "varchar", 255, 0);
        list.add(col1);
        final DItem col2 = new DItem("col2", 2, "col2", "", "varchar", 255, 0);
        list.add(col2);
        final DItem col3 = new DItem("col3", 3, "col3", "", "varchar", 255, 0);
        list.add(col3);

        final DFormat df = new DFormat("id", "tag", "dataformat", "folder", "transformer");
        df.setItems(list);

        folder_map.put("table", df);

        DataFormatCache.testInitialize(it_map, id_map, if_names, folder_map);
    }

    private Meta_transfer_actions insertAction(final long actionId, final String actionType) throws RockException, SQLException {
        return insertAction(actionId, actionType, Schema.etlrep);
    }

    private Meta_transfer_actions insertAction(final long actionId, final String actionType, final Schema connSchema) throws RockException,
            SQLException {
        final Meta_transfer_actions action = new Meta_transfer_actions(etlrep);
        action.setAction_type(actionType);
        action.setTransfer_action_id(actionId);
        action.setCollection_set_id(collectionSetId);
        action.setCollection_id(collectionId);
        action.setVersion_number("meta_versions");
        action.setTransfer_action_name(actionType + "-" + actionId);
        action.setOrder_by_no((long) 1);
        action.setEnabled_flag("Y");
        final long connectionId = connectionMapping.get(connSchema.name() + ":USER");
        action.setConnection_id(connectionId);
        action.setAction_contents(" ");
        action.setWhere_clause(" ");
        action.saveDB();
        return action;
    }

    private Meta_transfer_actions setupSqlTasks(final String actionType) throws RockException, SQLException, EngineMetaDataException {
        final Meta_transfer_actions action = insertAction(10, actionType);

        final Meta_source_tables mst = new Meta_source_tables(etlrep);
        mst.setTransfer_action_id(action.getTransfer_action_id());
        mst.setCollection_set_id(action.getCollection_set_id());
        mst.setCollection_id(action.getCollection_id());
        mst.setVersion_number(meta_versions.getVersion_number());
        mst.setTable_id((long) 1);
        mst.setUse_tr_date_in_where_flag("N");
        mst.setConnection_id(action.getConnection_id());
        mst.setDistinct_flag("N");
        mst.saveDB();

        final Meta_tables mt = new Meta_tables(etlrep);
        mt.setTable_id(mst.getTable_id());
        mt.setTable_name("temp_table");
        mt.setVersion_number(mst.getVersion_number());
        mt.setIs_join("N");
        mt.setConnection_id(mst.getConnection_id());
        mt.saveDB();

        final Meta_target_tables mtt = new Meta_target_tables(etlrep);
        mtt.setVersion_number(mst.getVersion_number());
        mtt.setCollection_set_id(mst.getCollection_set_id());
        mtt.setCollection_id(mst.getCollection_id());
        mtt.setTransfer_action_id(action.getTransfer_action_id());
        mtt.setConnection_id(action.getConnection_id());
        mtt.setTable_id(mst.getTable_id());
        mtt.saveDB();

        final Meta_files mf = new Meta_files(etlrep);
        mf.setCollection_id(action.getCollection_id());
        mf.setCollection_set_id(action.getCollection_set_id());
        mf.setTransfer_action_id(action.getTransfer_action_id());
        mf.setVersion_number(meta_versions.getVersion_number());
        mf.setFile_id((long) 1);
        mf.setFile_name("");
        mf.setFile_content_type("");
        mf.setIs_source("N");
        mf.saveDB();

        final Meta_plugins mp = new Meta_plugins(etlrep);
        mp.setPlugin_id((long) 1);
        mp.setPlugin_name(testPluginName);
        mp.setIs_source("N");
        mp.setCollection_id(action.getCollection_id());
        mp.setCollection_set_id(action.getCollection_set_id());
        mp.setTransfer_action_id(action.getTransfer_action_id());
        mp.setVersion_number(meta_versions.getVersion_number());
        mp.setConstructor_parameter("");
        mp.saveDB();

        final Meta_sql_loads mls = new Meta_sql_loads(etlrep);
        mls.setInput_file("");
        mls.setCtl_file("");
        mls.setCollection_id(action.getCollection_id());
        mls.setCollection_set_id(action.getCollection_set_id());
        mls.setConnection_id(action.getConnection_id());
        mls.setDis_file("");
        mls.setBad_file("");
        mls.setLoad_option("");
        mls.setLoad_type("");
        mls.setVersion_number(meta_versions.getVersion_number());
        mls.setTransfer_action_id(action.getTransfer_action_id());
        mls.setTable_id((long) 1);
        mls.setText("");
        mls.saveDB();

        return action;
    }

    private static void addPropertyToFile(final String propertyName, final String propertyValue, final File propertiesFile) throws IOException {
        final Properties properties = new Properties();
        properties.put(propertyName, propertyValue);
        properties.store(new FileOutputStream(propertiesFile), "properties for testing");
    }

    private static File createEmptyFile(final String localEniqConfigDirectory, final String fileName) throws IOException {
        final File directory = new File(localEniqConfigDirectory);
        directory.mkdirs();
        directory.deleteOnExit();
        final File etlcServerPropertiesFile = new File(directory, fileName);
        etlcServerPropertiesFile.createNewFile();
        etlcServerPropertiesFile.deleteOnExit();
        return etlcServerPropertiesFile;
    }
}
