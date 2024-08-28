package com.distocraft.dc5000.etl.engine.structure;

import java.lang.reflect.*;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;


import com.distocraft.dc5000.etl.engine.common.*;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.file.SQLInputFromFile;
import com.distocraft.dc5000.etl.engine.file.SQLOutputToFile;
import com.distocraft.dc5000.etl.engine.plugin.*;
import com.distocraft.dc5000.etl.engine.priorityqueue.PriorityQueue;
import com.distocraft.dc5000.etl.engine.sql.*;
import com.distocraft.dc5000.etl.engine.system.*;
import com.distocraft.dc5000.etl.rock.*;
import com.ericsson.eniq.backuprestore.backup.*;

/**
 * A class for transfer action.
 * 
 * @author Jukka Jaaheimo, Tuomas Lemminkainen, Jarno Savinen
 */
public class TransferAction {

    // version number
    Meta_versions version;

    // collection set id
    Long collectionSetId;

    // collection
    Meta_collections collection;

    // transfer action id
    Long transferActionId;

    // db connection object
    RockFactory rockFact;

    // the corresponding database object
    Meta_transfer_actions dbTrAction;

    // The action to be executed
    TransferActionBase trBaseAction;

    // Type of the transfer action
    String transferActionType;

    String transferActionName;

    // All connections used in this transfer
    ConnectionPool connectionPool;

    // Transfer batch id
    Long transferBatchId;

    // Number fk errors
    private int fkErrors;

    // Number of con const errors
    private int colConstErrors;

    // Batch column name
    private String batchColumnName;

    // collection set
    Meta_collection_sets collSet;

    private Logger log;

    /**
     * Empty protected constructor
     */
    protected TransferAction() {
    }

    /**
     * Constructor
     * 
     * @param rockFact
     *            metadata repository connection object
     * @param version
     *            metadata version
     * @param collectionSetId
     *            primary key for collection set
     * @param collection
     *            collection
     * @param transferActionId
     *            primary key for transfer action
     * @param transferBatchId
     *            primary key for transfer batch
     * @param dbTrAction
     *            object that holds transfer action information (db contents)
     * @param connectionPool
     *            a pool for database connections in this collection
     * @author Jukka Jaaheimo
     * @since JDK1.1
     */
    public TransferAction(final RockFactory rockFact, final Meta_versions version, final Long collectionSetId, final Meta_collection_sets collSet,
                          final Meta_collections collection, final Long transferActionId, final Meta_transfer_actions dbTrAction,
                          final Long transferBatchId, final ConnectionPool connectionPool, final String batchColumnName, final PluginLoader pLoader,
                          final SetContext sctx, final Logger log, final EngineCom eCom, final PriorityQueue pq) throws EngineMetaDataException {

        this.rockFact = rockFact;
        this.version = version;
        this.collectionSetId = collectionSetId;
        this.collection = collection;
        this.dbTrAction = dbTrAction;
        this.transferActionId = transferActionId;
        this.transferBatchId = transferBatchId;
        this.connectionPool = connectionPool;
        this.batchColumnName = batchColumnName;
        this.collSet = collSet;
        this.log = log;

        this.transferActionType = this.dbTrAction.getAction_type();
        this.transferActionName = this.dbTrAction.getTransfer_action_name();

        if (this.dbTrAction.getEnabled_flag().equalsIgnoreCase("Y")) {

            try {

                // --- OFFICIAL ACTIONS NAMES STARTS HERE ---

                if (this.transferActionType.equals("UnPartitioned Loader")) {
                    this.trBaseAction = new UnPartitionedLoader(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log);
                } else if (this.transferActionType.equals("Loader") || this.transferActionType.equals("Partitioned Loader")) {
                    this.trBaseAction = getPartitionedLoader(sctx);
                } else if (this.transferActionType.equals("EventLoader")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.EventLoaderFactory");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, SetContext.class, PriorityQueue.class,
                            PluginLoader.class, EngineCom.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, pq, pLoader, eCom, log };

                    getStaticMethodFromFactory(c, parameterTypes, args, "createEventLoaderAction");
                } else if (this.transferActionType.equals("TimeBase EventLoader")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.TimeBasePartitionLoaderFactory");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, SetContext.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log };

                    getStaticMethodFromFactory(c, parameterTypes, args, "createTimeBasePartitionLoaderAction");
                } else if (this.transferActionType.equals("UpdateDimSession")) {
                    this.trBaseAction = new UpdateDIMSession(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx);
                } else if (this.transferActionType.equals("Parse")) {
                    this.trBaseAction = new Parse(this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, this.connectionPool, sctx, log, eCom);
                } else if (this.transferActionType.equals("Uncompress")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.uncompress.UncompressorAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("System Call")) {
                    this.trBaseAction = new SystemCall(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("SessionLog Loader")) {
                    this.trBaseAction = new LogSessionLoader(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log);
                } else if (this.transferActionType.equals("Aggregator") || this.transferActionType.equals("Aggregation")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.monitoring.AggregationAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class };

                    final Object[] args = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("UpdateMonitoring")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.monitoring.UpdateMonitoringAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class };

                    final Object[] args = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("AutomaticAggregation")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.monitoring.AutomaticAggregationAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class };

                    final Object[] args = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("AutomaticREAggregati")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.monitoring.AutomaticReAggregationAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class };

                    final Object[] args = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("ManualReAggregation")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.monitoring.ManualReAggregationAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class };

                    final Object[] args = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("SQL Execute")) {
                    this.trBaseAction = new SQLExecute(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log);
                } else if (this.transferActionType.equals("CreateDir")) {
                    this.trBaseAction = new CreateDirAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("System call")) {
                    this.trBaseAction = new SystemCall(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("Distribute")) {
                    this.trBaseAction = new Distribute(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("JVMMonitor")) {
                    this.trBaseAction = new JVMMonitorAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("Diskmanager")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.diskmanager.DiskManagerAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class };

                    final Object[] args = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("DirectoryDiskmanager")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.diskmanager.DirectoryDiskManagerAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class };

                    final Object[] args = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("Trigger")) {
                    this.trBaseAction = new TriggerAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("SetContextTrigger")) {
                    this.trBaseAction = new SetContextTriggerAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx);
                } else if (this.transferActionType.equals("TriggerScheduledSet")) {
                    this.trBaseAction = new TriggerSetListInSchedulerAction(this.version, this.collectionSetId, this.collection,
                            this.transferActionId, this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx);
                } else if (this.transferActionType.equals("GateKeeper")) {
                    this.trBaseAction = new GateKeeperAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("TriggerDeltaView")) {
                		this.trBaseAction = new TriggerDeltaViewCreation(log);
                } else if (this.transferActionType.equals("ViewForInfoStore")) {
            		this.trBaseAction = new CreationOfViewForInfoStore(log);
            }
                else if (this.transferActionType.equals("BackupTopologyData") || this.transferActionType.equals("BackupAggregationDat") ) {
            		this.trBaseAction = new TriggerBackUp(log,this.rockFact,this.transferActionType);
                }
                else if (this.transferActionType.equals("BatchUpdateMonitorin")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.monitoring.BatchUpdateMonitoringAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class };

                    final Object[] args = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("import")) {
                    this.trBaseAction = new ImportAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx);
                } else if (this.transferActionType.equals("export")) {
                    this.trBaseAction = new ExportAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx);
                } else if (this.transferActionType.equals("Mediation")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.mediation.MediationAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("JDBC Mediation")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.mediation.jdbc.JDBCMediationAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("AlarmHandler")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.system.AlarmHandlerActionWrapper");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, SetContext.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("AlarmMarkup")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.system.AlarmMarkupActionWrapper");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, SetContext.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("UpdatePlan")) {
                    this.trBaseAction = new DWHMUpdatePlanAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("SMTP Mediation")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.mediation.smtp.SMTPMediationAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("System Monitor")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.monitoring.SystemMonitorAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class, Logger.class };

                    final Object[] args = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("DuplicateCheck")) {
                    this.trBaseAction = new DuplicateCheckAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx);
                } else if (this.transferActionType.equals("SQL Extract")) {
                    this.trBaseAction = new SQLExtract(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction);
                } else if (this.transferActionType.equals("ChangeProfile")) {
                    this.trBaseAction = new ChangeProfileAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("reloadProperties")) {
                    this.trBaseAction = new ReloadPropertiesAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("PartitionAction")) {
                    this.trBaseAction = new DWHMPartitionAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("StorageTimeAction")) {
                    this.trBaseAction = new DWHMStorageTimeAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("SanityCheck")) {
                    this.trBaseAction = new DWHSanityCheckerAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("CreateViews")) {
                    this.trBaseAction = new DWHMCreateViewsAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("VersionUpdate")) {
                    this.trBaseAction = new DWHMVersionUpdateAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("UpdateMonitoredTypes")) {
                    this.trBaseAction = new UpdateMonitoredTypes(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("TableCleaner")) {
                    this.trBaseAction = new TableCleaner(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("SetTypeTrigger")) {
                    this.trBaseAction = new SetTypeTriggerAction(version, collectionSetId, collection, transferActionId, transferBatchId,
                            this.dbTrAction.getConnection_id(), rockFact, dbTrAction, log);
                } else if (this.transferActionType.equals("TableCheck")) {
                    this.trBaseAction = new DWHMTableCheckAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("AggregationRuleCopy")) {
                    this.trBaseAction = new AggregationRuleCopy(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, log);
                } else if (this.transferActionType.equals("PartitionedSQLExec")) {
                    this.trBaseAction = new PartitionedSQLExecute(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log);
                } else if (this.transferActionType.equals("SQLLogResultSet")) {
                    this.trBaseAction = new SQLLogResultSet(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log);
                } else if (this.transferActionType.equals("ReloadDBLookups")) {
                    this.trBaseAction = new ReloadDBLookupsAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx, log);
                } else if (this.transferActionType.equals("ReloadTransformation")) {
                    this.trBaseAction = new ReloadTransformationsAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("AggRuleCacheRefresh")) {
                    this.trBaseAction = new AggregationRuleCacheRefreshAction(this.version, this.collectionSetId, this.collection,
                            this.transferActionId, this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("ExecutionProfiler")) {
                    this.trBaseAction = new ExecutionProfilerAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, log);
                } else if (this.transferActionType.equals("ReloadProperties")) {
                    this.trBaseAction = new ReloadPropertiesAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("RefreshDBLookup")) {
                    this.trBaseAction = new InvalidateDBLookupCache(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, log);
                } else if (this.transferActionType.equals("SQLJoiner")) {
                    this.trBaseAction = new SQLJoin(this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log);
                } else if (this.transferActionType.equals("EBSUpdate")) {
                    this.trBaseAction = new EBSUpdateAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, log);
                } else if (this.transferActionType.equals("StoreCountingData")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.StoreCountingManagementDataAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, SetContext.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("CountingIntervals")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.CountingIntervalsAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, SetContext.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("UpdateCountIntervals")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.UpdateCountIntervalsOnUpgrade");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, SetContext.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("CountingTrigger")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.CountingTrigger");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class, Logger.class, PluginLoader.class, EngineCom.class,
                            String.class, PriorityQueue.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx, log, pLoader, eCom,
                            collSet.getCollection_set_name(), pq, this.connectionPool };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("CountingDayTrigger")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.CountingDayTrigger");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class, Logger.class, PluginLoader.class, EngineCom.class,
                            String.class, PriorityQueue.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx, log, pLoader, eCom,
                            collSet.getCollection_set_name(), pq, this.connectionPool };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("CountReAggAction")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.CountingReAggAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class, Logger.class, PluginLoader.class, EngineCom.class,
                            String.class, PriorityQueue.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx, log, pLoader, eCom,
                            collSet.getCollection_set_name(), pq, this.connectionPool };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("BackupTrigger")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.BackupTrigger");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, SetContext.class, Logger.class, PluginLoader.class, EngineCom.class,
                            String.class, PriorityQueue.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, sctx, log, pLoader, eCom,
                            collSet.getCollection_set_name(), pq, this.connectionPool };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("BackupCountDay")) {
                    final Class<?> c = Class.forName("com.ericsson.eniq.engine.backup.BackupCountDayTables");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, String.class, Logger.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, collSet.getCollection_set_name(), log,
                            this.connectionPool };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("BackupTables")) {
                    final Class<?> c = Class.forName("com.ericsson.eniq.engine.backup.BackupTablesFactory");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, String.class, Logger.class, SetContext.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, collSet.getCollection_set_name(), log, sctx,
                            this.connectionPool };

                    getStaticMethodFromFactory(c, parameterTypes, args, "createBackupTablesAction");
                } else if (this.transferActionType.equals("BackupLoader")) {
                    final Class<?> c = Class.forName("com.ericsson.eniq.engine.backup.BackupLoader");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, String.class, Logger.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, collSet.getCollection_set_name(), log,
                            this.connectionPool };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("Restore")) {
                    final Class<?> c = Class.forName("com.ericsson.eniq.engine.backup.BackupLoaderFactory");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, String.class, Logger.class, SetContext.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, collSet.getCollection_set_name(), log, sctx,
                            this.connectionPool };

                    getStaticMethodFromFactory(c, parameterTypes, args, "createBackupLoaderAction");
                } else if (this.transferActionType.equals("CountingAction")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.CountingAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            SetContext.class, RockFactory.class, Meta_transfer_actions.class, String.class, Logger.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), sctx, this.rockFact, this.dbTrAction, collSet.getCollection_set_name(), log,
                            this.connectionPool };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("CountingDayAction")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.CountingDayAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            SetContext.class, RockFactory.class, Meta_transfer_actions.class, String.class, Logger.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), sctx, this.rockFact, this.dbTrAction, collSet.getCollection_set_name(), log,
                            this.connectionPool };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("CreateCollectedData")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.CreateCollectedDataFilesAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("UpdateCollectedData")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.UpdateCollectedDataAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log };

                    getInstanceOfClass(c, parameterTypes, args);

                } else if (this.transferActionType.equals("TopologySqlExecute")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.TopologySQLExecute");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("HistorySqlExecute")) {
                    // eeoidiv,20110926:Automatically create _CALC table for update policy=4=HistoryDynamic (like _CURRENT_DC).
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.HistorySQLExecute");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, log };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("UnknownTopology")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.UnknownTopologySQLExecute");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            SetContext.class, RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class, String.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), sctx, this.rockFact, this.connectionPool, this.dbTrAction, log,
                            collSet.getCollection_set_name() };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("IMSItoIMEI")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.IMSItoIMEISQLExecute");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            SetContext.class, RockFactory.class, ConnectionPool.class, Meta_transfer_actions.class, Logger.class, String.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), sctx, this.rockFact, this.connectionPool, this.dbTrAction, log,
                            collSet.getCollection_set_name() };

                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("GateKeeperProperty")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.GateKeeperPropertyAction");
                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, Logger.class, String.class };
                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, log, collSet.getCollection_set_name() };
                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("UpdateHashIds")) {
                    final Class<?> c = Class.forName("com.distocraft.dc5000.etl.engine.sql.UpdateHashIdsAction");

                    final Class<?>[] parameterTypes = { Meta_versions.class, Long.class, Meta_collections.class, Long.class, Long.class, Long.class,
                            RockFactory.class, Meta_transfer_actions.class, Logger.class, ConnectionPool.class };

                    final Object args[] = { this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, log, this.connectionPool };
                    getInstanceOfClass(c, parameterTypes, args);
                } else if (this.transferActionType.equals("SQLActionExecute")) {
                    this.trBaseAction = new SQLActionExecute(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction);
                } else if (this.transferActionType.equals("SQL Insert")) {
                    this.trBaseAction = new SQLInsert(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction,
                            this.batchColumnName);
                } else if (this.transferActionType.equals("SQL Update")) {
                    this.trBaseAction = new SQLUpdate(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction,
                            this.batchColumnName);
                } else if (this.transferActionType.equals("SQL Update&Ins")) {
                    this.trBaseAction = new SQLInsertAndUpdate(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction,
                            this.batchColumnName);
                } else if (this.transferActionType.equals("SQL Summary")) {
                    this.trBaseAction = new SQLSummary(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction,
                            this.batchColumnName);
                } else if (this.transferActionType.equals("SQL Delete")) {
                    this.trBaseAction = new SQLDelete(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction,
                            this.batchColumnName);
                } else if (this.transferActionType.equals("SQL Create as Select")) {
                    this.trBaseAction = new SQLCreateAsSelect(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction,
                            this.batchColumnName);
                } else if (this.transferActionType.equals("DB -> File")) {
                    this.trBaseAction = new SQLOutputToFile(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction,
                            this.batchColumnName);
                } else if (this.transferActionType.equals("File -> DB")) {
                    this.trBaseAction = new SQLInputFromFile(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction,
                            this.batchColumnName);
                } else if (this.transferActionType.equals("Plugin -> DB")) {
                    this.trBaseAction = new PluginToSql(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction,
                            this.batchColumnName, pLoader);
                } else if (this.transferActionType.equals("DB -> Plugin")) {
                    this.trBaseAction = new SqlToPlugin(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction,
                            this.batchColumnName, pLoader);
                } else if (this.transferActionType.equals("SQL Load")) {
                    this.trBaseAction = new SQLLoad(this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, this.connectionPool);
                } else if (this.transferActionType.equals("Plugin")) {
                    this.trBaseAction = new Plugin(this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction, pLoader);
                } else if (this.transferActionType.equals("Config")) {
                    this.trBaseAction = new Config(this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("Test") || this.transferActionType.equals("A") || this.transferActionType.equals("B")
                        || this.transferActionType.equals("C") || this.transferActionType.equals("D")) {
                    this.trBaseAction = new Test(this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                            this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("TestAction")) {
                    this.trBaseAction = new TestAction(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, this.dbTrAction.getConnection_id(), this.rockFact, this.dbTrAction);
                } else if (this.transferActionType.equals("DirSetPermissions")) {
                    /*
                     * Stop gap solution for slow CreateDirAction actions CreateDirAction can use up to 3 Runtime.exec() call to run chmod, chown &
                     * chgrp DirSetPermissions creates a script based on all the CreateDirAction in the set and executes the script using one
                     * Runtime.exec() call (CNAXE time went from 6 minutes to 9 seconds)
                     */
                    this.trBaseAction = new DirSetPermissions(this.version, this.collectionSetId, this.collection, this.transferActionId,
                            this.transferBatchId, (long) -1, this.rockFact, this.dbTrAction);
                } else {
                    throw new Exception("Unknown transfer action type \"" + this.transferActionType + "\"");
                }
            } catch (NoSuchMethodException nsm) {
                log.log(Level.FINEST, "Error initializing action", nsm);
                throw new EngineMetaDataException("Installed action \"" + transferActionType + "\" is not compatible with this version of engine",
                        null, "constructor");
            } catch (ClassNotFoundException cnf) {
                log.log(Level.FINEST, "Error initializing action", cnf);
                throw new EngineMetaDataException("Module implementing action \"" + transferActionType + "\" is not installed", null, "constructor");
            } catch (EngineMetaDataException ee) {
                log.log(Level.FINEST, "Error initializing action", ee);
                throw ee;
            } catch (Exception e) {
                log.log(Level.WARNING, "Error initializing action: \"" + transferActionType + "\"", e);
                throw new EngineMetaDataException("Error initializing action: \"" + transferActionType + "\"", e, "constructor");
            }

        } else {
            log.fine("Skipping... Action is not enabled");

            this.trBaseAction = null;
        }

    }

    private void getInstanceOfClass(final Class<?> c, final Class<?>[] parameterTypes, final Object[] args) throws NoSuchMethodException,
            InstantiationException, IllegalAccessException, InvocationTargetException {
        final Constructor<?> cont = c.getConstructor(parameterTypes);
        this.trBaseAction = (TransferActionBase) cont.newInstance(args);
    }

    private void getStaticMethodFromFactory(final Class<?> c, final Class<?>[] parameterTypes, final Object[] args, final String methodName)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        final Method cont = c.getMethod(methodName, parameterTypes);
        this.trBaseAction = (TransferActionBase) cont.invoke(null, args);
    }

    /**
     * Extracted out for testing purposes
     * 
     * @param sctx
     * @return
     * @throws Exception
     */
    protected PartitionedLoader getPartitionedLoader(final SetContext sctx) throws EngineMetaDataException {
        return new PartitionedLoader(this.version, this.collectionSetId, this.collection, this.transferActionId, this.transferBatchId,
                this.dbTrAction.getConnection_id(), this.rockFact, this.connectionPool, this.dbTrAction, sctx, log);

    }

    /**
     * Method for executing the action.
     * 
     */
    public void execute(final int maxErrs, final int maxFkErrs, final int maxColConstErrs, final int fkErrs, final int colConstErrs)
            throws EngineException {
        execute(maxErrs, maxFkErrs, maxColConstErrs, fkErrs, colConstErrs, SetListener.NULL);
    }

    /**
     * Method for executing the action.
     * 
     */
    public void execute(final int maxErrs, final int maxFkErrs, final int maxColConstErrs, int fkErrs, int colConstErrs, final SetListener setListener)
            throws EngineException {

        if (this.trBaseAction != null) {

            try {

                this.trBaseAction.execute(setListener);

                this.fkErrors += this.trBaseAction.executeFkCheck();
                this.colConstErrors += 0;

                fkErrs += this.fkErrors;
                colConstErrs += this.colConstErrors;

                if (maxColConstErrs < colConstErrs) {
                    throw new EngineException(EngineConstants.COL_CONST_ERROR_STOP_TEXT, new String[] { "" + colConstErrs + "",
                            "" + maxColConstErrs + "" }, null, this.trBaseAction, this.getClass().getName(), EngineConstants.ERR_TYPE_VALIDATION);
                }
                if (maxFkErrs < fkErrs) {
                    throw new EngineException(EngineConstants.COL_FK_ERROR_STOP_TEXT, new String[] { "" + fkErrs + "", "" + maxFkErrs + "" }, null,
                            this.trBaseAction, this.getClass().getName(), EngineConstants.ERR_TYPE_VALIDATION);

                }
                if (maxErrs < (fkErrs + colConstErrs)) {
                    throw new EngineException(EngineConstants.MAX_ERROR_STOP_TEXT, new String[] { "" + (fkErrs + colConstErrs) + "",
                            "" + maxErrs + "" }, null, this.trBaseAction, this.getClass().getName(), EngineConstants.ERR_TYPE_VALIDATION);

                }

            } catch (EngineException ee) {
                throw ee;
            } catch (Exception e) {
                throw new EngineException("Action execution failed exceptionally", e, this.trBaseAction, "execute", EngineConstants.CANNOT_EXECUTE);
            }

        }
    }

    /**
     * If transfer fails, removes the data transferred before fail
     */
    public void removeDataFromTarget() throws EngineMetaDataException, RemoveDataException {
        if (this.batchColumnName != null) {
            if (this.dbTrAction.getEnabled_flag().equals("Y")) {
                this.trBaseAction.removeDataFromTarget();
            }
        }
    }

    /**
     * Writes status information into the database
     * 
     * @param statusText
     *            text to write into db.
     */
    public void writeStatus(String statusText) throws RockException, SQLException {

        if (statusText == null) {
            statusText = EngineConstants.NO_STATUS_TEXT;
        }

        final Meta_statuses metaStatus = new Meta_statuses(this.rockFact);
        metaStatus.setStatus_description(statusText);
        metaStatus.setVersion_number(this.collection.getVersion_number());
        metaStatus.setCollection_set_id(this.collectionSetId);
        metaStatus.setCollection_id(this.collection.getCollection_id());
        metaStatus.setTransfer_batch_id(this.transferBatchId);
        metaStatus.setTransfer_action_id(this.transferActionId);
        metaStatus.insertDB();

        log.finer("Wrote into Meta_statuses: " + statusText);

    }

    /**
     * GET MEthods for member variables
     */
    public int getFkErrors() {
        return this.fkErrors;
    }

    public int getColConstErrors() {
        return this.colConstErrors;
    }

    public boolean isGateClosed() {

        if (this.trBaseAction != null) {
            return this.trBaseAction.isGateClosed();
        }

        return false;

    }

    public String getActionType() {
        return transferActionType;
    }

    public String getActionName() {
        return transferActionName;
    }

}
