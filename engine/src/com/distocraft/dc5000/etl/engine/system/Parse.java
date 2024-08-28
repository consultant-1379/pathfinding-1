package com.distocraft.dc5000.etl.engine.system;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.*;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.sql.SQLOperation;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.*;

public class Parse extends SQLOperation {

    private final Logger log;

    private Meta_collections collection;

    private Meta_transfer_actions actions;

    private String collectionSetName;

    private SetContext sctx;

    private RockFactory dwhreprock;

    private EngineCom eCom = null;

    /**
     * Empty protected constructor
     */
    protected Parse() {
        log = Logger.getLogger("etlengine.Parse");
    }

    /**
     * Constructor
     * 
     * @param versionNumber
     *            metadata version
     * @param collectionSetId
     *            primary key for collection set
     * @param collectionId
     *            primary key for collection
     * @param transferActionId
     *            primary key for transfer action
     * @param transferBatchId
     *            primary key for transfer batch
     * @param connectId
     *            primary key for database connections
     * @param etlreprock
     *            metadata repository connection object
     * @param connectionPool
     *            a pool for database connections in this collection
     * @param trActions
     *            object that holds transfer action information (db contents)
     */
    public Parse(final Meta_versions version, final Long collectionSetId, final Meta_collections collection, final Long transferActionId,
                 final Long transferBatchId, final Long connectId, final RockFactory etlreprock, final Meta_transfer_actions trActions,
                 final ConnectionPool connectionPool, final SetContext sctx, final Logger clog, final EngineCom eCom) throws EngineMetaDataException,
            RockException, SQLException {

        super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, etlreprock, connectionPool, trActions);

        this.log = Logger.getLogger(clog.getName() + ".Parse");

        // Get collection set name
        final Meta_collection_sets whereCollSet = new Meta_collection_sets(etlreprock);
        whereCollSet.setEnabled_flag("Y");
        whereCollSet.setCollection_set_id(collectionSetId);
        final Meta_collection_sets collSet = new Meta_collection_sets(etlreprock, whereCollSet);

        this.eCom = eCom;
        this.collectionSetName = collSet.getCollection_set_name();
        this.collection = collection;
        this.actions = trActions;
        this.sctx = sctx;
    }

    @Override
    public void execute() throws Exception {

        try {
            //createDwhRepRockFactory(getRockFact());
            final RockFactory rock = getConnection();

            final Properties properties = TransferActionBase.stringToProperties(this.actions.getAction_contents());

            final Class<?>[] parameterTypes = { Properties.class, String.class, String.class, String.class, RockFactory.class,
                    EngineCom.class };

            final Object args[] = { properties, collectionSetName, collection.getSettype(), collection.getCollection_name(), rock, eCom };

            final Class<?> c = Class.forName("com.distocraft.dc5000.etl.parser.Main");

            final Constructor<?> cont = c.getConstructor(parameterTypes);

            final Object action = cont.newInstance(args);

            final Method parse = c.getMethod("parse");

            final Map<String, Object> map = (Map<String, Object>) parse.invoke(action);

            sctx.put("parsedMeastypes", map.get("parsedMeastypes"));
        } catch (Exception e) {
            final String cause = e.getCause().getMessage();
            log.log(Level.SEVERE, "Problem calling Main.parse()." + (cause != null ? "\nCaused by: " + cause : ""), e);
            throw e; // exceptions not caught previously, so re-throw to maintain
                     // behaviour of calling method
        } 
        
        /*finally {
            if (dwhreprock != null) {
                try {
                    dwhreprock.getConnection().close();
                } catch (Exception e) {
                    log.log(Level.WARNING, "Error closing connection", e);
                }
            }
        }*/
    }

    private void createDwhRepRockFactory(final RockFactory etlreprock) throws SQLException, RockException {
        final Meta_databases md_cond = new Meta_databases(etlreprock);
        md_cond.setType_name("USER");
        md_cond.setConnection_name("dwhrep");
        final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlreprock, md_cond);

        final List<Meta_databases> listofDBs = md_fact.get();

        if (listofDBs.size() > 0) {
            final Meta_databases db = listofDBs.get(0);
            dwhreprock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(), db.getDriver_name(), "Parser", true);
        } else {
            log.warning("Unable to find dwhrep connection from etlrep.meta_databases. Transformer may not work.");
        }
    }
}
