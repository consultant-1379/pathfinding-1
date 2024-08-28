package com.distocraft.dc5000.etl.engine.system;

import com.ericsson.eniq.common.lwp.LwProcess;
import java.util.List;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_sql_loads;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that implements a system call execution
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLLoad extends TransferActionBase {

	/** Variable for Oracle JDBC Connection */
	private static final String CONNECTION_ORACLE_JDBC = "Oracle JDBC";
	/** JDBC connection type */
	private static final String CONNECTION_JDBC = "JDBC";

	/** Connection pool - nedded for Sybase load */
	private ConnectionPool m_connectionPool;

	private Meta_sql_loads sqlLoads;

	/**
	 * Empty protected constructor
	 * 
	 */
	protected SQLLoad() {
	}

	/**
	 * Constructor
	 * 
	 * @param version
	 *          metadata version
	 * @param collectionSetId
	 *          primary key for collection set
	 * @param collection
	 *          primary key for collection
	 * @param transferActionId
	 *          primary key for transfer action
	 * @param transferBatchId
	 *          primary key for transfer batch
	 * @param connectId
	 *          primary key for database connections
	 * @param rockFact
	 *          metadata repository connection object
	 * @param connectionPool
	 *          a pool for database connections in this collection
	 * @param trActions
	 *          object that holds transfer action information (db contents)
	 * 
	 * @author Jukka Jaaheimo
	 * @since JDK1.1
	 */
	public SQLLoad(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions, final ConnectionPool connectionPool) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		m_connectionPool = connectionPool;
		
		try {
			this.sqlLoads = new Meta_sql_loads(rockFact, collectionSetId, collection.getCollection_id(), connectId,
					version.getVersion_number(), trActions.getTransfer_action_id(), null);
		} catch (Exception e) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_READ_METADATA, new String[] { "META_SQL_LOADS" }, e,
					this, this.getClass().getName());
		}

	}

	/**
	 * Executes a SQL procedure
	 */
	public void execute() throws EngineException {

		String action = null;
		// check the DB type
		try {
			String dbType = null;
			final Meta_databases meta_databases = new Meta_databases(getRockFact());
			meta_databases.setConnection_id(sqlLoads.getConnection_id());
			final Meta_databasesFactory rockFactMeta_databases = new Meta_databasesFactory(getRockFact(), meta_databases);
			
			final List<Meta_databases> databases = rockFactMeta_databases.get();
			if (databases.size() == 1) {
				if (databases.get(0).getType_name().equals(CONNECTION_ORACLE_JDBC)) {
					// DB is ORACLE
					dbType = "ORACLE";
				}
				if (databases.get(0).getType_name().equals(CONNECTION_JDBC)) {
					// JDBC DB
					dbType = "SYBASE";
				}
			} else { // should never happend - returned vector must have only 1
								// element
				throw new Exception("More than 1 DB connection returned! " + "Location: engine.system.SQLLoad.execute().");
			}
			// the DB type is known
			if (dbType.equals("ORACLE")) { // DB is Oracle

				final String systemClause = this.sqlLoads.getSqlldr_cmd() + " control=" + this.sqlLoads.getCtl_file();
				action = systemClause;
				this.writeDebug(systemClause);
				
				if (systemClause != null) {
          LwProcess.execute(systemClause, true, null);
				}

			} else { // DB is Sybase

				// execute the load table clause
				action = sqlLoads.getText();

				final String sqlClause = sqlLoads.getText();
				this.writeDebug(sqlClause);
				final RockFactory connection = m_connectionPool.getConnect(this, sqlLoads.getVersion_number(),
						sqlLoads.getConnection_id());

				connection.executeSql(sqlClause);
				connection.commit();
			}
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { action }, e, this, this.getClass()
					.getName(), EngineConstants.ERR_TYPE_SYSTEM);
		}
	}
}
