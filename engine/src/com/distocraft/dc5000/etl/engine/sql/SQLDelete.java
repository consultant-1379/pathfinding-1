package com.distocraft.dc5000.etl.engine.sql;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.RemoveDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that holds common methods for all SQL actions
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLDelete extends SQLOperation {
	// The target table object
	private final SQLTarget target;

	/**
	 * Constructor
	 * 
	 * @param versionNumber
	 *          metadata version
	 * @param collectionSetId
	 *          primary key for collection set
	 * @param collectionId
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
	public SQLDelete(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final String batchColumnName)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.target = new SQLTarget(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
				rockFact, connectionPool, trActions, batchColumnName);

	}

	/**
	 * Executes a SQL delete clause
	 */
	public void execute() throws EngineException {
		String sqlClause = "";
		try {
			sqlClause = "DELETE FROM ";
			sqlClause += this.target.getTableName();
			if (this.getWhereClause().length() > 0) {
				sqlClause += " WHERE ";
			}
			sqlClause += this.getWhereClause();
			this.writeDebug(sqlClause);
			this.target.getConnection().executeSql(sqlClause);
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { sqlClause }, e, this, this.getClass()
					.getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}
	}

	/**
	 * Returns the where clause, the last transfer condition is added to th e
	 * clause if defined.
	 */
	public String getWhereClause() {
		String whereClause = this.getTrActions().getWhere_clause();
		if (whereClause == null) {
			whereClause = "";
		}

		return whereClause;
	}

	/**
	 * Executes the foreign key constraint checking
	 * 
	 * @return int number of fk errors
	 */
	public int executeFkCheck() throws EngineException {
		return this.target.sqlFkFactory.executeFkCheck();
	}

	/**
	 * If transfer fails, removes the data transferred before fail
	 */
	public void removeDataFromTarget() throws EngineMetaDataException, RemoveDataException {
		// Removal not needed for Delete, JJ 4.12.2002
		// this.target.removeDataFromTarget();

	}
}
