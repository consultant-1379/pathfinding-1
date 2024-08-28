package com.distocraft.dc5000.etl.engine.sql;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that executes SQL create as select clauses
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLCreateAsSelect extends SQLOperation {

	// The source table object
	private final SQLSource source;
	// The select part of the sql clause
	private final String selectClause;
	// The executable sql clause
	private final String createAsSelectClause;

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
	public SQLCreateAsSelect(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final String batchColumnName)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.source = new SQLSource(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
				rockFact, connectionPool, trActions, batchColumnName);

		final String createAsSelectTableName = trActions.getAction_contents();

		String selectClause = this.source.getSelectClause(false);
		if (this.source.getWhereClause().length() > 0) {
			selectClause += " WHERE ";
		}
		selectClause += this.source.getWhereClause();

		this.selectClause = selectClause;
		
		String createAsSelectClause = "CREATE TABLE " + createAsSelectTableName + " "
				+ this.source.getTable().getAs_select_options();

		if ((this.source.getTable().getAs_select_tablespace() != null)
				&& (this.source.getTable().getAs_select_tablespace().length() > 0)) {
			createAsSelectClause += " TABLESPACE ";
		}
		createAsSelectClause += this.source.getTable().getAs_select_tablespace();
		createAsSelectClause += " AS SELECT " + this.selectClause;
		
		this.createAsSelectClause = createAsSelectClause;
		
	}

	/*For Junit Visibility*/
	protected String getcreateAsSelectClause() {
		return this.createAsSelectClause;
	}
	
	/**
	 * Executes an insert clause
	 * 
	 */

	public void execute() throws EngineException {

		// Updates last transferred value to META_SOURCES
		this.source.setLastTransferDate();

		executeInsideDB();

	}

	/**
	 * Executes an update clause inside a database
	 * 
	 */
	private void executeInsideDB() throws EngineException {
		try {
			this.writeDebug(this.createAsSelectClause);
			this.source.getConnection().executeSql(this.createAsSelectClause);
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { this.createAsSelectClause }, e, this,
					this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}
	}

}
