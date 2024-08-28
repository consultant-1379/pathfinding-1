package com.distocraft.dc5000.etl.engine.sql;

import java.util.Vector;

import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that executes SQL insert and update clauses
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLInsertAndUpdate extends SQLUpdate {

	// The insert clause
	private final String insertClause;

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
	public SQLInsertAndUpdate(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final String batchColumnName)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions, batchColumnName);

		final StringBuilder insertClause = new StringBuilder();
		insertClause.append("INSERT INTO ").append(this.getTarget().getTableName()).append(" (");
    insertClause.append(this.getTarget().getCommaSeparatedColumns(false,false)).append(")");

		insertClause.append(this.getSource().getSelectClause(this.getTarget().tableContainsBatchColumn()));
    insertClause.append(" WHERE ");
    insertClause.append(this.getSource().getWhereClause());                        
    if (this.getSource().getWhereClause().length()>0) {
        insertClause.append(" AND ");
    }
    
    insertClause.append("NOT EXISTS (select * from ");
    insertClause.append(this.getTarget().getTableName()).append(" ").append(EngineConstants.TARGET_TABLE_ALIAS);
    insertClause.append(" WHERE ").append(this.getTarget().getPkWhereClause(this.getSource())).append(")");

    this.insertClause = insertClause.toString();
    
	}

	/**
	 * Executes an update clause
	 * 
	 */
	public void execute() throws EngineException {
		if (this.getSource().getConnection() == this.getTarget().getConnection()) {
			executeInsideDB();
		} else {
			executeThroughJava();
		}
	}

	/**
	 * Executes an update clause inside a database
	 * 
	 */
	protected void executeInsideDB() throws EngineException {
		try {
			super.executeInsideDB();

			this.writeDebug(this.insertClause);
			this.getSource().getConnection().executeSql(this.insertClause);
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { this.insertClause }, e, this, this
					.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}
	}

	/**
	 * Executes an update clause via Java: First select into a vector, then update
	 * into DB
	 * 
	 */
	protected void executeThroughJava() throws EngineException {

		String sqlClause = "";
		try {

			// Updates last updated value to META_SOURCES
			this.getSource().setLastTransferDate();

			sqlClause = this.getUpdateSelectClause();
			writeDebug(this.getUpdateSelectClause());
			final RockResultSet results = this.getSource().getConnection().setSelectSQL(this.getUpdateSelectClause());
			final Vector<Object> objVec = this.getSource().getSelectObjVec(results, true);
			String preparedUpdStr = this.getPreparedUpdateClause(false);
			final String preparedWhereStr = this.getTarget().getPkPreparedWhereClause(false);
			if (preparedWhereStr.length() > 0) {
				preparedUpdStr += " WHERE " + preparedWhereStr;
			}
			final String preparedInsStr = this.getTarget().getPreparedInsertClause();
			sqlClause = preparedInsStr;
			this.writeDebug(preparedUpdStr);
			this.writeDebug(preparedInsStr);
			this.getTarget().getConnection().executePreparedInsAndUpdSql(preparedUpdStr, objVec, preparedInsStr);
			results.close();
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { sqlClause }, e, this, this.getClass()
					.getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}

	}

}
