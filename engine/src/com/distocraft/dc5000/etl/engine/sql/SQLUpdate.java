package com.distocraft.dc5000.etl.engine.sql;

import java.util.Vector;

import ssc.rockfactory.FactoryRes;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.RemoveDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that executes SQL insert clauses
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLUpdate extends SQLOperation {
	// The source table object
	private SQLSource source;
	// The target table object
	private final SQLTarget target;
	// The insert clause
	private String updateClause;
	// The select part of update clause
	private String updateSelectClause;
	// Batch column name
	private final String batchColumnName;

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
	public SQLUpdate(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final String batchColumnName)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.source = new SQLSource(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
				rockFact, connectionPool, trActions, batchColumnName);

		this.target = new SQLTarget(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
				rockFact, connectionPool, trActions, batchColumnName);

		this.batchColumnName = batchColumnName;

		if ((this.target.getPkWhereClause(this.source).trim().length() > 0)
				|| this.target.getTableName().equals(this.source.getTableName())
				|| this.source.getWhereClause().trim().length() > 0) {

			this.updateSelectClause = this.source.getSelectClause(false, false, this.target.tableContainsBatchColumn());
			if (this.source.getWhereClause().trim().length() > 0) {
				this.updateSelectClause += " WHERE ";
				this.updateSelectClause += this.source.getWhereClause();
			}

			if (this.target.getConnection().getDriverName().indexOf(FactoryRes.SYBASE_DRIVER_NAME) > 0) {
				this.setSybaseUpdateClause();
			} else {
				this.setOracleUpdateClause();
			}
		} else {
			throw new EngineMetaDataException(EngineConstants.NO_PRIMARY_KEY_DEFINED, new String[] {
					this.source.getTableName(), this.target.getTableName() }, null, this, this.getClass().getName());
		}
	}

	/**
	 * Executes an update clause
	 * 
	 */
	public void execute() throws EngineException {
		// Updates last transferred value to META_SOURCES
		this.source.setLastTransferDate();

		if (this.source.getConnection() == this.target.getConnection()) {
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
			this.writeDebug(this.updateClause);
			this.source.getConnection().executeSql(this.updateClause);
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { this.updateClause }, e, this, this
					.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}
	}

	/**
	 * Executes an update clause via Java: First select into a vector, then update
	 * into DB
	 * 
	 */
	protected void executeThroughJava() throws EngineException {
		try {

			writeDebug(this.getUpdateSelectClause());
			final RockResultSet results = this.source.getConnection().setSelectSQL(this.updateSelectClause);
			final Vector objVec = this.source.getSelectObjVec(results, true);
			String preparedSqlStr = this.getPreparedUpdateClause(false);
			final String preparedWhereStr = this.target.getPkPreparedWhereClause(false);
			if (preparedWhereStr.length() > 0) {
				preparedSqlStr += " WHERE " + preparedWhereStr;
			}
			this.writeDebug(preparedSqlStr);
			this.target.getConnection().executePreparedSql(preparedSqlStr, objVec);
			results.close();
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { this.getUpdateSelectClause() }, e, this,
					this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}

	}

	/**
	 * A method to create a prepared update clause
	 * 
	 */
	public String getPreparedUpdateClause(final boolean useTargetAliasName) {
		String updateStr = "";

		if (this.target.getJoinedColumns() != null) {
			updateStr += "UPDATE " + this.target.getTableName();
			if (useTargetAliasName) {
				updateStr += " " + EngineConstants.TARGET_TABLE_ALIAS;
			}
			updateStr += " SET ";
			int counter = 0;

			for (int i = 0; i < this.target.getJoinedColumns().size(); i++) {
				final Meta_columns targetColumn = (Meta_columns) this.target.getColumns().elementAt(i);

				if (counter > 0) {
					updateStr += ",";
				}
				counter++;
				updateStr += targetColumn.getColumn_name() + " = ? ";

			}
		}

		return updateStr;
	}

	/**
	 * Sets the Sybase specific update clause.
	 * 
	 */
	private void setSybaseUpdateClause() {
		this.updateClause = "UPDATE " + this.target.getTableName() + " " + EngineConstants.TARGET_TABLE_ALIAS + " SET ";
		int counter = 0;
		for (int i = 0; i < this.target.getJoinedColumns().size(); i++) {
			final String targetColumn = this.target.getColumnName(i, false, true);
			final String sourceColumn = this.source.getSourceColumn(i, false, true, false);
			if (targetColumn != null) {
				if (counter > 0) {
					this.updateClause += ",";
				}
				counter++;
				this.updateClause += EngineConstants.TARGET_TABLE_ALIAS + "." + targetColumn + "=";
				this.updateClause += sourceColumn;
			}
		}
		if (this.target.tableContainsBatchColumn()) {
			this.updateClause += "," + batchColumnName + "=" + this.source.getTransferBatchId();
		}
		final String pkWhereClause = this.target.getPkWhereClause(this.source).trim();
		final String whereClause = this.source.getWhereClause().trim();

		if (pkWhereClause.length() > 0 || !this.target.getTableName().equals(this.source.getTableName())) {

			this.updateClause += " FROM " + this.target.getTableName() + " " + EngineConstants.TARGET_TABLE_ALIAS + ",";
			this.updateClause += this.source.getTableName() + " " + EngineConstants.SOURCE_TABLE_ALIAS;

		}

		if ((pkWhereClause.length() > 0) || (whereClause.length() > 0)) {
			this.updateClause += " WHERE ";

			this.updateClause += pkWhereClause;

			if ((pkWhereClause.length() > 0) && (whereClause.length() > 0)) {
				this.updateClause += " AND ";
			}

			this.updateClause += whereClause;
		}

	}

	/**
	 * Sets the Oracle specific update clause.
	 * 
	 * @return Sqlclause
	 */
	private void setOracleUpdateClause() {
		final String pkWhereClause = this.target.getPkWhereClause(this.source).trim();
		final String whereClause = this.source.getWhereClause().trim();

		this.updateClause = "UPDATE " + this.target.getTableName() + " " + EngineConstants.TARGET_TABLE_ALIAS + " SET ";
		if ((pkWhereClause.length() == 0) && this.target.getTableName().equals(this.source.getTableName())) {

			int counter = 0;
			for (int i = 0; i < this.target.getJoinedColumns().size(); i++) {
				final String targetColumn = this.target.getColumnName(i, false, true);
				final String sourceColumn = this.source.getSourceColumn(i, false, true, false);
				if (targetColumn != null) {
					if (counter > 0) {
						this.updateClause += ",";
					}
					counter++;
					this.updateClause += EngineConstants.TARGET_TABLE_ALIAS + "." + targetColumn + "=";
					this.updateClause += sourceColumn;
				}
			}
			if (this.target.tableContainsBatchColumn()) {
				this.updateClause += "," + batchColumnName + "=" + this.source.getTransferBatchId();
			}
			if (whereClause.length() > 0) {
				this.updateClause += " WHERE ";
				this.updateClause += whereClause;
			}

		} else {
			this.updateClause += "(";
			this.updateClause += this.target.getCommaSeparatedColumns(false, true) + ")=(";
			this.updateClause += this.source.getSelectClause(false, true, this.target.tableContainsBatchColumn());

			if ((pkWhereClause.length() > 0) || (whereClause.length() > 0)) {

				this.updateClause += " WHERE ";

				this.updateClause += whereClause;

				if ((pkWhereClause.length() > 0) && (whereClause.length() > 0)) {
					this.updateClause += " AND ";
				}

				this.updateClause += pkWhereClause + ")";
			}

			this.updateClause += " WHERE (" + this.target.getCommaSeparatedColumns(true, false, false) + ") IN ";
			this.updateClause += "(" + this.source.getSelectClause(true, false, this.target.tableContainsBatchColumn());
			if (this.source.getWhereClause().length() > 0) {
				this.updateClause += " WHERE " + this.source.getWhereClause();
			}
			this.updateClause += " INTERSECT ";
			this.updateClause += this.target.getSelectClause(true, false) + ")";
		}
	}

	/**
	 * Get methods for member variables
	 * 
	 */

	public String getUpdateClause() {
		return this.updateClause;
	}

	public String getUpdateSelectClause() {
		return this.updateSelectClause;
	}

	public SQLSource getSource() {
		return this.source;
	}

	public SQLTarget getTarget() {
		return this.target;
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
	 * 
	 */
	public void removeDataFromTarget() throws EngineMetaDataException, RemoveDataException {
		// no need to remove the data for updates
		// this.target.removeDataFromTarget();
	}
}
