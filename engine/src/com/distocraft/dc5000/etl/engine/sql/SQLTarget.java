package com.distocraft.dc5000.etl.engine.sql;

import java.util.Vector;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.RemoveDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_columnsFactory;
import com.distocraft.dc5000.etl.rock.Meta_joints;
import com.distocraft.dc5000.etl.rock.Meta_jointsFactory;
import com.distocraft.dc5000.etl.rock.Meta_tables;
import com.distocraft.dc5000.etl.rock.Meta_target_tables;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that holds common methods for all SQL actions that have target
 * component
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLTarget extends TransferActionBase {
	// The name of the target table
	private String targetTableName;
	// Target table pk
	private Long targetTableId;
	// Target table connection id
	private Long targetConnectionId;
	// Target table db connection
	private RockFactory targetConnection;
	// Column joints
	private Meta_jointsFactory joinedColumns;
	// target columns
	private Vector<Meta_columns> targetColumns;
	// Object for checkin foreign key errors
	protected SQLFkFactory sqlFkFactory;
	// Batch column name
	private String batchColumnName;
	// The whole table object
	private Meta_tables tables;
	// All columns of the target table (not just joined)
	private Meta_columnsFactory tableColumns;

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
	 *          object that hold transfer action information (db contents)
	 * 
	 * @author Jukka Jaaheimo
	 * @since JDK1.1
	 */
	public SQLTarget(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final String batchColumnName)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		String tempTableName = "META_TARGET_TABLES";

		try {
			this.batchColumnName = batchColumnName;
			final Meta_target_tables targetTable = new Meta_target_tables(rockFact, version.getVersion_number(),
					collectionSetId, collection.getCollection_id(), transferActionId, (Long) null, (Long) null);

			tempTableName = "META_TABLES";

			this.tables = new Meta_tables(this.getRockFact(), targetTable.getTable_id(), this.getVersionNumber(),
					targetTable.getConnection_id());

			// this.tableColumns = this.tables.getMeta_columnss();

			// hack we dont use this action...
			this.tableColumns = null;

			this.targetTableName = tables.getTable_name();
			this.targetTableId = targetTable.getTable_id();
			this.targetConnectionId = targetTable.getConnection_id();

			this.targetConnection = connectionPool.getConnect(this, this.getVersionNumber(), this.targetConnectionId);

			final String orderByStr = "ORDER BY FILE_ORDER_BY,COLUMN_ID_SOURCE_COLUMN,ID";

			tempTableName = "META_JOINTS";

			final Meta_joints whereJoint = new Meta_joints(this.getRockFact());
			whereJoint.setVersion_number(this.getVersionNumber());
			whereJoint.setCollection_set_id(this.getCollectionSetId());
			whereJoint.setCollection_id(this.getCollectionId());
			whereJoint.setTransfer_action_id(this.getTransferActionId());
			whereJoint.setTarget_table_id(this.targetTableId);
			this.joinedColumns = new Meta_jointsFactory(this.getRockFact(), whereJoint, orderByStr);

			final Meta_columns whereCol = new Meta_columns(this.getRockFact());
			whereCol.setVersion_number(this.getVersionNumber());
			whereCol.setConnection_id(this.targetConnectionId);
			whereCol.setTable_id(this.targetTableId);

			this.targetColumns = new Vector<Meta_columns>();

			for (int i = 0; i < this.getJoinedColumns().size(); i++) {
				final Meta_joints joint = this.getJoinedColumns().getElementAt(i);
				whereCol.setColumn_id(joint.getColumn_id_target_column());

				tempTableName = "META_COLUMNS";

				final Meta_columns column = new Meta_columns(this.getRockFact(), whereCol);

				this.targetColumns.addElement(column);

			}

			this.sqlFkFactory = new SQLFkFactory(version, collectionSetId, collection, transferActionId, transferBatchId,
					this.targetConnectionId, this.targetConnection, rockFact, trActions, this.targetTableId, this.targetTableName);
		}

		catch (Exception e) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_READ_METADATA, new String[] { tempTableName }, e, this,
					this.getClass().getName());
		}

	}

	/**
	 * return a prepared insert clause, where all columns are replased by "?"
	 * 
	 * @return Prepared sql string
	 */
	public String getPreparedInsertClause() {
		String insertStr = "";

		if (this.getJoinedColumns() != null) {

			insertStr += "INSERT INTO " + this.targetTableName + "(";

			insertStr += getCommaSeparatedColumns(false, false);

			insertStr += ") VALUES (" + getPreparedValuesStr(false, false) + ")";
		}
		return insertStr;
	}

	/**
	 * Returns the name of the target table
	 * 
	 */
	public String getTableName() {
		return this.targetTableName;
	}

	/**
	 * Returns a comma separated "?" string.
	 * 
	 * @return comma separated "?" string
	 */
	public String getPreparedValuesStr(final boolean getPrimaryKeys, final boolean getUpdatable) {

		String columnsStr = "";

		int counter = 0;

		if (this.getJoinedColumns() != null) {
			for (int i = 0; i < this.getJoinedColumns().size(); i++) {
				final Meta_joints metaJoint = (Meta_joints) this.getJoinedColumns().getElementAt(i);

				if ((!getPrimaryKeys && !getUpdatable) || (getPrimaryKeys && metaJoint.getIs_pk_column().equals("Y"))
						|| (getUpdatable && metaJoint.getIs_pk_column().equals("N"))) {

					if (counter > 0) {
						columnsStr += ",";
					}
					counter++;
					columnsStr += "?";
				}
			}
			if (tableContainsBatchColumn()) {
				columnsStr += ",'" + this.getTransferBatchId() + "'";
			}
		}

		return columnsStr;

	}

	public String getColumnName(final int i, final boolean getPrimaryKeys, final boolean getUpdatable) {
		final Meta_joints metaJoint = (Meta_joints) this.getJoinedColumns().getElementAt(i);
		final Meta_columns targetColumn = (Meta_columns) this.targetColumns.elementAt(i);

		if ((!getPrimaryKeys && !getUpdatable)
				|| (getPrimaryKeys && metaJoint.getIs_pk_column().equals("Y"))
				|| (getUpdatable && metaJoint.getIs_pk_column().equals("N"))) {

			return targetColumn.getColumn_name();
		}
		return null;
	}

	/**
	 * Returns a comma separated column string.
	 * 
	 * @return comma separated columnn string
	 */
	public String getCommaSeparatedColumns(final boolean getPrimaryKeys, final boolean getUpdatable) {

		String columnsStr = "";

		int counter = 0;

		if (this.getJoinedColumns() != null) {
			for (int i = 0; i < this.getJoinedColumns().size(); i++) {
				final String tempColumnStr = this.getColumnName(i, getPrimaryKeys, getUpdatable);

				if (tempColumnStr != null) {
					if (counter > 0) {
						columnsStr += ",";
					}
					counter++;
					columnsStr += tempColumnStr;
				}
			}
			if (tableContainsBatchColumn()) {
				columnsStr += "," + this.batchColumnName;
			}
		}

		return columnsStr;

	}

	/**
	 * Returns a comma separated column string.
	 * 
	 * @return comma separated columnn string
	 */
	public String getCommaSeparatedColumns(final boolean getPrimaryKeys, final boolean getUpdatable, final boolean getBatchColumn) {

		String columnsStr = "";

		int counter = 0;

		if (this.getJoinedColumns() != null) {
			for (int i = 0; i < this.getJoinedColumns().size(); i++) {
				final String tempColumnStr = this.getColumnName(i, getPrimaryKeys, getUpdatable);

				if (tempColumnStr != null) {
					if (counter > 0) {
						columnsStr += ",";
					}
					counter++;
					columnsStr += tempColumnStr;
				}
			}
			if (tableContainsBatchColumn() && getBatchColumn) {
				columnsStr += "," + this.batchColumnName;
			}
		}

		return columnsStr;

	}

	/**
	 * Returns the target select clause.
	 * 
	 * @return the select clause
	 */
	public String getSelectClause(final boolean getPrimaryKeys, final boolean getUpdatable) {

		String selectStr = "";

		if (this.getJoinedColumns() != null) {
			selectStr = "SELECT ";

			int counter = 0;

			for (int i = 0; i < this.getJoinedColumns().size(); i++) {
				final Meta_joints metaJoint = (Meta_joints) this.getJoinedColumns().getElementAt(i);
				final Meta_columns targetColumn = (Meta_columns) this.targetColumns.elementAt(i);

				if ((!getPrimaryKeys && !getUpdatable)
						|| (getPrimaryKeys && metaJoint.getIs_pk_column().equals("Y"))
						|| (getUpdatable && metaJoint.getIs_pk_column().equals("N"))) {

					if (counter > 0) {
						selectStr += ",";
					}
					counter++;

					selectStr += targetColumn.getColumn_name();
				}
			}
			selectStr += " FROM " + this.targetTableName + " " + EngineConstants.TARGET_TABLE_ALIAS;
		}

		return selectStr;

	}

	/**
	 * Returns the primary key where clause
	 * 
	 * @return the primary key where clause
	 */
	public String getPkWhereClause(final SQLSource source) {

		String selectStr = "";

		if (this.getJoinedColumns() != null) {

			int counter = 0;

			for (int i = 0; i < this.getJoinedColumns().size(); i++) {
				final Meta_joints metaJoint = (Meta_joints) this.getJoinedColumns().getElementAt(i);
				final Meta_columns targetColumn = (Meta_columns) this.targetColumns.elementAt(i);
				final Meta_columns sourceColumn = (Meta_columns) source.getColumns().elementAt(i);

				if (metaJoint.getIs_pk_column().equals("Y")) {

					if (counter > 0) {
						selectStr += " AND ";
					}
					counter++;

					selectStr += EngineConstants.TARGET_TABLE_ALIAS + "." + targetColumn.getColumn_name() + " = ";
					selectStr += EngineConstants.SOURCE_TABLE_ALIAS + "." + sourceColumn.getColumn_name();
				}
			}
		}

		return selectStr;

	}

	/**
	 * Returns the primary key where clause for prepared sql
	 * 
	 * @return the primary key where clause
	 */
	public String getPkPreparedWhereClause(final boolean useTargetAliasName) {

		String selectStr = "";

		if (this.getJoinedColumns() != null) {

			int counter = 0;

			for (int i = 0; i < this.getJoinedColumns().size(); i++) {
				final Meta_joints metaJoint = (Meta_joints) this.getJoinedColumns().getElementAt(i);
				final Meta_columns targetColumn = (Meta_columns) this.targetColumns.elementAt(i);

				if (metaJoint.getIs_pk_column().equals("Y")) {

					if (counter > 0) {
						selectStr += " AND ";
					}
					counter++;

					if (useTargetAliasName) {
						selectStr += EngineConstants.TARGET_TABLE_ALIAS + ".";
					}
					selectStr += targetColumn.getColumn_name() + " = ? ";
				}
			}
		}

		return selectStr;

	}

	/**
	 * Compares target columns and batchColumnName, if a match is found -> true
	 * 
	 * @return boolean true if any target column == batch column name
	 */
	public boolean tableContainsBatchColumn() {

		//if (this.batchColumnName == null) {	//JVesey 22/07/2011 this.tablecolumns causes error in for loop below if null
		if (this.batchColumnName == null || this.tableColumns==null) {
			return false;
		}

		for (int i = 0; i < this.tableColumns.size(); i++) {

			final Meta_columns column = (Meta_columns) this.tableColumns.getElementAt(i);

			if (column.getColumn_name().toUpperCase().equals(this.batchColumnName.toUpperCase())) {
				return true;
			}

		}
		return false;
	}

	/**
	 * If transfer fails, removes the data transferred before fail
	 * 
	 */
	public void removeDataFromTarget() throws EngineMetaDataException, RemoveDataException {

		if (this.batchColumnName != null) {

			if (tableContainsBatchColumn()) {
				String sqlStr = "DELETE FROM " + this.targetTableName;
				sqlStr += " WHERE " + this.batchColumnName + "=" + this.getTransferBatchId();
				this.writeDebug(sqlStr);
				try {
					this.getConnection().executeSql(sqlStr);
				} catch (Exception e) {
					throw new RemoveDataException(EngineConstants.CANNOT_REMOVE_DATA, new String[] { this.targetTableName,
							this.batchColumnName }, e, this, this.getClass().getName());
				}
			}
		}
	}

	/**
	 * Get methods for member variables
	 * 
	 * 
	 */
	public Long getConnectionId() {
		return this.targetConnectionId;
	}

	public RockFactory getConnection() {
		return this.targetConnection;
	}

	public Meta_jointsFactory getJoinedColumns() {
		return this.joinedColumns;
	}

	public Vector getColumns() {
		return this.targetColumns;
	}

	public SQLFkFactory getSqlFkFactory() {
		return this.sqlFkFactory;
	}

}
