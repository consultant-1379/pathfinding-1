package com.distocraft.dc5000.etl.engine.sql;

import java.sql.ResultSet;
import java.util.List;
import java.util.Vector;

import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_fk_table_joints;
import com.distocraft.dc5000.etl.rock.Meta_fk_table_jointsFactory;
import com.distocraft.dc5000.etl.rock.Meta_fk_tables;
import com.distocraft.dc5000.etl.rock.Meta_tables;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that holds common methods for all SQL actions that have a source
 * component
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLFkTable extends TransferActionBase {
	// The corresponding database object
	private final Meta_fk_tables dbFkTable;
	// List of Fk table joints (db objects)
	private Meta_fk_table_jointsFactory fkTableJoints;
	// The joint corresponding column names
	private List<Meta_columns> vecTargetColumnNames;
	// The joint corresponding column names
	private List<String> vecFkColumnNames;
	// The foreign key table name
	private String fkTableName;
	// The target table name
	private String targetTableName;
	// true if defective values should be filtered
	private boolean isFilterable;
	// true if defective values should be replaced
	private boolean isReplaceable;
	// the value for replacing an invalid value
	private final String replaceValue;
	// The sql clause for checking the fk constraint
	private String fkValueGetClause;
	// Sql clause for deleting invalid values
	private String fkDeleteClause;
	// Sql clause for replacing invalid values
	private String fkReplaceClause;
	// Target tables db connection for executing sql
	private final RockFactory targetConnection;

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
	 * @param trActions
	 *          object that hold transfer action information (db contents)
	 * 
	 * @author Jukka Jaaheimo
	 * @since JDK1.1
	 */
	public SQLFkTable(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory targetConnection, final RockFactory rockFact, final Meta_transfer_actions trActions,
			final Long targetTableId, final String targetTableName, final Meta_fk_tables dbFkTable)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		String tableName = "META_FK_TABLE_JOINTS";

		this.targetTableName = targetTableName;
		this.dbFkTable = dbFkTable;
		this.targetConnection = targetConnection;

		if ((this.dbFkTable.getReplace_errors_flag() != null) && (this.dbFkTable.getReplace_errors_flag().equals("Y"))) {
			this.isReplaceable = true;
		}

		if ((this.dbFkTable.getFilter_errors_flag() != null) && (this.dbFkTable.getFilter_errors_flag().equals("Y"))) {
			this.isFilterable = true;
		}
		this.replaceValue = this.dbFkTable.getReplace_errors_with();
		try {

			final Meta_fk_table_joints whereFkJoint = new Meta_fk_table_joints(rockFact, version.getVersion_number(), connectId,
					this.dbFkTable.getTable_id(), (Long) null, targetTableId, (Long) null, collectionSetId,
					collection.getCollection_id(), transferActionId);

			this.fkTableJoints = new Meta_fk_table_jointsFactory(rockFact, whereFkJoint);

			this.vecTargetColumnNames = new Vector<Meta_columns>();
			this.vecFkColumnNames = new Vector<String>();
			for (int i = 0; i < this.fkTableJoints.size(); i++) {
				final Meta_fk_table_joints joint = this.fkTableJoints.getElementAt(i);

				tableName = "META_COLUMNS";

				Meta_columns column = new Meta_columns(joint.getRockFactory(), joint.getColumn_id(), joint.getVersion_number(),
						joint.getConnection_id(), joint.getTarget_table_id());

				this.vecTargetColumnNames.add(column);

				column = new Meta_columns(joint.getRockFactory(), joint.getColumn_id_fk_column(), joint.getVersion_number(),
						joint.getConnection_id(), joint.getTable_id());
				
				this.vecFkColumnNames.add(column.getColumn_name());
			}

			tableName = "META_TABLES";

			final Meta_tables table = new Meta_tables(this.dbFkTable.getRockFactory(), this.dbFkTable.getTable_id(),
					this.dbFkTable.getVersion_number(), this.dbFkTable.getConnection_id());
			this.fkTableName = table.getTable_name();

			this.fkValueGetClause = createFkValueGetClause();
			this.fkDeleteClause = createFkDeleteClause();
			this.fkReplaceClause = createFkReplaceClause();

		} catch (Exception e) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_READ_METADATA, new String[] { tableName }, e, this, this
					.getClass().getName());
		}

	}

	/**
	 * Function to compose the fk value get clause
	 * 
	 * 
	 */
	private String createFkValueGetClause() {

		String sqlStr = "SELECT COUNT(*),";
		sqlStr += this.getTargetCommaSeparatedColumns() + " FROM ";
		sqlStr += this.targetTableName + " " + EngineConstants.TARGET_TABLE_ALIAS;
		sqlStr += " WHERE NOT EXISTS (SELECT " + this.getFkCommaSeparatedColumns();
		sqlStr += " FROM " + this.fkTableName + " " + EngineConstants.FK_TABLE_ALIAS;
		sqlStr += " WHERE " + this.getTargetFkColWhereClause(true);

		if (getFkWhereClause().length() > 0) {
			sqlStr += " AND ";
		}
		sqlStr += getFkWhereClause() + ")";
		;

		if ((this.getTargetWhereClause() != null) && (this.getTargetWhereClause().length() > 0)) {
			sqlStr += " WHERE ";
		}
		sqlStr += this.getTargetWhereClause();
		sqlStr += " GROUP BY ";
		sqlStr += this.getTargetCommaSeparatedColumns();
		return sqlStr;
	}

	/**
	 * Function to compose the fk delete clause
	 * 
	 * 
	 */
	private String createFkDeleteClause() {

		String sqlStr = "DELETE FROM ";
		sqlStr += this.targetTableName;
		sqlStr += " WHERE NOT EXISTS (SELECT " + this.getFkCommaSeparatedColumns();
		sqlStr += " FROM " + this.fkTableName + " " + EngineConstants.FK_TABLE_ALIAS;
		sqlStr += " WHERE " + this.getTargetFkColWhereClause(false);

		if (getFkWhereClause().length() > 0) {
			sqlStr += " AND ";
		}
		sqlStr += getFkWhereClause() + ")";
		;

		if ((this.getTargetWhereClause() != null) && (this.getTargetWhereClause().length() > 0)) {
			sqlStr += " WHERE ";
		}
		sqlStr += this.getTargetWhereClause();

		return sqlStr;
	}

	/**
	 * Function to compose the fk replace clause
	 * 
	 * 
	 */
	private String createFkReplaceClause() {

		String sqlStr = "UPDATE ";
		sqlStr += this.targetTableName;
		sqlStr += " SET " + this.getTargetSetValuesClause();
		sqlStr += " WHERE NOT EXISTS (SELECT " + this.getFkCommaSeparatedColumns();
		sqlStr += " FROM " + this.fkTableName + " " + EngineConstants.FK_TABLE_ALIAS;
		sqlStr += " WHERE " + this.getTargetFkColWhereClause(false);

		if (getFkWhereClause().length() > 0) {
			sqlStr += " AND ";
		}
		sqlStr += getFkWhereClause() + ")";
		;

		if ((this.getTargetWhereClause() != null) && (this.getTargetWhereClause().length() > 0)) {
			sqlStr += " WHERE ";
		}
		sqlStr += this.getTargetWhereClause();

		return sqlStr;
	}

	/**
	 * Return the target tables joined columns with "," in between
	 * 
	 */
	private String getTargetCommaSeparatedColumns() {
		String sqlStr = "";

		for (int i = 0; i < this.vecTargetColumnNames.size(); i++) {

			if (i > 0) {
				sqlStr += ",";
			}
			sqlStr += (String) ((Meta_columns) this.vecTargetColumnNames.get(i)).getColumn_name();

		}
		return sqlStr;
	}

	/**
	 * Return the fk tables joined columns with "," in between
	 * 
	 */
	private String getFkCommaSeparatedColumns() {
		String sqlStr = "";

		for (int i = 0; i < this.vecFkColumnNames.size(); i++) {

			if (i > 0) {
				sqlStr += ",";
			}
			sqlStr += (String) this.vecFkColumnNames.get(i);

		}
		return sqlStr;
	}

	/**
	 * Return the fk tables and target tables joined columns where clause
	 * 
	 */
	private String getTargetFkColWhereClause(final boolean useTargetAliasName) {
		String sqlStr = "";

		for (int i = 0; i < this.vecFkColumnNames.size(); i++) {

			if (i > 0) {
				sqlStr += " AND ";
			}
			sqlStr += EngineConstants.FK_TABLE_ALIAS + "." + (String) this.vecFkColumnNames.get(i) + " = ";
			if (useTargetAliasName) {
				sqlStr += EngineConstants.TARGET_TABLE_ALIAS + "."
						+ (String) ((Meta_columns) this.vecTargetColumnNames.get(i)).getColumn_name();
			} else {
				sqlStr += this.targetTableName + "."
						+ (String) ((Meta_columns) this.vecTargetColumnNames.get(i)).getColumn_name();
			}

		}
		return sqlStr;
	}

	/**
	 * Return the set clause from target tables joined columns
	 * 
	 */
	private String getTargetSetValuesClause() {
		String sqlStr = "";

		for (int i = 0; i < this.vecTargetColumnNames.size(); i++) {

			if (i > 0) {
				sqlStr += ",";
			}
			sqlStr += (String) ((Meta_columns) this.vecTargetColumnNames.get(i)).getColumn_name();
			if ((((Meta_columns) this.vecTargetColumnNames.get(i)).getColumn_type().toUpperCase()
					.equals(EngineConstants.NUMERIC_NAME_ORACLE))
					|| (((Meta_columns) this.vecTargetColumnNames.get(i)).getColumn_type().toUpperCase()
							.equals(EngineConstants.NUMERIC_NAME_SYBASE))) {
				sqlStr += " = " + this.replaceValue;
			} else {
				sqlStr += " = '" + this.replaceValue + "'";
			}

		}
		return sqlStr;
	}

	/**
	 * Return the where clause for fk table
	 * 
	 */
	private String getFkWhereClause() {
		final String str = this.dbFkTable.getWhere_clause();
		if (str == null) {
			return "";
		} else {
			return str;
		}
	}

	/**
	 * Return the where clause for target table
	 * 
	 */
	private String getTargetWhereClause() {
		return "";
	}

	/**
	 * Executes the fk check clause
	 * 
	 * @return int number of defective rows
	 */
	public int executeFkCheck() throws EngineException {
		final int numOfErrors = executeInsideDB();
		String sqlClause = "";
		try {

			if (this.isFilterable) {
				this.writeDebug(this.fkDeleteClause);
				sqlClause = this.fkDeleteClause;
				this.targetConnection.executeSql(this.fkDeleteClause);
			} else if (this.isReplaceable) {
				this.writeDebug(this.fkReplaceClause);
				sqlClause = this.fkReplaceClause;
				this.targetConnection.executeSql(this.fkReplaceClause);
			}

			if (numOfErrors > this.dbFkTable.getMax_errors().intValue()) {
				throw new EngineException(EngineConstants.FK_ERROR_STOP_TEXT, new String[] { "" + numOfErrors + "",
						this.dbFkTable.getMax_errors().toString() }, null, this, this.getClass().getName(),
						EngineConstants.ERR_TYPE_VALIDATION);
			}

		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { sqlClause }, e, this, this.getClass()
					.getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}

		return numOfErrors;
	}

	/**
	 * Executes the fk clause inside a database
	 * 
	 * @return number of errors
	 */
	private int executeInsideDB() throws EngineException {
		ResultSet results = null;
		try {
			this.writeDebug(this.fkValueGetClause);
			final RockResultSet rockResults = this.targetConnection.setSelectSQL(this.fkValueGetClause);
			results = rockResults.getResultSet();

			int errCount = 0;
			String errString = "";

			while (results.next()) {
				final int errors = results.getInt(1);
				errCount += errors;
				errString += "#" + this.targetTableName + "->" + this.fkTableName + "(errs:" + errors + "):";

				for (int i = 0; i < fkTableJoints.size(); i++) {
					final Object resultObject = results.getObject(i + 2);
					errString += ((Meta_columns) this.vecTargetColumnNames.get(i)).getColumn_name() + "="
							+ resultObject.toString();
				}
				errString += "#";

				if (this.isFilterable) {
					errString = "FK Constraint error (FILTERED): " + errString;
				} else if (this.isReplaceable) {
					errString = "FK Constraint error (REPLACED): " + errString;
				} else {
					errString = "FK Constraint error: " + errString;
				}
				this.writeError(errString, "SQLFkTable.execute()", EngineConstants.ERR_TYPE_WARNING);
			}
			return errCount;
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { this.fkValueGetClause }, e, this, this
					.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
		} finally {
			try {
				results.close();
			} catch(Exception e) {}
		}
	}
}
