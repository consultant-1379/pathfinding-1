package com.distocraft.dc5000.etl.engine.sql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

import com.distocraft.dc5000.etl.engine.common.DatabaseSpecific;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_columnsFactory;
import com.distocraft.dc5000.etl.rock.Meta_joints;
import com.distocraft.dc5000.etl.rock.Meta_jointsFactory;
import com.distocraft.dc5000.etl.rock.Meta_parameter_tables;
import com.distocraft.dc5000.etl.rock.Meta_source_tables;
import com.distocraft.dc5000.etl.rock.Meta_tables;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transformation_rules;
import com.distocraft.dc5000.etl.rock.Meta_transformation_tables;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that holds common methods for all SQL actions that have a source
 * component
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLSource extends TransferActionBase {
	// The corresponding db row
	Meta_source_tables sourceTable;
	// The name of the source table
	private String sourceTableName;
	// Source table pk
	private Long sourceTableId;
	// Source table connection id
	private Long connectionId;
	// If true, only distinct values are selected
	private boolean isDistinct;
	// Last executed transfer date for the source table
	private Timestamp lastTransferDate;
	// Should last transfer date limit the selection
	private boolean useTrDateInWhere;
	// Name of the timestamp column in sourcetable
	private String timeStampColumnName;
	// Column joints
	private Meta_jointsFactory joinedColumns;
	// Source tables db connection
	private RockFactory sourceConnection;
	// If transformation table is used, this must be added to where condition.
	private String transformationWhere = "";
	// Source columns corresponding to joinedColumns
	private Vector<Meta_columns> sourceColumns;
	// Parameters corresponding to joinedColumns
	private Vector<String> parameters;
	// Transformation table columns corresponding to joinedColumns
	private Vector<String> transfTableColumns;
	// Transformation rules corresponding to joinedColumns
	private Vector transformationRules;
	// The need tables to get transformation tables
	private String transfTablesStr = "";
	// The join for transformation tables
	private String transfValueWhere = "";
	// Batch column name
	private final String batchColumnName;
	// String vector containig target table names (if any exist),
	// this is for replacing columns with META_TRANSFER_BATCHES.ID
	// (if target column name=META_PARAMETERS.BATCH_COLUMN_NAME)
	// Not created if BATCH_COLUMN_NAME = NULL
	private Vector targetColumnNames;
	// Transfer action database element
	private final Meta_transfer_actions trActions;

	private final Meta_columnsFactory allColumns;

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
	public SQLSource(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final String batchColumnName)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		this.batchColumnName = batchColumnName;
		this.trActions = trActions;

		String tempTableName = "META_SOURCE_TABLES";

		try {
			this.sourceTable = new Meta_source_tables(rockFact, transferActionId, (Long) null, collectionSetId,
					collection.getCollection_id(), (Long) null, version.getVersion_number());

			if (sourceTable.getDistinct_flag().equals("Y")) {
				this.isDistinct = true;
			} else {
				this.isDistinct = false;
			}

			if (sourceTable.getUse_tr_date_in_where_flag().equals("Y")) {
				this.useTrDateInWhere = true;
			} else {
				this.useTrDateInWhere = false;
			}

			this.lastTransferDate = sourceTable.getLast_transfer_date();

			if (sourceTable.getTimestamp_column_id() != null) {
				final Meta_columns column = new Meta_columns(this.getRockFact(), sourceTable.getTimestamp_column_id(),
						this.getVersionNumber(), this.getConnectionId(), sourceTable.getTable_id());
				this.timeStampColumnName = column.getColumn_name();
			}

			tempTableName = "META_TABLES";

			final Meta_tables tables = new Meta_tables(this.getRockFact(), sourceTable.getTable_id(),
					this.getVersionNumber(), sourceTable.getConnection_id());
			this.sourceTableId = sourceTable.getTable_id();
			this.connectionId = sourceTable.getConnection_id();

			this.sourceConnection = connectionPool.getConnect(this, this.getVersionNumber(), this.connectionId);

			String orderByStr = "ORDER BY FILE_ORDER_BY,COLUMN_ID_SOURCE_COLUMN,ID";

			tempTableName = "META_JOINTS";

			final Meta_joints whereJoint = new Meta_joints(this.getRockFact());
			whereJoint.setVersion_number(this.getVersionNumber());
			whereJoint.setCollection_set_id(this.getCollectionSetId());
			whereJoint.setCollection_id(this.getCollectionId());
			whereJoint.setTransfer_action_id(this.getTransferActionId());
			whereJoint.setSource_table_id(this.sourceTableId);
			this.joinedColumns = new Meta_jointsFactory(this.getRockFact(), whereJoint, orderByStr);

			tempTableName = "META_COLUMNS";

			final Meta_columns whereCol = new Meta_columns(this.getRockFact());
			whereCol.setVersion_number(this.getVersionNumber());
			whereCol.setConnection_id(this.connectionId);
			whereCol.setTable_id(this.sourceTableId);

			this.sourceColumns = new Vector<Meta_columns>();
			this.parameters = new Vector<String>();
			this.transformationRules = new Vector();
			this.transfTableColumns = new Vector<String>();

			final boolean isLookUpSet = false;
			final boolean isInsideLookUpSet = false;

			Meta_columns column = new Meta_columns(this.getRockFact());
			column.setVersion_number(this.getVersionNumber());
			column.setConnection_id(this.getConnectionId());
			column.setTable_id(this.sourceTable.getTable_id());

			orderByStr = "ORDER BY COLUMN_ID";
			this.allColumns = new Meta_columnsFactory(this.getRockFact(), column, orderByStr);

			final Map<Long, Meta_columns> cols = new Hashtable<Long, Meta_columns>();
			for (int i = 0; i < allColumns.size(); i++) {
				final Meta_columns col = (Meta_columns) allColumns.getElementAt(i);
				cols.put(col.getColumn_id(), col);
			}

			for (int i = 0; i < this.joinedColumns.size(); i++) {
				final Meta_joints joint = this.joinedColumns.getElementAt(i);

				column = cols.get(joint.getColumn_id_source_column());
				whereCol.setColumn_id(joint.getColumn_id_source_column());

				this.sourceColumns.addElement(column);

				this.parameters.addElement((String) null);
				this.transformationRules.addElement((String) null);

				if (joint.getPar_name() != null) {
					tempTableName = "META_PARAMETER_TABLES";

					final Meta_parameter_tables whereParTable = new Meta_parameter_tables(this.getRockFact());
					whereParTable.setVersion_number(this.getVersionNumber());
					whereParTable.setPar_name(joint.getPar_name());
					final Meta_parameter_tables parTable = new Meta_parameter_tables(this.getRockFact(), whereParTable);
					this.parameters.setElementAt(parTable.getPar_value(), i);
				}
				if (joint.getTransformation_id() != null) {
					tempTableName = "META_TRANSFORMATION_RULES";

					final Meta_transformation_rules whereRules = new Meta_transformation_rules(this.getRockFact());
					whereRules.setVersion_number(this.getVersionNumber());
					whereRules.setTransformation_id(joint.getTransformation_id());
					final Meta_transformation_rules rules = new Meta_transformation_rules(this.getRockFact(), whereRules);
					this.transformationRules.setElementAt(rules.getCode(), i);
				} else if (joint.getFree_format_transformat() != null) {
					this.transformationRules.setElementAt(joint.getFree_format_transformat(), i);
				}
				if (joint.getTransf_table_id() != null) {
					final Meta_transformation_tables trTables = new Meta_transformation_tables(this.getRockFact(),
							joint.getTransf_table_id(), this.getVersionNumber());

					if (trTables.getIs_lookup().equals("Y")) {
						final Meta_tables lookupTable = new Meta_tables(this.getRockFact(), trTables.getTable_id(),
								this.getVersionNumber(), trTables.getConnection_id());

						final Meta_columns valueCol = new Meta_columns(this.getRockFact(), trTables.getValue_column_id(),
								this.getVersionNumber(), trTables.getConnection_id(), trTables.getTable_id());
						final Meta_columns keyCol = new Meta_columns(this.getRockFact(), trTables.getKey_column_id(),
								this.getVersionNumber(), trTables.getConnection_id(), trTables.getTable_id());
						if (!isLookUpSet) {

							this.transfTablesStr += "," + lookupTable.getTable_name() + " ";
							this.transfTablesStr += EngineConstants.TRANSF_OUT_TABLE_ALIAS;
						}
						if (this.transfValueWhere.length() > 0) {
							this.transfValueWhere += " AND ";
						}

						this.transfValueWhere += " " + EngineConstants.SOURCE_TABLE_ALIAS + "." + column.getColumn_name() + " = "
								+ EngineConstants.TRANSF_OUT_TABLE_ALIAS + "." + keyCol.getColumn_name() + " ";

						this.transfTableColumns
								.addElement(EngineConstants.TRANSF_OUT_TABLE_ALIAS + "." + valueCol.getColumn_name());

					} else {
						if (trTables.getIs_lookup().equals("N")) {
							if (!isInsideLookUpSet) {

								this.transfTablesStr += "," + DatabaseSpecific.getDaggerOwnerName(this.getRockFact())
										+ ".META_TRANSFORMATION_TABLES ";
								this.transfTablesStr += EngineConstants.TRANSF_TABLE_ALIAS + ","
										+ DatabaseSpecific.getDaggerOwnerName(this.getRockFact());
								this.transfTablesStr += ".META_TRANSF_TABLE_VALUES " + EngineConstants.TRANSF_TABLE_VALUE_ALIAS;
								if (this.transfValueWhere.length() > 0) {
									this.transfValueWhere += " AND ";
								}
								this.transfValueWhere += " " + EngineConstants.TRANSF_TABLE_ALIAS + ".VERSION_NUMBER='"
										+ this.getVersionNumber() + "' AND ";
								this.transfValueWhere += " " + EngineConstants.TRANSF_TABLE_ALIAS + ".TRANSF_TABLE_ID="
										+ joint.getTransf_table_id() + " AND ";
								this.transfValueWhere += " " + EngineConstants.TRANSF_TABLE_ALIAS + ".VERSION_NUMBER="
										+ EngineConstants.TRANSF_TABLE_VALUE_ALIAS + ".VERSION_NUMBER AND ";
								this.transfValueWhere += " " + EngineConstants.TRANSF_TABLE_ALIAS + ".TRANSF_TABLE_ID="
										+ EngineConstants.TRANSF_TABLE_VALUE_ALIAS + ".TRANSF_TABLE_ID ";
							}
							this.transfValueWhere += " AND " + EngineConstants.TRANSF_TABLE_VALUE_ALIAS + ".OLD_VALUE = "
									+ EngineConstants.SOURCE_TABLE_ALIAS + "." + column.getColumn_name();
							this.transfTableColumns.addElement(EngineConstants.TRANSF_TABLE_VALUE_ALIAS + ".NEW_VALUE");

						} else {
							this.transfTableColumns.addElement("");
						}
					}
				} else {
					this.transfTableColumns.addElement("");
				}

			}

			this.targetColumnNames = new Vector();

			if (this.batchColumnName != null) {

				for (int i = 0; i < this.joinedColumns.size(); i++) {
					String targetTableName = "";
					final Meta_joints joint = this.joinedColumns.getElementAt(i);

					if ((joint.getTarget_table_id() != null) && (joint.getColumn_id_target_column() != null)) {
						tempTableName = "META_COLUMNS";

						// ----------------
						// whereCol.setConnection_id(this.connectionId); replaced by Jakub
						// the connection used by source table this.connectionId
						// may be different from target connection, so the target connection
						// ID
						// must be taken from joint.getTarget_connection_id()
						whereCol.setConnection_id(joint.getTarget_connection_id());

						whereCol.setTable_id(joint.getTarget_table_id());
						whereCol.setColumn_id(joint.getColumn_id_target_column());

						final Meta_columns col = new Meta_columns(this.getRockFact(), whereCol);

						targetTableName = col.getColumn_name();

					}
					this.targetColumnNames.addElement(targetTableName);
				}
			}
			if ((tables.getIs_join() != null) && (tables.getIs_join().equals("Y"))) {
				this.sourceTableName = buildSourceFromJoin(tables);
			} else {
				this.sourceTableName = tables.getTable_name();
			}
		} catch (Exception e) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_READ_METADATA, new String[] { tempTableName }, e, this,
					this.getClass().getName());
		}

	}

	private String buildSourceFromJoin(final Meta_tables tables) {
		final StringBuffer str = new StringBuffer();
		str.append("(SELECT ");

		if (this.allColumns != null) {

			for (int i = 0; i < this.allColumns.size(); i++) {
				final Meta_columns column = (Meta_columns) this.allColumns.getElementAt(i);
				if (i > 0) {
					str.append(",");
				}
				final String columnTableAliasName = column.getColumn_alias_name();
				str.append(columnTableAliasName);
				if (columnTableAliasName != null) {
					final String colName = columnTableAliasName.substring(columnTableAliasName.indexOf(".") + 1,
							columnTableAliasName.length());
					final String columnAliasName = column.getColumn_name();
					if ((columnAliasName != null) && !colName.equals(columnAliasName)) {
						str.append(" ");
						str.append(columnAliasName);
					}
				}

			}
		}
		str.append(" FROM ");
		str.append(tables.getTables_and_aliases());

		if ((tables.getJoin_clause() != null) && (tables.getJoin_clause().length() > 0)) {
			str.append(" WHERE ");
			str.append(tables.getJoin_clause());
		}

		str.append(")");

		return str.toString();
	}

	/**
	 * Returns the name of the source table
	 * 
	 */
	public String getTableName() {
		return this.sourceTableName;
	}

	/**
	 * Returns the where clause, the last transfer condition is added to th e
	 * clause if defined.
	 * 
	 */
	public String getWhereClause() {
		String whereClause = this.trActions.getWhere_clause();
		if (whereClause == null) {
			whereClause = "";
		}

		if (this.useTrDateInWhere) {
			if (this.lastTransferDate != null) {
				if (this.timeStampColumnName != null) {
					if (whereClause.length() > 0) {
						whereClause += " AND ";
					}
					whereClause += this.timeStampColumnName + " > ";
					whereClause += "TO_DATE('" + this.lastTransferDate + "','yyyy-mm-dd hh24:mi:ss')";

				}
			}
		}
		if (this.transfValueWhere.length() > 0) {
			if (whereClause.length() > 0) {
				whereClause += " AND ";
			}
			whereClause += this.transfValueWhere;
		}
		if (this.transformationWhere.length() > 0) {
			if (whereClause.length() > 0) {
				whereClause += " AND ";
			}
			whereClause += this.transformationWhere;
		}
		return whereClause;
	}

	/**
	 * Returns a comma separated group by string.
	 * 
	 * @return comma separated columnn string
	 */
	public String getGroupByClause() {

		String columnsStr = "";

		int counter = 0;

		if (this.getJoinedColumns() != null) {
			for (int i = 0; i < this.getJoinedColumns().size(); i++) {
				final Meta_joints metaJoint = (Meta_joints) this.getJoinedColumns().getElementAt(i);

				if (metaJoint.getIs_group_by_column().equals("Y")) {

					if (counter > 0) {
						columnsStr += ",";
					}

					counter++;
					columnsStr += getSourceColumn(i, false, false, false);
				}
			}
		}
		return columnsStr;
	}

	/**
	 * Returns the source select clause.
	 * 
	 * @return the select clause
	 */

	public String getSelectClause(final boolean targetContainsBatchColumn) {

		return getSelectClausePriv(false, false, false, targetContainsBatchColumn);

	}

	/**
	 * Returns the source select clause.
	 * 
	 * @return the select clause
	 */
	public String getSelectClause(final boolean getPrimaryKeys, final boolean getUpdatable,
			final boolean targetContainsBatchColumn) {

		return getSelectClausePriv(getPrimaryKeys, getUpdatable, false, targetContainsBatchColumn);

	}

	/**
	 * Returns the source select clause.
	 * 
	 * @return the select clause
	 */
	public String getSelectClause(final boolean setSummary, final boolean targetContainsBatchColumn) {

		return getSelectClausePriv(false, false, setSummary, targetContainsBatchColumn);

	}

	public String getSourceColumn(final int i, final boolean getPrimaryKeys, final boolean getUpdatable,
			final boolean setSummary) {

		String selectStr = "";

		final Meta_joints metaJoint = (Meta_joints) this.joinedColumns.getElementAt(i);
		final Meta_columns sourceColumn = (Meta_columns) this.sourceColumns.elementAt(i);
		if ((!getPrimaryKeys && !getUpdatable) || (getPrimaryKeys && metaJoint.getIs_pk_column().equals("Y"))
				|| (getUpdatable && metaJoint.getIs_pk_column().equals("N"))) {

			if (metaJoint.getIs_sum_column().equals("Y")) {
				selectStr += "SUM(";
			}

			if ((batchColumnName != null) && (this.batchColumnName.equals((String) this.targetColumnNames.elementAt(i)))) {
				selectStr += "'" + this.getTransferBatchId().toString() + "'";
			} else if (metaJoint.getPar_name() != null) {
				selectStr += "'" + this.parameters.elementAt(i) + "'";
			} else if (this.transformationRules.elementAt(i) != null) {
				selectStr += this.transformationRules.elementAt(i);
			} else if (metaJoint.getTransf_table_id() != null) {
				selectStr += this.transfTableColumns.elementAt(i);
			} else {
				selectStr += EngineConstants.SOURCE_TABLE_ALIAS + "." + sourceColumn.getColumn_name();
			}
			if (metaJoint.getIs_sum_column().equals("Y")) {
				selectStr += ")";
			}

		}

		return selectStr;
	}

	/**
	 * Returns the source select clause.
	 * 
	 * @return the select clause
	 */
	private String getSelectClausePriv(final boolean getPrimaryKeys, final boolean getUpdatable,
			final boolean setSummary, final boolean targetContainsBatchColumn) {

		String selectStr = "";

		if (this.joinedColumns != null) {
			selectStr = "SELECT ";

			if (this.isDistinct) {
				selectStr += "DISTINCT ";
			}
			int counter = 0;

			for (int i = 0; i < this.joinedColumns.size(); i++) {

				final String columnStr = getSourceColumn(i, getPrimaryKeys, getUpdatable, setSummary);

				if (columnStr.length() > 0) {

					if (counter > 0) {
						selectStr += ",";
					}
					counter++;
					selectStr += columnStr;
				}
			}
			if (targetContainsBatchColumn && ((getUpdatable) || (!getUpdatable && !getPrimaryKeys))) {

				selectStr += "," + this.getTransferBatchId();
			}
			selectStr += " FROM " + this.sourceTableName + " " + EngineConstants.SOURCE_TABLE_ALIAS;
			// Add TRANSFORMATION TABLE NAMES
			selectStr += this.transfTablesStr;
		}
		return selectStr;
	}

	/**
	 * Returns a vector containing source table db selection objects
	 * 
	 * @param results
	 *          The database result set.
	 * @param getPkValuesForWhere
	 *          Are pk values added separately at the end of objVec for where part
	 */
	public Vector<Object> getSelectObjVec(final RockResultSet results, final boolean getPkValuesForWhere)
			throws SQLException {
		final Vector<Object> rowVec = new Vector<Object>();

		while (results.getResultSet().next()) {
			Object[] vecs = new Object[2];
			final Vector objVec = new Vector();
			final Vector pkVec = new Vector();
			vecs[0] = objVec;
			vecs[1] = pkVec;

			for (int i = 0; i < this.getJoinedColumns().size(); i++) {
				final Meta_joints joint = this.getJoinedColumns().getElementAt(i);

				Object[] obj = new Object[2];
				final Object obj1 = results.getResultSet().getObject(i + 1);
				int sqlType = 0;
				if (obj1 == null) {
					final ResultSetMetaData resultMetaData = results.getResultSet().getMetaData();
					sqlType = resultMetaData.getColumnType(i + 1);
				}
				obj[0] = obj1;
				obj[1] = new Integer(sqlType);
				objVec.addElement(obj);

				if (getPkValuesForWhere) {

					if (joint.getIs_pk_column().equals("Y")) {
						Object[] pkObj = new Object[2];
						final Object pkObj1 = results.getResultSet().getObject(i + 1);
						if (pkObj1 == null) {
							final ResultSetMetaData resultMetaData = results.getResultSet().getMetaData();
							sqlType = resultMetaData.getColumnType(i + 1);
						}
						pkObj[0] = pkObj1;
						pkObj[1] = new Integer(sqlType);
						pkVec.addElement(pkObj);
					}
				}
			}

			rowVec.addElement(vecs);
		}
		return rowVec;
	}

	/**
	 * Update the last transfer date into META_SOURCES table
	 * 
	 */

	public void setLastTransferDate() throws EngineException {
		try {
			if (this.useTrDateInWhere) {
				if (this.timeStampColumnName != null) {
					this.sourceTable.setLast_transfer_date(new Timestamp(System.currentTimeMillis()));
					this.sourceTable.updateDB();

				}
			}
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_SET_TIMESTAMP, new String[] { this.sourceTableName,
					this.timeStampColumnName }, e, this, this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}
	}

	/**
	 * Returns wheather the select part should be distinct
	 */
	/**public String getIsDistinct() {
		return this.getIsDistinct();
	}
**/
	public boolean getIsDistinct() {	//JVesey 29/07/2011 getter should not return getIsDistinct() above
		return this.isDistinct;
	}
	/**
	 * Get methods for member variables
	 */
	public final Long getConnectionId() {
		return this.connectionId;
	}

	public RockFactory getConnection() {
		return this.sourceConnection;
	}

	public Meta_jointsFactory getJoinedColumns() {
		return this.joinedColumns;
	}

	public Vector<Meta_columns> getColumns() {
		return this.sourceColumns;
	}

	public Vector<String> getParameters() {
		return this.parameters;
	}

	public Vector getTransformationRules() {
		return this.transformationRules;
	}

	public Meta_source_tables getTable() {
		return this.sourceTable;
	}

}