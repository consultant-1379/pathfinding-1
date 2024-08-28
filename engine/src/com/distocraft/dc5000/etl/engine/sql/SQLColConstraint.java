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
import com.distocraft.dc5000.etl.rock.Meta_column_constraints;
import com.distocraft.dc5000.etl.rock.Meta_column_constraintsFactory;
import com.distocraft.dc5000.etl.rock.Meta_columns;
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
public class SQLColConstraint extends TransferActionBase {
	// The joint corresponding columns
	private final List<Meta_columns> vecTargetColumns;
	// The joint corresponding column constraints
	private final List<Meta_column_constraintsFactory> vecFactoryConstraints;
	// The target table name
	private final String targetTableName;
	// A vector of sqlclauses for checking the constraints
	private final List<String> vecSqlClauses;
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
	public SQLColConstraint(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory targetConnection, final RockFactory rockFact, final Meta_transfer_actions trActions,
			final Long targetTableId, final String targetTableName, final List<Meta_columns> vecTargetColumns)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		this.targetTableName = targetTableName;
		this.targetConnection = targetConnection;
		this.vecTargetColumns = vecTargetColumns;
		this.vecFactoryConstraints = new Vector<Meta_column_constraintsFactory>();

		try {

			for (Meta_columns column : this.vecTargetColumns) {

				final Meta_column_constraints whereConstraint = new Meta_column_constraints(rockFact);
				whereConstraint.setVersion_number(version.getVersion_number());
				whereConstraint.setConnection_id(connectId);
				whereConstraint.setTable_id(targetTableId);
				whereConstraint.setColumn_id(column.getColumn_id());

				final Meta_column_constraintsFactory factConstraint = new Meta_column_constraintsFactory(rockFact, whereConstraint);

				this.vecFactoryConstraints.add(factConstraint);

			}

			this.vecSqlClauses = createConstraintClauses();

		} catch (Exception e) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_READ_METADATA,
					new String[] { "META_COLUMN_CONSTRAINTS" }, e, this, this.getClass().getName());
		}

	}

	/**
	 * Function to compose the column constraint clauses
	 * 
	 * 
	 */
	private List<String> createConstraintClauses() {

		final List<String> vecSqlClauses = new Vector<String>();

		for (int i = 0; i < this.vecTargetColumns.size(); i++) {
			final Meta_columns column = (Meta_columns) this.vecTargetColumns.get(i);
			final Meta_column_constraintsFactory factConstraint = this.vecFactoryConstraints.get(i);

			String sqlStr = "";

			for (int j = 0; j < factConstraint.size(); j++) {

				if (j == 0) {
					sqlStr = "SELECT COUNT(*) FROM " + this.targetTableName + " WHERE ";
				}

				final Meta_column_constraints colConst = factConstraint.getElementAt(i);

				final String lowValue = colConst.getLow_value();
				final String highValue = colConst.getHigh_value();

				if (j > 0) {
					sqlStr += " AND ";
				}

				if ((highValue != null) && (highValue.length() > 0)) {
					sqlStr += column.getColumn_name() + " > '" + lowValue + "'";
					sqlStr += " AND " + column.getColumn_name() + " < '" + highValue + "'";
				} else {
					sqlStr += column.getColumn_name() + " = '" + lowValue + "'";
				}
			}
			vecSqlClauses.add(sqlStr);

		}
		return vecSqlClauses;
	}

	/**
	 * Executes the fk check clause
	 * 
	 * @return int number of defective rows
	 */
	public int executeColConstCheck() throws EngineException {
		return executeInsideDB();
	}

	/**
	 * Executes the fk clause inside a database
	 * 
	 * @return number of errors
	 */
	private int executeInsideDB() throws EngineException {
		String sqlClause = "";

		ResultSet results = null;
		
		try {

			int errCount = 0;
			for (int i = 0; i < this.vecSqlClauses.size(); i++) {
				sqlClause = this.vecSqlClauses.get(i);
				final Meta_columns column = (Meta_columns) this.vecTargetColumns.get(i);
				this.writeDebug(sqlClause);
				final RockResultSet rockResults = this.targetConnection.setSelectSQL(sqlClause);
				results = rockResults.getResultSet();

				String errString = "";

				while (results.next()) {
					final int errors = results.getInt(1);
					errCount += errors;
					errString += "#";
					errString += "Table:" + this.targetTableName + ", Column:" + column.getColumn_name() + ", Number of errors:"
							+ errors;
					errString += "#";

					errString = "Column Constraint error: " + errString;

					this.writeError(errString, "SQLColConstraint.execute()", EngineConstants.ERR_TYPE_WARNING);
				}
			}
			return errCount;
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { sqlClause }, e, this, this.getClass()
					.getName(), EngineConstants.ERR_TYPE_EXECUTION);
		} finally {
			try {
				results.close();
			} catch(Exception e) {}
		}

	}
}
