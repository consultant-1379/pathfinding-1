package com.distocraft.dc5000.etl.engine.file;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.util.Vector;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.RemoveDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.sql.SQLOperation;
import com.distocraft.dc5000.etl.engine.sql.SQLTarget;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_joints;
import com.distocraft.dc5000.etl.rock.Meta_jointsFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that implements file construction from a table
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLInputFromFile extends SQLOperation {
	private final String preparedInsertClause;
	// Batch column name
	private final String batchColumnName;
	// Target table
	private final SQLTarget target;
	// File object
	private final SQLFile sqlFile;

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
	public SQLInputFromFile(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool,final Meta_transfer_actions trActions, final String batchColumnName)
			throws EngineMetaDataException {
		
		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.sqlFile = new SQLFile(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
				rockFact, trActions, batchColumnName);

		this.target = new SQLTarget(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
				rockFact, connectionPool, trActions, batchColumnName);

		this.batchColumnName = batchColumnName;
		this.preparedInsertClause = target.getPreparedInsertClause();

	}

	/**
	 * Finds a column that is joined into the specified position in the file.
	 * 
	 * @param position
	 * @return Meta_columns
	 */
	private Meta_columns getColumnElementBasedOnPosition(final int position) {

		final Meta_jointsFactory joints = this.target.getJoinedColumns();

		for (int i = 0; i < joints.size(); i++) {

			final Meta_joints joint = joints.getElementAt(i);

			if (joint.getFile_order_by().equals(new Long(position))) {
				final Meta_columns column = (Meta_columns) this.target.getColumns().elementAt(i);
				return column;
			}
		}

		return null;
	}

	/**
	 * Executes the file output
	 * 
	 */
	public void execute() throws EngineException {
		try {
			final FileReader fileReader = new FileReader(this.sqlFile.getFile());
			final BufferedReader buffReader = new BufferedReader(fileReader);

			final Vector rowVec = new Vector();
			int rowsToCommit = 0;

			String lineStr = "";
			boolean isFirstInsert = true;
			while (lineStr != null) {

				rowsToCommit++;
				lineStr = buffReader.readLine();

				if ((lineStr != null) && !lineStr.trim().equals("")) {
					Object[] vecs = new Object[2];
					final Vector objVec = new Vector();
					final Vector pkVec = new Vector();
					vecs[0] = objVec;
					vecs[1] = pkVec;
					int colCounter = 0;

					if (!this.sqlFile.getFillWithBlanks()) {
						final String colDelim = this.sqlFile.getColumnDelim();
						if (colDelim == null) {
							throw new EngineException(EngineConstants.NO_COL_DELIM_TEXT, null, this, this.getClass().getName(),
									EngineConstants.ERR_TYPE_DEFINITION);
						}
						final StringTokenizer st = new StringTokenizer(lineStr, colDelim, true);

						String prevToken = colDelim;
						String strFromToken = "";

						while (st.hasMoreTokens()) {

							strFromToken = st.nextToken();

							String strCell = strFromToken;

							if ((strCell.equals(colDelim)) && !st.hasMoreTokens()) {
								strCell = "";
							}
							if ((strCell.equals(colDelim)) && (prevToken.equals(colDelim))) {
								strCell = "";
							}
							prevToken = strFromToken;

							if (!strCell.equals(colDelim)) {

								colCounter++;

								final Meta_columns column = this.getColumnElementBasedOnPosition(colCounter);

								if (column != null) {
									// String convertedStr =
									// createConvertedString(column,strCell);
									objVec.addElement(new Object[] { strCell, null });
								}
							}
						}
					} else {
						int startPos = 0;
						final int maxPos = lineStr.length() - 1;

						for (int i = 0; i < this.target.getJoinedColumns().size(); i++) {
							final Meta_joints joint = (Meta_joints) this.target.getJoinedColumns().getElementAt(i);
							final Meta_columns column = (Meta_columns) this.target.getColumns().elementAt(colCounter);

							if (joint.getColumn_space_at_file() == null) {
								throw new EngineException(EngineConstants.EX_NO_COL_LENGTH_TEXT,
										new String[] { column.getColumn_name() }, null, this, this.getClass().getName(),
										EngineConstants.ERR_TYPE_DEFINITION);
							}
							final int cellEndPos = joint.getColumn_space_at_file().intValue();

							if ((cellEndPos + startPos) > maxPos) {
								throw new EngineException(EngineConstants.EX_TOO_LONG_COL_TEXT,
										new String[] { column.getColumn_name() }, null, this, this.getClass().getName(),
										EngineConstants.ERR_TYPE_DEFINITION);
							}
							final String strCell = lineStr.substring(startPos, cellEndPos);
							startPos += cellEndPos;

							// String convertedStr = createConvertedString(column,strCell);
							objVec.addElement(strCell);

							colCounter++;

						}

					}

					rowVec.addElement(vecs);

				}
				if (((this.sqlFile.getCommitAfterNRows() > 0) && (this.sqlFile.getCommitAfterNRows() == rowsToCommit))
						|| (lineStr == null)) {

					if (isFirstInsert) {
						this.writeDebug(this.preparedInsertClause);
					}
					isFirstInsert = false;
					this.target.getConnection().executePreparedSql(this.preparedInsertClause, rowVec);
					this.target.getConnection().commit();
					rowsToCommit = 0;

					for (int i = 0; i < rowVec.size(); i++) {
						final Object[] sVecs = (Object[]) rowVec.elementAt(i);
						final Vector objVec = (Vector) sVecs[0];
						objVec.removeAllElements();
					}
					rowVec.removeAllElements();
				}
			}
			buffReader.close();
			fileReader.close();

		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_READ_FILE, new String[] { this.sqlFile.getFileName() }, e, this,
					this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}

	}

	/**
	 * Executes the foreign key constraint checking
	 * 
	 * @return int number of fk errors
	 */
	public int executeFkCheck() throws EngineException {
		return this.target.getSqlFkFactory().executeFkCheck();
	}

	/**
	 * If transfer fails, removes the data transferred before fail
	 * 
	 */
	public void removeDataFromTarget() throws EngineMetaDataException, RemoveDataException {
		this.target.removeDataFromTarget();
	}

	public String getBatchColumnName() {
		return batchColumnName;
	}
}
