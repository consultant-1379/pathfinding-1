package com.distocraft.dc5000.etl.engine.plugin;

import java.lang.reflect.Method;
import java.util.List;
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
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that implements plugin construction from a table
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class PluginToSql extends SQLOperation {

	// Plugin
	private final TransferPlugin plugin;
	// Target table
	private final SQLTarget target;
	// The insert clause
	private final String preparedInsertClause;

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
	public PluginToSql(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final String batchColumnName,
			final PluginLoader pLoader) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.plugin = new TransferPlugin(version, collectionSetId, collection, transferActionId, transferBatchId,
				connectId, rockFact, trActions, pLoader);

		this.target = new SQLTarget(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
				rockFact, connectionPool, trActions, batchColumnName);

		this.preparedInsertClause = target.getPreparedInsertClause();

	}

	/**
	 * Executes the file output
	 * 
	 */
	public void execute() throws EngineException {
		try {
			final Object pluginObject = plugin.getPluginObject();
			final Method pluginIterateMethod = this
					.getObjectMethod(pluginObject, EngineConstants.PLUGIN_ITERATOR_METHOD_NAME, null);
			final Method pluginHasNextMethod = this.getObjectMethod(pluginObject, EngineConstants.PLUGIN_HASNEXT_METHOD_NAME, null);
			final List<String> pluginGetMethodNames = plugin.getJoinedPluginMethodNames();
			final List<Object[]> pluginGetMethodParams = plugin.getJoinedPluginMethodParams();

			final Vector<Object[]> rowVec = new Vector<Object[]>();
			int rowsToCommit = 0;
			boolean isFirstInsert = true;

			while (((Boolean) pluginIterateMethod.invoke(pluginObject, (Object)null)).booleanValue()) {

				rowsToCommit++;

				Object[] vecObjs = new Object[2];
				final Vector<Object> objVec = new Vector<Object>();
				final Vector<Object> pkVec = new Vector<Object>();
				vecObjs[0] = objVec;
				vecObjs[1] = pkVec;
				for (int i = 0; i < pluginGetMethodNames.size(); i++) {

					final String methodName = (String) pluginGetMethodNames.get(i);
					final Object[] objs = (Object[]) pluginGetMethodParams.get(i);
					final Method pluginMethod = this.getObjectMethod(pluginObject, methodName, objs);
					final String strCell = (String) pluginMethod.invoke(pluginObject, objs);

					final Meta_columns column = (Meta_columns) this.target.getColumns().elementAt(i);

					final Object convertedStr = createConvertedString(column, strCell);
					objVec.addElement(new Object[] { convertedStr, null });

				}
				rowVec.add(vecObjs);

				if (((plugin.getCommitAfterNRows() > 0) && (plugin.getCommitAfterNRows() == rowsToCommit))
						|| !((Boolean) pluginHasNextMethod.invoke(pluginObject, (Object)null)).booleanValue()) {

					if (isFirstInsert) {
						this.writeDebug(this.preparedInsertClause);
					}

					isFirstInsert = false;
					this.target.getConnection().executePreparedSql(this.preparedInsertClause, rowVec);

					this.target.getConnection().commit();
					rowsToCommit = 0;

					for (int i = 0; i < rowVec.size(); i++) {
						final Object objs[] = (Object[]) rowVec.elementAt(i);
						((Vector) objs[0]).removeAllElements();
					}
					rowVec.removeAllElements();
				}

			}
		} catch (Exception e) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { this.preparedInsertClause }, e, this,
					this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}

	}

	/**
	 * Create a string to insert to the database accoding to column type
	 * 
	 * 
	 */
	private Object createConvertedString(final Meta_columns column, final String strCell) {

		if ((column.getColumn_type().toUpperCase().equals("TIMESTAMP"))) {
			return strCell;
		}

		if ((column.getColumn_type().toUpperCase().equals("DATE"))) {
			return strCell;
		}

		if ((column.getColumn_type().toUpperCase().equals("INT"))
				|| (column.getColumn_type().toUpperCase().equals("INTEGER"))
				|| (column.getColumn_type().toUpperCase().equals("NUMERIC"))) {
			return Integer.decode(strCell);
		}

		if ((column.getColumn_type().toUpperCase().equals("TINYINT"))
				|| (column.getColumn_type().toUpperCase().equals("SMALLINT"))
				|| (column.getColumn_type().toUpperCase().equals("UNSIGNED INT")))

		{
			return (Object) Integer.decode(strCell);
		}

		return strCell;

	}

	/**
	 * Returns the objects corresponding method
	 * 
	 * @param Object
	 *          obj The object holding the method
	 * @param String
	 *          name The method name to look for.
	 * @return Method The method found.
	 */
	private Method getObjectMethod(final Object obj, final String name, final Object[] objs) throws NoSuchMethodException {
		if (objs != null) {
			Class<?>[] paramClasses = new Class<?>[objs.length];
			for (int i = 0; i < objs.length; i++) {
				paramClasses[i] = objs[i].getClass();
			}
			final Class<?> objClass = obj.getClass();
			return objClass.getMethod(name, paramClasses);
		} else {
			final Class<?> objClass = obj.getClass();
			return objClass.getMethod(name, (Class<?>)null);
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
}
