package com.distocraft.dc5000.etl.engine.plugin;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_joints;
import com.distocraft.dc5000.etl.rock.Meta_jointsFactory;
import com.distocraft.dc5000.etl.rock.Meta_plugins;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that implements common plugin handling
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class TransferPlugin extends TransferActionBase {
	
	// The database plugin object
	private Meta_plugins dbPlugin;
	// Plugin method names
	private List<String> pluginMethodNames;
	// Plugin method parameters
	private List<Object[]> pluginMethodParams;
	// Plugin class
	private Class pluginClass;
	// Instantiated Plugin Object
	private Object pluginObject;
	// Meta joints that have a plugin method name
	private Meta_jointsFactory joinedColumns;

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
	public TransferPlugin(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions, final PluginLoader pLoader)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		String tableName = "META_PLUGINS";

		try {

			this.dbPlugin = new Meta_plugins(rockFact, (Long) null, collectionSetId, collection.getCollection_id(),
					version.getVersion_number(), transferActionId);

			if (this.dbPlugin.getPlugin_name() == null) {
				throw new EngineMetaDataException(EngineConstants.NO_PLUGIN_NAME, null, this, this.getClass().getName());
			}

			this.pluginClass = pLoader.loadClass(this.getPluginName());
			final Constructor<?> pluginConstr = pluginClass.getConstructor(toClassArray(this.dbPlugin.getConstructor_parameter()));
			this.pluginObject = pluginConstr.newInstance(stringToArray(this.dbPlugin.getConstructor_parameter()));

			tableName = "META_JOINTS";

			final String orderByStr = "ORDER BY FILE_ORDER_BY,ID";
			final Meta_joints whereJoint = new Meta_joints(this.getRockFact());
			whereJoint.setVersion_number(this.getVersionNumber());
			whereJoint.setCollection_set_id(this.getCollectionSetId());
			whereJoint.setCollection_id(this.getCollectionId());
			whereJoint.setTransfer_action_id(this.getTransferActionId());
			whereJoint.setPlugin_id(this.dbPlugin.getPlugin_id());
			this.joinedColumns = new Meta_jointsFactory(this.getRockFact(), whereJoint, orderByStr);

			this.pluginMethodNames = new ArrayList<String>();
			this.pluginMethodParams = new ArrayList<Object[]>();

			for (Meta_joints joint : this.getJoinedColumns().get()) {
				this.pluginMethodNames.add(joint.getPlugin_method_name());
				this.pluginMethodParams.add(stringToArray(joint.getMethod_parameter()));
			}

		} catch (ClassNotFoundException e) {
			throw new EngineMetaDataException("Exception in TransferPlugin !", new String[] { e.getMessage() }, e, this, this
					.getClass().getName());

		} catch (NoSuchMethodException e) {
			throw new EngineMetaDataException("Exception in TransferPlugin !", new String[] { e.getMessage() }, e, this, this
					.getClass().getName());

		} catch (InstantiationException e) {
			throw new EngineMetaDataException("Exception in TransferPlugin !", new String[] { e.getMessage() }, e, this, this
					.getClass().getName());

		} catch (IllegalAccessException e) {
			throw new EngineMetaDataException("Exception in TransferPlugin !", new String[] { e.getMessage() }, e, this, this
					.getClass().getName());

		} catch (InvocationTargetException e) {
			throw new EngineMetaDataException("Exception in TransferPlugin !", new String[] { e.getMessage() }, e, this, this
					.getClass().getName());
		} catch (SQLException e) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_READ_METADATA, new String[] { tableName }, e, this, this
					.getClass().getName());
		} catch (RockException e) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_READ_METADATA, new String[] { tableName }, e, this, this
					.getClass().getName());
		}
	}

	/**
	 * GET methods for member variables
	 */
	public final String getPluginName() {
		return this.dbPlugin.getPlugin_name();
	}

	public Class<?> getPluginClass() {
		return this.pluginClass;
	}

	public Object getPluginObject() {
		return this.pluginObject;
	}

	public List<String> getJoinedPluginMethodNames() {
		return this.pluginMethodNames;
	}

	public List<Object[]> getJoinedPluginMethodParams() {
		return this.pluginMethodParams;
	}

	public int getCommitAfterNRows() {
		if (this.dbPlugin.getCommit_after_n_rows() == null) {
			return 0;
		} else {
			return this.dbPlugin.getCommit_after_n_rows().intValue();
		}
	}

	public Meta_jointsFactory getJoinedColumns() {
		return this.joinedColumns;
	}

	private Object[] stringToArray(final String parameters) {

		StringTokenizer sToken = new StringTokenizer(parameters, ",", true);

		int numOfElems = 0;
		String prevToken = "";
		boolean first = true;
		while (sToken.hasMoreTokens()) {
			final String token = sToken.nextToken();
			if (!token.equals(",") || prevToken.equals(token) || first) {
				numOfElems++;
			}
			first = false;
			prevToken = token;
		}
		if (prevToken.equals(",")) {
			numOfElems++;
		}

		final Object[] retString = new Object[numOfElems];

		numOfElems = 0;
		prevToken = "";
		first = true;
		sToken = new StringTokenizer(parameters, ",", true);
		while (sToken.hasMoreTokens()) {
			final String token = sToken.nextToken();
			if (!token.equals(",") || prevToken.equals(token) || first) {
				if (token.equals(",")) {
					retString[numOfElems] = "";
				} else {
					retString[numOfElems] = token;
				}
				numOfElems++;
			}
			first = false;
			prevToken = token;
		}
		if (prevToken.equals(",")) {
			retString[numOfElems] = "";
			numOfElems++;
		}

		return retString;
	}

	protected Class<?>[] toClassArray(final String parameters) {

		StringTokenizer sToken = new StringTokenizer(parameters, ",", true);

		int numOfElems = 0;
		String prevToken = "";
		boolean first = true;
		while (sToken.hasMoreTokens()) {
			final String token = sToken.nextToken();
			if (!token.equals(",") || prevToken.equals(token) || first) {
				numOfElems++;
			}
			first = false;
			prevToken = token;
		}
		if (prevToken.equals(",")) {
			numOfElems++;
		}
		final Class<?>[] retString = new Class<?>[numOfElems];

		numOfElems = 0;
		prevToken = "";
		first = true;
		sToken = new StringTokenizer(parameters, ",", true);
		while (sToken.hasMoreTokens()) {
			final String token = sToken.nextToken();
			if (!token.equals(",") || prevToken.equals(token) || first) {

				retString[numOfElems] = token.getClass();
				numOfElems++;
			}
			first = false;
			prevToken = token;
		}
		if (prevToken.equals(",")) {
			retString[numOfElems] = prevToken.getClass();
			numOfElems++;
		}
		return retString;
	}
}
