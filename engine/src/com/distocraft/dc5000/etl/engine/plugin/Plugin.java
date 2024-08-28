/*
 * @(#) $Header: /usr/cvsrepository/dc5000/src/etl/engine/java/com/distocraft/dc5000/etl/engine/plugin/Plugin.java,v 1.6 2005/12/05 11:23:31 lemminkainen Exp $
 * 
 * $Author: lemminkainen $ $Date: 2005/12/05 11:23:31 $
 * 
 * $Log: Plugin.java,v $
 * Revision 1.6  2005/12/05 11:23:31  lemminkainen
 * Rollback
 *
 * Revision 1.4  2005/04/12 04:53:59  savinen
 * *** empty log message ***
 *
 * Revision 1.3  2005/02/02 12:42:12  savinen
 * *** empty log message ***
 *
 * Revision 1.2  2005/02/02 08:48:19  savinen
 * *** empty log message ***
 *
 * Revision 1.1.1.1  2003/02/07 06:55:58  jakub
 * Updated UI and added new functionality for Sybase.
 *
 *
 */
package com.distocraft.dc5000.etl.engine.plugin;

import java.lang.reflect.Constructor;
import java.util.StringTokenizer;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_plugins;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * Represents a plugin without source or target.
 * 
 * Created on 15.1.2003
 * 
 * @author jakub
 */
public class Plugin extends TransferActionBase {

	/** the plugin to execute */
	private PluginClass m_plugin;

	/**
	 * Default constructor inits variables.
	 * 
	 * @param version
	 *          metadata version
	 * @param collectionSetId
	 *          collectionSetId (transfer group)
	 * @param collection
	 *          collection (transfer set)
	 * @param transferActionId
	 * @param connectionId
	 * @param rockFact
	 *          connection to the repository
	 * @param trActions
	 *          all the transfer actions of this transfer set
	 */
	public Plugin(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions, final PluginLoader pLoader) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		try {
			final Meta_plugins plugin = new Meta_plugins(rockFact, (Long) null, collectionSetId, collection.getCollection_id(),
			// version.getVersion_number(),
					collection.getVersion_number(), transferActionId);

			if (plugin.getPlugin_name() == null) {
				throw new EngineMetaDataException(EngineConstants.NO_PLUGIN_NAME, null, this, this.getClass().getName());
			}

			final Class<?> pluginClass = pLoader.loadClass(plugin.getPlugin_name());
			final Constructor<?> pluginConstr = pluginClass.getConstructor(toClassArray(plugin.getConstructor_parameter()));
			m_plugin = (PluginClass) pluginConstr.newInstance(stringToArray(plugin.getConstructor_parameter()));
		} catch (Exception e) {
			throw new EngineMetaDataException("Plugin init exception ! ," + e.getMessage() + " , " + e + "\n"
					+ " Possible cause - wrong plugin parameters.", new String[] { e.getMessage(), e.toString(), }, e, this, this
					.getClass().getName());
		}

	}

	/**
	 * Runs this plugin.
	 */
	public void execute() throws EngineException {
		try {
			if (m_plugin != null) {
				m_plugin.commit();
			}
		} catch (PluginException e) {
			throw new EngineException("Plugin Exception!", new String[] { "Plugin: " + m_plugin.getClass() }, e, this, this
					.getClass().getName(), EngineConstants.ERR_TYPE_SYSTEM);
		}

	}

	/**
	 * Helping method taken from TransferPlugin
	 * 
	 * @param parameters
	 * @return Class[]
	 */
	private Class<?>[] toClassArray(final String parameters) {

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
		Class<?>[] retString = new Class[numOfElems];

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

	/**
	 * Helping method taken from TransferPlugin
	 * 
	 * @param parameters
	 * @return Object[]
	 */
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

		Object[] retString = new Object[numOfElems];

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

}
