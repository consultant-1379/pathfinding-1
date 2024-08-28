package com.distocraft.dc5000.etl.engine.plugin;

/**
 * Interface for Dagger plugins
 * each plugin must implement this interface in order to work.
 */
public interface PluginClass {

	/**
	 * commit Commits the set method invocations
	 */
	void commit() throws PluginException;

}
