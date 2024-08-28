package com.distocraft.dc5000.etl.engine.plugin;

/**
 * Interface for Dagger plugins
 * each plugin must implement this interface in order to work.
 */
public interface PluginWriteClass {

	/**
	 * commit Commits the set method invocations
	 */
	void commit() throws PluginException;

	/**
	 * addRow Adds a row to the XML -document
	 */
	void addRow() throws PluginException;

}