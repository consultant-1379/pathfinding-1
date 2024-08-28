package com.distocraft.dc5000.etl.engine.plugin;

/**
 * Interface for Dagger plugins
 * each plugin must implement this interface in order to work.
 */
public interface PluginReadClass {

	/**
	 * Next Sets the next readable element, when no more elements exist returns
	 * false
	 * 
	 * @return boolean True if a new element exists otherwise false.
	 */
	boolean next();

	/**
	 * Test if there are more elements to iterate
	 * 
	 * @return boolean true if more elements exist
	 */
	boolean hasNext();

}