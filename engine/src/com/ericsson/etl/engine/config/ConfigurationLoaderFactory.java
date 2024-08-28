package com.ericsson.etl.engine.config;

import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

public final class ConfigurationLoaderFactory {

	private static ConfigurationLoader _mocked_instance;
	
	private ConfigurationLoaderFactory() {}
	
	/**
	 * Create a mocked instance of ConfigurationLoader. Use to override default class
	 * for test purposes.
	 * 
	 * Use setMockedInstance(null) to clear existing value and revert to normal instance 
	 * creation
	 * 
	 * @param configurationLoader
	 */
	public static void setMockedInstance(final ConfigurationLoader configurationLoader) {
		_mocked_instance = configurationLoader;
	}

	/**
	 * Get an instance of ConfigurationLoader. If _mocked_instance exists, it will be returned.
	 * If no _mocked_instance exists, a normal instance of ConfigurationLoader will be created 
	 * so long as rockFact and parentLog are set.
	 * If either rockFact or parentLog are null, an IllegalArgumentException will be thrown. 
	 * 
	 * @return an instance of ConfigurationLoader or null
	 */
	public static ConfigurationLoader getInstance(final RockFactory rockFact, final Logger parentLog) {
		
		ConfigurationLoader configLoader = null;
		
		if (_mocked_instance != null) {
			configLoader = _mocked_instance;
		} else {
			if (parentLog == null) {
				throw new IllegalArgumentException("ConfigurationLoaderFactory: Attempt to get instance with parentLog==null.");
			} else if (rockFact == null) {
				throw new IllegalArgumentException("ConfigurationLoaderFactory: Attempt to get instance with rockFact==null.");
			} else {
				configLoader = new ConfigurationLoader(rockFact, parentLog);
			}
		}
		return configLoader;
	}
}


