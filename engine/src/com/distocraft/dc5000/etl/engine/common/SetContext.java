package com.distocraft.dc5000.etl.engine.common;

import java.util.HashMap;

/**
 * Class used to transfer objects between different actions of an ETL set.
 * Implementation is based on HashMap.<br>
 * Copyright Distocraft 2005<br>
 * 
 * @author lemminkainen
 */
public class SetContext extends HashMap<String, Object> {

	public SetContext() {
		super(5);
	}

	/**
	 * Fetches defined object from SetContext. If object is not found
	 * EngineMetaDataException is thrown. If exception is not desired get(String
	 * key) method of underlaying HashMap should be used.
	 */
	public Object getObjectFromSetContext(final String key) throws EngineMetaDataException {
		final Object object = get(key);

		if (object == null) {
			throw new EngineMetaDataException("Setcontext parameter \"" + key + "\" was not found. Exiting....",
					new Exception(), "getObjectFromSetContext"); // NOPMD
		} else {
			return object;
		}
	}

}
