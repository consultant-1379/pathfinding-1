package com.distocraft.dc5000.etl.engine.common;

import java.util.Properties;

/**
 * TODO intro <br>
 * TODO usage <br>
 * TODO used databases/tables <br>
 * TODO used properties <br>
 * <br>
 * Copyright Distocraft 2005<br>
 * <br>
 * $id$
 * 
 * @author lemminkainen
 */
public class ExceptionalProperties extends Properties {

	public String getProperty(final String name) {

		final String val = getProperty(name, null);

		if (val != null) {
			return val;
		} else {
			throw new NullPointerException("Property " + name + " is undefined"); //NOPMD
		}

	}

}
