package com.distocraft.dc5000.repository.cache;

import com.distocraft.dc5000.repository.dwhrep.Grouptypes;

public final class GroupTypeKeyDef {
	/**
	 * The key name i.e. dataname
	 */
	private final String keyName;
	/**
	 * The key type.
	 */
	private final String keyType;
	/**
	 * The key data size
	 */
	private final int keySize;
	/**
	 * The keys data scale
	 */
	private final int keyScale;
	/**
	 * Indicates if the key is a data column or not
	 */
	private final boolean isDataColumn;
	/**
	 * Indicates wheather or not the value can be null
	 */
	private final int isNullable;
	/**
	 * Default unique value count
	 */
	private static final int DEFAULT_UNIQUE_VALUE = 255;
	/**
	 * Default unique key count
	 */
	private static final int DEFAULT_UNIQUE_KEY = 255;
	/**
	 * Default index type
	 */
	private static final String DEFAULT_INDEX_TYPE = "HG";

	/**
	 * Constructor, uses in tests.
	 * @param name The key name
	 * @param type The data type of the key
	 * @param size The size of the data type
	 * @param scale The scale of the data type
	 * @param nullable Indicated wheather or not the value can be null
	 * @param dataKey TRUE for data key, FALSE othersize
	 */
	GroupTypeKeyDef(final String name, final String type, final int size, final int scale,
									final int nullable, final boolean dataKey){
		keyName = name;
		keyType = type;
		keySize = size;
		keyScale = scale;
		isNullable = nullable;
		isDataColumn = dataKey;
	}

	/**
	 * Constructor for key def, isDataColumn is set to TRUE and isNullable is set to 0 (false)
	 * Wraps the dwhrep.Grouptypes object
	 * @param key The Grouptypes read from dwhrep
	 */
	public GroupTypeKeyDef(final Grouptypes key){
		this(key.getDataname(), key.getDatatype(), key.getDatasize(), key.getDatascale(), key.getNullable(), true);
	}

	/**
	 * Get the keys name
	 * @return The key name.
	 */
	public String getKeyName() {
		return keyName;
	}

	/**
	 * The the data type of the key
	 * @return The data type
	 */
	public String getKeyType() {
		return keyType;
	}

	/**
	 * Get the keys data type
	 * @return The data type of the key
	 */
	public int getKeySize() {
		return keySize;
	}

	/**
	 * Get the keys data scale.
	 * @return The data types data scale.
	 */
	public int getKeyScale() {
		return keyScale;
	}
	/**
	 * The the unique value count
	 * @return <code>DEFAULT_UNIQUE_VALUE</code>
	 */
	public long getKeyUniqueValue(){
		return DEFAULT_UNIQUE_VALUE;
	}

	/**
	 * The the unique key count
	 * @return <code>DEFAULT_UNIQUE_KEY</code>
	 */
	public int getKeyUniqueKey(){
		return DEFAULT_UNIQUE_KEY;
	}

	/**
	 * Indicates of the key value can be null
	 * @return 0 for non-null values only, 1 for null or non-null values
	 */
	public int isKeyNullable(){
		return isNullable;
	}

	/**
	 * Is the key a data column or not
	 * @return <code>TRUE</code>is the key was derived from a MeasurementKey or MeasurementCounter, <code>FALSE</code> otherwise  
	 */
	public boolean isDataColumn() {
		return isDataColumn;
	}

	/**
	 * Get the index type for the key
	 * @return The inxed type, defaults to <code>DEFAULT_INDEX_TYPE</code>
	 */
	public String getKeyIndexType(){
		return DEFAULT_INDEX_TYPE;
	}
}
