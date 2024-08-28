package com.distocraft.dc5000.repository.cache;

import com.distocraft.dc5000.repository.dwhrep.Grouptypes;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.MissingResourceException;

public final class GroupTypeDef {
	/**
	 * Group Management dwhdb table name prefix.
	 */
	private static final String GROUP_TABLE_PREFIX = "GROUP_TYPE_E";
	/**
	 * Map of the Groups keys definitions
	 * groupKeys.key is the dataname
	 * groupKeys.value is the GroupTypeKeyDef for the dataname
	 */
	private final Map<String, GroupTypeKeyDef> groupKeys;
	/**
	 * The GroupType this object ismodeling e.g. APN or TAC
	 */
	private final String groupType;
	/**
	 * The version of this Grouptype
	 */
	private final String versionID;
	/**
	 * The Tech Pack this Grouptype belongs to, generated from <code>versionID</code>
	 */
	private final String techPackName;
	/**
	 * Storage Type Constant
	 */
	private static final String STORAGE_TYPE_PLAIN = ":PLAIN";
	/**
	 * Import tamp table prefix
	 */
	private static final String TEMP_TABLE_PREFIX = "TMP_"; // hsql doesnt like #
	/**
	 * Group name Key
	 */
	public static final String KEY_NAME_GROUP_NAME = "GROUP_NAME";
	/**
	 * Start Time key
	 */
	public static final String KEY_NAME_START_TIME = "START_TIME";
	/**
	 * Stop time Key
	 */
	public static final String KEY_NAME_STOP_TIME = "STOP_TIME";

	/**
	 * Constructor for the Grouptype, uses the Grouptypes rock objects.
	 * @param groupSetup The list of dwhrep.Grouptypes that make up the keys in the group.
	 * @param gpType The Group type i.e. APN, TAC or IMSI.
	 * @param versionId The Tech Pack version ID this GRouptype belongs too.
	 */
  public GroupTypeDef(final List<Grouptypes> groupSetup, final String gpType, final String versionId){
		groupKeys = new HashMap<String, GroupTypeKeyDef>();
		addDefaultKeys();
		for(Grouptypes key : groupSetup){
			final String kName = key.getDataname();
			groupKeys.put(kName, new GroupTypeKeyDef(key));
		}
		groupType = gpType;
		if(null == groupType){
			throw new MissingResourceException("No GroupType element found in Grouptypes", "Grouptypes", "Grouptype");
		}
		versionID = versionId;
		if(null == versionID){
			throw new MissingResourceException("No VERSIONID element found in Grouptypes", "Grouptypes", "VERSIONID");
		}
		if(-1 == versionID.indexOf(':')){
			throw new IllegalArgumentException("VersionID not in correct format");
		}
		techPackName = versionID.substring(0, versionID.indexOf(':'));
  }

	/**
	 * Default keys in the Grouptype
	 *
	 * //TODO This could be moved into the dwhrep.Grouptypes table, table would need one more column: DATACOLUMN
	 * //TODO to indicate if the key is from the measurement type or not (needed for the table name generation)
	 */
	private void addDefaultKeys(){
		groupKeys.put(KEY_NAME_GROUP_NAME, new GroupTypeKeyDef(KEY_NAME_GROUP_NAME, "varchar", 64, 0, 0, false));
		groupKeys.put(KEY_NAME_START_TIME, new GroupTypeKeyDef(KEY_NAME_START_TIME, "datetime", 0, 0, 1, false));
		groupKeys.put(KEY_NAME_STOP_TIME, new GroupTypeKeyDef(KEY_NAME_STOP_TIME, "datetime", 0, 0, 1, false));
	}

	/**
	 * Get the Tech Pack this Grouptype belongs too.
	 * @return Tech Pack name
	 */
	public String getTechpackName(){
		return techPackName.toUpperCase();
	}

	/**
	 * Get the version of Tech Pack this Grouptype belongs too.
	 * @return The VersionID
	 */
	public String getVersionID(){
		return versionID.toUpperCase();
	}

	/**
	 * Get the typename for this Grouptype e.g. EVENTS_E_SGEH_GROUP_TYPE_E_APN
	 * @return the type name to use for this grouptypes partition
	 */
	public String getTypename(){
		return getTechpackName() + "_" + GROUP_TABLE_PREFIX + "_" +getGroupType();
	}

	/**
	 * Get the StorageID to use for this Grouptype
	 * @return <code>getTypename()+":PLAIN"</code>
	 */
	public String getStorageId(){
		return getTypename() + STORAGE_TYPE_PLAIN;
	}

	/**
	 * Get the key definition from the Grouptype
	 * @param keyDataname The key name
	 * @return The GroupTypeKeyDef wrapper for the key.
	 * @throws IndexOutOfBoundsException is the key does not exist.
	 */
	public GroupTypeKeyDef getKey(final String keyDataname){
		if(isValidKey(keyDataname)){
			return groupKeys.get(keyDataname);
		}
		throw new IndexOutOfBoundsException("No Dataname called "+keyDataname+" found.");
	}

	/**
	 * Get the list of keys that make up this Groutype.
	 * @return List of GroupTypeKeyDef belonging to this Grouptype
	 */
	public Collection<GroupTypeKeyDef> getKeys(){
		final List<GroupTypeKeyDef> sorted = new ArrayList<GroupTypeKeyDef>(groupKeys.values());
		// Doing this so that the keys are at the end of the table i.e. GroupName, starttime, stoptime are first
		// then the keys that make up the actual data filters...
		Collections.sort(sorted, keyComparator);
		return sorted;
	}
	/**
	 * Get an order list of the data keys in the Group type i.e. where isDataColumn == TRUE
	 * @return Ordered list of data keys
	 */
	public Collection<GroupTypeKeyDef> getDataKeys(){
		final Collection<GroupTypeKeyDef> dataKeys = new ArrayList<GroupTypeKeyDef>();
		for(GroupTypeKeyDef key : getKeys()){
			if(key.isDataColumn()){
				dataKeys.add(key);
			}
		}
		return dataKeys;
	}

	/**
	 * Check to ss if the key name is part of this Grouptype.
	 * @param toCheck The ket name to check e.g. APN
	 * @return TRUE is the key is part of this Grouptype, FALSE otherwise.
	 */
  public boolean isValidKey(final String toCheck){
    return groupKeys.containsKey(toCheck.toUpperCase());
  }

	/**
	 * Get the Grouptype name e.g. IMSI
	 * @return The Grouptype name
	 */
	public String getGroupType() {
		return groupType.toUpperCase();
	}

	/**
	 * Get the table name to use in dwhdb for the Grouptype
	 * @return The name of the dwhdb table that contains the Groups data i.e. that imported via the xml
	 */
	public String getTableName(){
		final StringBuilder tableName = new StringBuilder(GROUP_TABLE_PREFIX);
    tableName.append("_").append(getGroupType());
		return tableName.toString();
	}

	/**
	 * Get the name of the temporary table the import uses.
	 * @return Temp table name
	 */
	public String getTempTableName(){
		return TEMP_TABLE_PREFIX + getTableName();
	}
	private final Comparator<GroupTypeKeyDef> keyComparator = new Comparator<GroupTypeKeyDef>(){
		@Override
		public int compare(final GroupTypeKeyDef o1, final GroupTypeKeyDef o2) {
			if(o1.isDataColumn() == o2.isDataColumn()){
				return o1.getKeyName().compareTo(o2.getKeyName());
			}
			return Boolean.valueOf(o1.isDataColumn()).compareTo(o2.isDataColumn());
		}
	};
}
