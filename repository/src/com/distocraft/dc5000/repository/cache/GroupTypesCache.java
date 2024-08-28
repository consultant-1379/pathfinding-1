package com.distocraft.dc5000.repository.cache;

import com.distocraft.dc5000.repository.dwhrep.Grouptypes;
import com.distocraft.dc5000.repository.dwhrep.GrouptypesFactory;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class GroupTypesCache {

	/**
	 * Cache containing all the GroupMgt definitions per versionid
	 * DC_E_ABC:((2)) --> TYPE_1 	--> KeyA
	 * 										 				--> KeyB
	 * 										 				--> KeyC
	 * 							      TYPE_2 	--> Key1
	 * 							      TYPE_3 	--> KeyY
	 * 							      TYPE_4 	--> KeyZ
	 * DC_E_ABC:((3)) --> TYPE_1	-->	KeyA
	 * 														--> KeyC
	 * 							      TYPE_2 	--> Key1
	 * 							      TYPE_3 	--> KeyX
	 * 							      TYPE_3 	--> KeyY
	 * 							      TYPE_4 	--> KeyZ
	 *
	 */
  private static Map<String, Map<String, GroupTypeDef>> groupsCache = new HashMap<String, Map<String, GroupTypeDef>>(2);
  private GroupTypesCache(){/**/}

	/**
	 * Check to see if there are any group types defined in the teck pack
	 * @param versionId The tech pack version
	 * @return TRUE is the tech pack has groups defined, FALSE otherwise.
	 */
	public static boolean areGroupsDefined(final String versionId){
		return groupsCache.containsKey(versionId);
	}

	/**
	 * Get the group types for a tech pack
	 * @param versionId The tech packs version
	 * @return Map of the Group type defs, if there are no group types defined for the
	 * versionid, an empty map is returned i.e. <code>areGroupsDefined(versionId)</code> == FALSE
	 */
	public static Map<String, GroupTypeDef> getGrouptypesDef(final String versionId) {
		if(!areGroupsDefined(versionId)){
			return new HashMap<String, GroupTypeDef>(0);
		}
		return groupsCache.get(versionId);
  }

	/**
	 * Checks to see if the DWH Type is a GroupType
	 * @param versionId Tee tech pack verision
	 * @param dwhTypename The group DWH type name
	 * @return TRUE is the group is defined in the tach pack, FALSE otherwise.
	 */
	public static boolean isGroupMgtDwhType(final String versionId, final String dwhTypename){
		if(areGroupsDefined(versionId)){
			for(GroupTypeDef def : groupsCache.get(versionId).values()){
				if(def.getTypename().equals(dwhTypename)){
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Check to see if the GroupType is valid within the techpack
	 * @param versionId the Tech Pack VersionID
	 * @param groupType the Group Type e.g. APN, IMSI or TAC
	 * @return TRUE is the group type is defined in the Tech Pack, FALSE otherwise.
	 */
	public static boolean isGroupMgtType(final String versionId, final String groupType){
		assert groupType != null;
		return areGroupsDefined(versionId) && groupsCache.get(versionId).keySet().contains(groupType.toUpperCase());
	}

	/**
	 * Initialize the cache or refresh it...
	 * @param dwhrep REPDB connection
	 * @throws RockException Errors
	 * @throws SQLException Errors
	 */
  public static void init(final RockFactory dwhrep) throws RockException, SQLException {
    final Grouptypes where = new Grouptypes(dwhrep);
    final GrouptypesFactory fac = new GrouptypesFactory(dwhrep, where);
    final List<Grouptypes> groupTypes = fac.get();
    groupsCache.clear();
		// sorted list from db......
		final Map<String/*versionId*/, Map<String, List<Grouptypes>>> tmp = new HashMap<String, Map<String, List<Grouptypes>>>();
		for (Grouptypes gt : groupTypes) {
			final Map<String, List<Grouptypes>> map;
			if(tmp.containsKey(gt.getVersionid())){
				map = tmp.get(gt.getVersionid());
			} else {
				map = new HashMap<String, List<Grouptypes>>();
				tmp.put(gt.getVersionid(), map);
			}
			final String mapKey = gt.getGrouptype();
			final List<Grouptypes> keyTypes;
			if(map.containsKey(mapKey)){
        keyTypes = map.get(mapKey);
      } else {
        keyTypes = new ArrayList<Grouptypes>();
        map.put(mapKey, keyTypes);
      }
      keyTypes.add(gt);
		}

		for(String versionId : tmp.keySet()){
			final Map<String, List<Grouptypes>> defList = tmp.get(versionId);
			for(String gpType : defList.keySet()){
				final List<Grouptypes> groupSetup = defList.get(gpType);
				final GroupTypeDef def = new GroupTypeDef(groupSetup, gpType, versionId);
				final String vId = def.getVersionID();
				final Map<String, GroupTypeDef> list;
				if(groupsCache.containsKey(vId)){
					list = groupsCache.get(vId);
				} else {
					list = new HashMap<String, GroupTypeDef>();
					groupsCache.put(vId, list);
				}
				list.put(def.getGroupType(), def);
			}
		}
  }
}
