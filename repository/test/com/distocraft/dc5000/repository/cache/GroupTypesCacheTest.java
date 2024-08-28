package com.distocraft.dc5000.repository.cache;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;

import java.util.Map;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;
import java.util.Collection;

public class GroupTypesCacheTest {
	private final String tpName = "EVENT_E_SGEH";
	private final String versionId = tpName + ":((42))";
	private final List<String> defaultKeyNames = Arrays.asList("GROUP_NAME", "START_TIME", "STOP_TIME");
	private final String testType = "TAC";
	private final String testKey = "TAC";
	@BeforeClass
	public static void dbSetup() throws Exception {
		final RockFactory dwhrep = DatabaseTestUtils.getTestDbConnection();
    DatabaseTestUtils.loadSetup(dwhrep, "gpmgtCache");
    try{
		  GroupTypesCache.init(dwhrep);
    } finally {
      DatabaseTestUtils.close(dwhrep);
    }
	}
	@Test
	public void testCacheGetTypesNotExisting(){
		final Map<String, GroupTypeDef> def = GroupTypesCache.getGrouptypesDef(versionId+"jjjjjj");
		Assert.assertTrue(def.isEmpty());
	}
	@Test
	public void testIsNotGroupDwhMgtType1(){
		final String typeName = tpName + "_GROUP_TYPE_E_" +testType;
		final boolean actual = GroupTypesCache.isGroupMgtDwhType(versionId+"df", typeName);
		Assert.assertFalse(actual);
	}
	@Test
	public void testIsNotGroupDwhMgtType2(){
		final String typeName = tpName + "_EVENTS_E_SGEH_" +testType;
		final boolean actual = GroupTypesCache.isGroupMgtDwhType(versionId, typeName);
		Assert.assertFalse(actual);
	}
	@Test
	public void testIsGroupDwhMgtType(){
		final String typeName = tpName + "_GROUP_TYPE_E_" +testType;
		final boolean actual = GroupTypesCache.isGroupMgtDwhType(versionId, typeName);
		Assert.assertTrue(typeName + " was expected to be true", actual);
	}
	@Test
	public void testIsGroupMgtType1(){
		final boolean actual = GroupTypesCache.isGroupMgtType(versionId, testType);
		Assert.assertTrue(actual);
	}

	@Test
	public void testIsGroupMgtType2(){
		final boolean actual = GroupTypesCache.isGroupMgtType(versionId, "foo");
		Assert.assertFalse(actual);
	}

	@Test
	public void testIsGroupMgtType3(){
		final boolean actual = GroupTypesCache.isGroupMgtType("this_doesnt_exist", testType);
		Assert.assertFalse(actual);
	}

	@Test
	public void testVersionId(){
		final  Map<String, GroupTypeDef> types = GroupTypesCache.getGrouptypesDef(versionId);
		for(String t : types.keySet()){
			final GroupTypeDef g = types.get(t);
			Assert.assertEquals(versionId, g.getVersionID());
		}
	}
	@Test
	public void testValidKeyCheck(){
		final GroupTypeDef tType = GroupTypesCache.getGrouptypesDef(versionId).get(testType);
		Assert.assertEquals(4, tType.getKeys().size());
		Assert.assertTrue(tType.isValidKey(testKey));
		Assert.assertFalse(tType.isValidKey("booo"));
	}
	@Test
	public void testGetGroupTypes(){
		final Map<String, GroupTypeDef> types = GroupTypesCache.getGrouptypesDef(versionId);
		Assert.assertEquals("Only expecting 4 GroupTypes", 4, types.size());
		final Iterator<String> iter = types.keySet().iterator();
		final List<String> eTypes = Arrays.asList("APN", "IMSI", "MCC_MNC", testType);
		while(iter.hasNext()){
            final String eType = iter.next();
			Assert.assertTrue("Unexpected GroupType " + eType, eTypes.contains(eType));
		}
	}
	@Test
	public void testCachedDefaultKeyDefs(){
		final Map<String, GroupTypeDef> types = GroupTypesCache.getGrouptypesDef(versionId);
		for(String type : types.keySet()){
			final GroupTypeDef typeDef = types.get(type);
			final Collection<GroupTypeKeyDef> c = typeDef.getKeys();

			for(GroupTypeKeyDef key : c){
				if(!key.isDataColumn()){
					Assert.assertTrue("Expected key not found", defaultKeyNames.contains(key.getKeyName()));
				}
			}
		}
	}
}
