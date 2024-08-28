package com.distocraft.dc5000.repository.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.MissingResourceException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.repository.dwhrep.Grouptypes;

public class GroupTypeDefTest {
	private final String tpName = "dc_e_abc";
	private final String testVersionId = tpName+":((123))";
	private static final String DEF_GP_TYPE = "TESTGRP";
  private static final int isNullable = 0;

  private final Grouptypes key1 = getKey(DEF_GP_TYPE, "KEY_B", "varchar", 64, 0, testVersionId, isNullable);

  private final Grouptypes key2 = getKey(DEF_GP_TYPE, "KEY_A", "varchar", 64, 0, testVersionId, isNullable);

  private final Grouptypes key3 = getKey(DEF_GP_TYPE, "KEY_Z", "varchar", 64, 0, testVersionId, isNullable);

  private final Grouptypes key4 = getKey(DEF_GP_TYPE, "MNC", "varchar", 64, 0, testVersionId, isNullable);

  private final Grouptypes key5 = getKey(DEF_GP_TYPE, "MCC", "varchar", 64, 0, testVersionId, isNullable);

  private final Grouptypes key6 = getKey(DEF_GP_TYPE, "AKEY", "varchar", 64, 0, testVersionId, isNullable);
	private final List<String> expecteKeyOrder = Arrays.asList("AKEY", "KEY_A", "KEY_B", "KEY_Z", "MCC", "MNC");
	private final String expectedTableName = "GROUP_TYPE_E_"+DEF_GP_TYPE;
	private GroupTypeDef def = null;

	@Before
	public void setUp() throws Exception {
		final List<Grouptypes> keys = Arrays.asList(key1, key2, key3, key4, key5, key6);
		def = new GroupTypeDef(keys, DEF_GP_TYPE, testVersionId);
	}
	@Test
	public void testGetInvalidKey(){
		try{
			def.getKey("boooooo");
			Assert.fail("Exception should have been thrown as dataname is incorrect");
		} catch (IndexOutOfBoundsException e){
			// ok, expected this...
		}
	}
	@Test
	public void testGetKey(){
		final String kName = "AKEY";
		final GroupTypeKeyDef k = def.getKey(kName);
		Assert.assertEquals(kName, k.getKeyName());
	}
  
	@Test
	public void testGetDataKeys(){
		final Collection<GroupTypeKeyDef> keys = def.getDataKeys();
		final List<String> names = new ArrayList<String>();
		for(GroupTypeKeyDef key : keys){
			Assert.assertTrue(key.isDataColumn());
			names.add(key.getKeyName());
		}
		Assert.assertFalse(names.contains(GroupTypeDef.KEY_NAME_GROUP_NAME));
		Assert.assertFalse(names.contains(GroupTypeDef.KEY_NAME_START_TIME));
		Assert.assertFalse(names.contains(GroupTypeDef.KEY_NAME_STOP_TIME));
	}
	@Test
	public void testDefCorrect(){
		final String eType = "EXPECTED_TYPE";
		final int eSize = 345;
		final int eScale = 34344444;
		final String eTpName = "EXPECTED_TP_NAME";
		final String eVersion = eTpName + ":((42))";
    final int eNullable = 1;
    final List<Grouptypes> keys = Arrays.asList(getKey(DEF_GP_TYPE, DEF_GP_TYPE, eType, eSize, eScale, eVersion,
        eNullable));
		final GroupTypeDef toCheck = new GroupTypeDef(keys, DEF_GP_TYPE, eVersion);
		Assert.assertEquals(eTpName, toCheck.getTechpackName());
		Assert.assertEquals(eVersion, toCheck.getVersionID());
		Assert.assertEquals("GROUP_TYPE_E_"+DEF_GP_TYPE, toCheck.getTableName());
		final String eTypeName = eTpName + "_GROUP_TYPE_E_" +DEF_GP_TYPE;
		Assert.assertEquals(eTypeName, toCheck.getTypename());
		Assert.assertEquals(eTypeName+":PLAIN", toCheck.getStorageId());
	}
	@Test
	public void testCreateDefVersionIdWrongFormat(){
    final List<Grouptypes> keys = Arrays.asList(getKey(DEF_GP_TYPE, "KEY_B", "varchar", 64, 0, "dddd", 0));
		try{
			new GroupTypeDef(keys, DEF_GP_TYPE, tpName);
			Assert.fail("Exception should have been thrown as VERSIONID is not the correct format");
		} catch (IllegalArgumentException e){
			// ok, expected this
		}
	}
	@Test
	public void testCreateDefNoVersionId(){
    final List<Grouptypes> keys = Arrays.asList(getKey(DEF_GP_TYPE, "KEY_B", "varchar", 64, 0, null, 0));
		try{
			new GroupTypeDef(keys, DEF_GP_TYPE, null);
			Assert.fail("Exectrion should have been thrown as VERSIONID is not specified in db info");
		} catch (MissingResourceException e){
			// ok, expected this
			Assert.assertEquals("VERSIONID", e.getKey());
		}
	}

	@Test
	public void testCreateDefNoTypename(){
    final List<Grouptypes> keys = Arrays.asList(getKey(null, "KEY_B", "varchar", 64, 0, null, 0));
		try{
			new GroupTypeDef(keys, null, testVersionId);
			Assert.fail("Exectrion should have been thrown as Grouptype is constructor");
		} catch (MissingResourceException e){
			Assert.assertEquals("Grouptype", e.getKey());
			// ok, expected this
		}
	}

  @Test
  public void testGetTempTableName(){
    final String tempTableName = def.getTempTableName();
    Assert.assertEquals("Table name not generated correctly ", "TMP_"+expectedTableName, tempTableName);
  }
	@Test
	public void testGetTableName(){
		final String generatedTableName = def.getTableName();
		Assert.assertEquals("Table name not generated correctly ", expectedTableName, generatedTableName);
	}
	@Test
	public void testGetTableName2(){
		final List<String> keyNames = Arrays.asList("B", "Z", "A", "X");
		final List<Grouptypes> keys = new ArrayList<Grouptypes>();
		for(String kName : keyNames){
      keys.add(getKey(DEF_GP_TYPE, kName, "varchar", 32, 0, testVersionId, 0));
		}
		final GroupTypeDef test = new GroupTypeDef(keys, DEF_GP_TYPE, testVersionId);
		final String generatedTableName = test.getTableName();
		final String eTableName = "GROUP_TYPE_E_" + DEF_GP_TYPE;
		Assert.assertEquals("Table name not generated correctly ", eTableName, generatedTableName);
	}
	@Test
	public void testGetTableName3(){
    final String groupType = "SOMEFREEGROUPNAME";
    final List<Grouptypes> keys = Arrays.asList(getKey(groupType, "WOOO", "varchar", 32, 0, testVersionId, 0));
    final GroupTypeDef test = new GroupTypeDef(keys, groupType, testVersionId);
		final String generatedTableName = test.getTableName();
    final String expectedName = "GROUP_TYPE_E_"+groupType;
		Assert.assertEquals("Table name not generated correctly ", expectedName, generatedTableName);
	}

	@Test
	public void testGetKeys(){
		final Collection<GroupTypeKeyDef> keys = def.getKeys();
		final List<String> generatedKeyOrder = new ArrayList<String>();
		for(GroupTypeKeyDef key : keys){
			if(key.isDataColumn()){
				generatedKeyOrder.add(key.getKeyName());
			}
		}
		Assert.assertEquals("Generated Key List not the correct size", expecteKeyOrder.size(), generatedKeyOrder.size());
		final Iterator<String> e = expecteKeyOrder.iterator();
		final Iterator<String> g = generatedKeyOrder.iterator();
		while( e.hasNext() && g.hasNext()){
			Assert.assertEquals("Key Order not correct", e.next(), g.next());
		}
	}

  private Grouptypes getKey(final String gpType, final String name, final String type, final int size, final int scale,
      final String vId, int nullable) {
		final Grouptypes key = new Grouptypes(null);
		key.setGrouptype(gpType);
		key.setDataname(name);
		key.setDatatype(type);
		key.setDatasize(size);
		key.setDatascale(scale);
		key.setVersionid(vId);
    key.setNullable(nullable);
		return key;
	}
}
