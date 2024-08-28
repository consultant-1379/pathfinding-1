package com.distocraft.dc5000.repository.cache;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class GroupTypeKeyDefTest {
	private final String name = "aKey";
	private final String keyType = "aKey";
	private final int keySize = 32;
	private final int keyScale = 0;
	private GroupTypeKeyDef def = null;

	@Before
	public void setUp() throws Exception {
		def = new GroupTypeKeyDef(name, keyType, keySize, keyScale, 1, false);
	}
	@Test
	public void testCreate(){
		//code coverage......
		new GroupTypeKeyDef(name, keyType, keySize, keyScale, 1, false);
	}
	@Test
	public void testGetKeyName() throws Exception {
		Assert.assertEquals("Name not set/reported correctly ", name, def.getKeyName());
	}
	@Test
	public void testGetKeyType() throws Exception {
		Assert.assertEquals("Type not set/reported correctly ", keyType, def.getKeyType());
	}
	@Test
	public void testGetKeySize() throws Exception {
		Assert.assertEquals("Size not set/reported correctly ", keySize, def.getKeySize());
	}
	@Test
	public void testGetKeyScale() throws Exception {
		Assert.assertEquals("Scale not set/reported correctly ", keyScale, def.getKeyScale());
	}
	@Test
	public void testGetKeyUniqueValue() throws Exception {
		Assert.assertEquals("UniqueValue not set/reported correctly ", 255, def.getKeyUniqueValue());
	}
	@Test
	public void testGetKeyIndexType() throws Exception {
		Assert.assertEquals("IndexType not set/reported correctly ", "HG", def.getKeyIndexType());
	}
	@Test
	public void testGetKeyUniqueKey() throws Exception {
		Assert.assertEquals("UniqueKey not set/reported correctly ", 255, def.getKeyUniqueKey());
	}
	@Test
	public void testIsKeyNullable1() throws Exception {
		Assert.assertEquals("Nullable not set/reported correctly ", 1, def.isKeyNullable());
	}
	@Test
	public void testIsKeyNullable2() throws Exception {
		final GroupTypeKeyDef test = new GroupTypeKeyDef(name, keyType, keySize, keyScale, 0, false);
		Assert.assertEquals("Nullable not set/reported correctly ", 0, test.isKeyNullable());
	}
}
