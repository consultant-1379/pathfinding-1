package com.distocraft.dc5000.etl.engine.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class ShareTest {

  public static final String TEST_APPLICATION = ShareTest.class.getName();

  public List<Object> list = new ArrayList<Object>();;
  
  Object obj = new Object();
  
  Share sh = Share.instance();
  
  @Test
  public void testSizeandRemove() throws Exception {
	  assertEquals(0, sh.size());
	  sh.add("KeeY", obj);
	  assertEquals(1, sh.size());
	  sh.remove("KeeY");
	  boolean bo1 = sh.contains("KeeY");
	  assertFalse(bo1);
	}
  
  @Test
  public void testAddandGet() throws Exception {
	  
	  sh.add("KeY", obj);
	  Object o = sh.get("KeY");
	  assertNotNull(o);
	  Object o1 = sh.get("Key");
	  assertNull(o1);
	  }
  
  @Test
  public void testAddandContains() throws Exception {
	  
	  sh.add("KeeY", obj);
	  boolean bo = sh.contains("KeeY");
	  assertTrue(bo);
	  
	  }

}