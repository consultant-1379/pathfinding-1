package com.distocraft.dc5000.etl.engine.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

public class PluginLoaderTest {
 
  @Test
  public void testgetPluginMethodNames() throws Exception{
	PluginLoader pl = new PluginLoader("com.distocraft.dc5000.etl.engine.plugin.PluginLoader");
	String[] st = pl.getPluginMethodNames("com.distocraft.dc5000.etl.engine.plugin.PluginLoader", true, true);
    assertNotNull(st);
    }
  
  @Test
  public void testgetPluginMethodParameters() throws Exception{
	PluginLoader pl = new PluginLoader("com.distocraft.dc5000.etl.engine.plugin.PluginLoader");
	String st = pl.getPluginMethodParameters("com.distocraft.dc5000.etl.engine.plugin.PluginLoader", "getPluginMethodNames");
    assertEquals("String,boolean,boolean", st);
    }
  
  @Test
  public void testgetPluginConstructorParameters() throws Exception{
	PluginLoader pl = new PluginLoader("com.distocraft.dc5000.etl.engine.plugin.PluginLoader");
	String st = pl.getPluginConstructorParameters("com.distocraft.dc5000.etl.engine.plugin.PluginLoader");
    assertEquals("String", st);
    }

}

