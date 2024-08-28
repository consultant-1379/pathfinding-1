package com.distocraft.dc5000.etl.engine.plugin;

import junit.framework.JUnit4TestAdapter;
import static org.junit.Assert.*;
import org.junit.Test;

public class PluginExceptionTest {

  @Test
  public void testConstructor(){
    PluginException pe = new PluginException("Message");
    assertEquals("Message", pe.getMessage());
  }
  
  @Test
  public void testConstructor2(){
    PluginException pe = new PluginException(new Throwable("Message2"));
    assertEquals("Message2", pe.getMessage());
  }
  
  /*public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PluginExceptionTest.class);
  }*/
}
