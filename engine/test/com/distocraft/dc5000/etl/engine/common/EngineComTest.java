package com.distocraft.dc5000.etl.engine.common;

import static org.junit.Assert.*;
import org.junit.Test;

public class EngineComTest {
  
  @Test
  public void testSetAndGetCommand(){
    EngineCom ec = new EngineCom();
    ec.setCommand("command"); 
    assertEquals("command", ec.getCommand());
  }
  
}
