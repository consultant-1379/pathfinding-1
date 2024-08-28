package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.distocraft.dc5000.etl.engine.common.EngineException;

/**
 * 
 * @author ejarsok
 *
 */

public class JVMMonitorActionTest {

  // useless
  @Test
  public void testExecute() {
    JVMMonitorAction ma = new JVMMonitorAction();
    try {
      ma.execute();
    } catch (EngineException e) {
      e.printStackTrace();
      fail("testExecute() failed, EngineException");
    }
  }

  /*public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(JVMMonitorActionTest.class);
  }*/
}
