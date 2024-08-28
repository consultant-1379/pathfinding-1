package com.distocraft.dc5000.etl.engine.system;

import junit.framework.JUnit4TestAdapter;
import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;

/**
 * 
 * @author ejarsok
 *
 */

public class SetListenerTest {

  private SetListener sl = new SetListener();
  
  @Test
  public void testGetStatus() {
    SetListener sl = new SetListener();
    assertEquals("", sl.getStatus());
  }
  
  @Test
  public void testDropped() {
    sl.dropped();
    assertEquals(SetListener.DROPPED, sl.getStatus());
  }
  
  @Test
  public void testFailed() {
    sl.failed();
    assertEquals(SetListener.FAILED, sl.getStatus());
  }
  
  @Test
  public void testSucceeded() {
    sl.succeeded();
    assertEquals(SetListener.SUCCEEDED, sl.getStatus());
  }
  
  /*public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(SetListenerTest.class);
  }*/
}
