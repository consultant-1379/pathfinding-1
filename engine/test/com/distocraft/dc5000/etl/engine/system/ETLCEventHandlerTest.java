package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.Vector;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;
import org.junit.BeforeClass;

/**
 * 
 * @author ejarsok
 *
 */

public class ETLCEventHandlerTest {

  private class Listener implements ETLCEventListener {

    private String s;
    
    public void triggerEvent(String key) {
      s = key;
    }
    
    public String getS() {
      return s;
    }
  };
  
  private static ETLCEventHandler eht;
  
  private static Listener li;
  
  private static Vector vec;
  
  @BeforeClass
  public static void init() {
    ETLCEventHandlerTest EHT = new ETLCEventHandlerTest();
    li = EHT.new Listener();
    eht = new ETLCEventHandler("key");
    Class secretClass = eht.getClass();
    Field field;
    
    try {
      field = secretClass.getDeclaredField("listeners");
      field.setAccessible(true);  
      vec = (Vector) field.get(eht);
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @Test
  public void testAddListener() {  
      eht.addListener(li);
      assertEquals(true, vec.contains(li));
  }

  @Test
  public void testTriggerListeners() {  
    eht.triggerListeners("string_key");
    assertEquals("string_key", li.getS());
  }
  
  @Test
  public void testRemoveListener() {  
    eht.removeListener(li);
    assertEquals(false, vec.contains(li));
  }
  
  /*public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ETLCEventHandlerTest.class);
  }*/
}
