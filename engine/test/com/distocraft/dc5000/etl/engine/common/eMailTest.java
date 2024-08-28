package com.distocraft.dc5000.etl.engine.common;

import static org.junit.Assert.*;
import java.lang.reflect.Field;
import java.util.Vector;
import junit.framework.JUnit4TestAdapter;
import org.junit.Test;
import org.junit.BeforeClass;

public class eMailTest {

  private static Field fields[];
  private static EMail eMailInstance;
  
  @BeforeClass
  public static void setup(){
    eMailInstance = new EMail();
    Class secretClass = eMailInstance.getClass();
    fields = secretClass.getDeclaredFields();
  }
  
  @Test
  public void testEmail(){
    eMailInstance.setMailServer("mailServer");
    eMailInstance.setPort(313);
    eMailInstance.setDomain("domain.com");
    eMailInstance.setSenderName("Sender");
    eMailInstance.setSenderAddress("sender@address");
    eMailInstance.setRecipients(new Vector());
    eMailInstance.setSubject("Subject");
    eMailInstance.setMessage("Message is set");
    
    for (int i = 0; i < fields.length; i++){
      fields[i].setAccessible(true); 
      try {
        
        if("myMailServer" == fields[i].getName())
          assertEquals("mailServer", fields[i].get(eMailInstance));
        if("myPort" == fields[i].getName())
          assertEquals(313, fields[i].get(eMailInstance));
        if("myDomain" == fields[i].getName())
          assertEquals("domain.com", fields[i].get(eMailInstance));
        if("mySenderName" == fields[i].getName())
          assertEquals("Sender", fields[i].get(eMailInstance));
        if("mySenderAddress" == fields[i].getName())
          assertEquals("sender@address", fields[i].get(eMailInstance));
        //if("myRecipients" == fields[i].getName())
          //assertEquals("mailServer", fields[i].get(eMailInstance));
        if("mySubject" == fields[i].getName())
          assertEquals("Subject", fields[i].get(eMailInstance));
        if("myMessage" == fields[i].getName())
          assertEquals("Message is set", fields[i].get(eMailInstance));

      } catch (IllegalArgumentException e) {
        e.printStackTrace();
        fail("get() failed. IllegalArgumentException");
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        fail("get() failed. IllegalAccessException");
      } 
   }
  }
  
  @Test
  public void testEmailConstructor(){
    eMailInstance = new EMail("server", 100, "do.main", "senderName", "senderAddress", "recipient", "sub", "mes");
    
    for (int i = 0; i < fields.length; i++){
      fields[i].setAccessible(true); 
      try {
        
        if("myMailServer" == fields[i].getName())
          assertEquals("server", fields[i].get(eMailInstance));
        if("myPort" == fields[i].getName())
          assertEquals(100, fields[i].get(eMailInstance));
        if("myDomain" == fields[i].getName())
          assertEquals("do.main", fields[i].get(eMailInstance));
        if("mySenderName" == fields[i].getName())
          assertEquals("senderName", fields[i].get(eMailInstance));
        if("mySenderAddress" == fields[i].getName())
          assertEquals("senderAddress", fields[i].get(eMailInstance));
        //if("myRecipients" == fields[i].getName())
          //assertEquals("mailServer", fields[i].get(eMailInstance));
        if("mySubject" == fields[i].getName())
          assertEquals("sub", fields[i].get(eMailInstance));
        if("myMessage" == fields[i].getName())
          assertEquals("mes", fields[i].get(eMailInstance));

      } catch (IllegalArgumentException e) {
        e.printStackTrace();
        fail("get() failed. IllegalArgumentException");
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        fail("get() failed. IllegalAccessException");
      } 
   }
  }
  
  @Test
  public void testEmailConstructor2(){
    Vector v = new Vector();
    eMailInstance = new EMail("server", 100, "do.main", "senderName", "senderAddress", v, "sub", "mes");

    for (int i = 0; i < fields.length; i++){
      fields[i].setAccessible(true); 
      try {
        
        if("myMailServer" == fields[i].getName())
          assertEquals("server", fields[i].get(eMailInstance));
        if("myPort" == fields[i].getName())
          assertEquals(100, fields[i].get(eMailInstance));
        if("myDomain" == fields[i].getName())
          assertEquals("do.main", fields[i].get(eMailInstance));
        if("mySenderName" == fields[i].getName())
          assertEquals("senderName", fields[i].get(eMailInstance));
        if("mySenderAddress" == fields[i].getName())
          assertEquals("senderAddress", fields[i].get(eMailInstance));
        //if("myRecipients" == fields[i].getName())
          //assertEquals("mailServer", fields[i].get(eMailInstance));
        if("mySubject" == fields[i].getName())
          assertEquals("sub", fields[i].get(eMailInstance));
        if("myMessage" == fields[i].getName())
          assertEquals("mes", fields[i].get(eMailInstance));

      } catch (IllegalArgumentException e) {
        e.printStackTrace();
        fail("get() failed. IllegalArgumentException");
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        fail("get() failed. IllegalAccessException");
      } 
   }
  }
  
  /*public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(eMailTest.class);
  }*/
}
