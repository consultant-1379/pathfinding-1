package com.distocraft.dc5000.etl.engine.common;

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import org.junit.Test;

public class UtilTest {
  
  @Test
  public void testdateToMilli(){
	  
	  Method m;
	try {
		m = Util.class.getDeclaredMethod("dateToMilli", String.class);
	    m.setAccessible(true);
	    Long actual = (Long) m.invoke(new Util(), "2011:01:31");
	    assertNotNull(actual);
	}
	catch(Exception e){
		System.out.println(e);
	}
 
    
  }
  
}
