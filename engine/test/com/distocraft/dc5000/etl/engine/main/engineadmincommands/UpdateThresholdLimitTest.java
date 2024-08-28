package com.distocraft.dc5000.etl.engine.main.engineadmincommands;


import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Properties;

import junit.framework.JUnit4TestAdapter;

import org.hamcrest.Description;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.BeforeClass;
import org.junit.Test;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 *
 * @author eneacon
 *
 */


public class UpdateThresholdLimitTest {


	protected Mockery context = new JUnit4Mockery();
	  {
	    // we need to mock classes, not just interfaces.
	    context.setImposteriser(ClassImposteriser.INSTANCE);
	  }


	
	  private static Properties prop;

	  private static String homeDir;
	  
	  private EngineAdmin engineAdmin;
	
	
	  @BeforeClass
	  public static void setup() throws Exception {

	    Map<String, String> env = System.getenv();

	   homeDir = env.get("WORKSPACE");

	    File sp = new File(homeDir, "static.properties");
	    sp.deleteOnExit();
	    try {
	      PrintWriter pw = new PrintWriter(new FileWriter(sp));
	      pw.print("foo=bar");
	      pw.close();
	    } catch (Exception e) {
	      e.printStackTrace();
	      fail("Can´t write in file!");
	    }

	    prop = new Properties();
	    prop.setProperty("property1", "value1");
	    StaticProperties.giveProperties(null);
	    System.clearProperty("dc5000.config.directory");
	  }
	

  @Test
  public void testSetProperty() throws Exception {

    engineAdmin = context.mock(EngineAdmin.class);

    context.checking(new Expectations() {
      {
        oneOf(engineAdmin).updateThresholdProperty(with(any(Integer.class)));
        will(setProperty(EngineConstants.THRESHOLD_NAME, "100"));
      }
    });

    try {

      String name = EngineConstants.THRESHOLD_NAME;
      final String[] arguments = new String[] {"updatethresholdLimit", "100"};

      UpdateThresholdLimit testInstance = new UpdateThresholdLimit(arguments) {
        protected EngineAdmin createNewEngineAdmin() {
          return engineAdmin;
        }
      };
      
      testInstance.performCommand();
      int result = Integer.parseInt(prop.getProperty(name));

      assertTrue("The 2 values should be same", result == 100);
    } catch (Exception setexc) {
      setexc.printStackTrace();

      fail("Failed, Exception");

    }
  }
  
  private static <T> Action setProperty(final String key, final String value) {
    return new SetProperty<T>(prop, key, value);
  }

}

 




  
  
  
  


