/**
 * 
 */
package com.distocraft.dc5000.etl.engine.main;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;


/**
 * @author eheijun
 *
 */
public class EngineAdminFactoryTest {

  private final Mockery context = new JUnit4Mockery() {
    {
      setImposteriser(ClassImposteriser.INSTANCE);
    }
  };
  
  /**
   * @throws Exception
   */
  @After
  public void tearDownAfterClass() throws Exception {
    EngineAdminFactory.setInstance(null);
  }
  
  /**
   * Test method for {@link com.ericsson.eniq.afj.common.EngineAdminFactory#getInstance()}.
   */
  @Test
  public void testGetDefaultInstance() {
    EngineAdminFactory.setInstance(null);
    try {
    	final EngineAdmin engineAdmin = EngineAdminFactory.getInstance();
      assertTrue(engineAdmin != null);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  /**
   * Test method for {@link com.ericsson.eniq.afj.common.EngineAdminFactory#getInstance()}.
   */
  @Test
  public void testGetMockInstance() {
	  final EngineAdmin mockEngineAdmin = context.mock(EngineAdmin.class);
    EngineAdminFactory.setInstance(mockEngineAdmin);
    try {
    	final EngineAdmin engineAdmin = EngineAdminFactory.getInstance();
      assertTrue(engineAdmin == mockEngineAdmin);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

}
