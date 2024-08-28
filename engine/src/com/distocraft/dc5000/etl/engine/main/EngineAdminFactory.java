/**
 * 
 */
package com.distocraft.dc5000.etl.engine.main;

/**
 * Helper class for creating EngineAdmin instance. For example jmock junit test can
 * set own mocked EngineAdmin instance using this class:<br>
 * <br>
 * EngineAdmin mockedEngineAdmin = context(EngineAdmin.class);<br>
 * EngineAdminFactory.setInstance(mockedEngineAdmin);<br>
 * ...<br>
 * engineAdmin = EngineAdminFactory.getInstance();<br>
 * ...<br>
 * @author eheijun
 *
 */
public class EngineAdminFactory {
  
  private static EngineAdmin _instance;
  
  private EngineAdminFactory() {}
  
  /**
   * Get instance of the EngineAdmin. 
   * @return
   */
  public static EngineAdmin getInstance()  {
    if (_instance == null) {
      _instance = new EngineAdmin(); 
    }
    return _instance;
  }

  /**
   * Set alternative instance of the EngineAdmin
   * @param instance
   */
  public static void setInstance(final EngineAdmin instance)  {
    _instance = instance;
  }

}
