package com.distocraft.dc5000.etl.engine.common;

import org.junit.Test;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
/**
 * @author edujmau
 */
public class SetContextTest {

  @Test(expected=EngineMetaDataException.class)
  public void getObjectFromSetContextTest() throws EngineMetaDataException{
    
    SetContext setContext = new SetContext();
    setContext.getObjectFromSetContext("Testing");
  }
}
