package com.distocraft.dc5000.etl.engine.system;

import java.util.Enumeration;
import java.util.Vector;
import java.util.logging.Logger;

public class ETLCEventHandler {

	// This has to be Vector for not to fail fast
  private final Vector<ETLCEventListener> listeners = new Vector<ETLCEventListener>(); // NOPMD
  
  private final Logger log;
  
  public ETLCEventHandler(final String shareKey) {
    log = Logger.getLogger("ETLCEventHandler." + shareKey);
  }

  public void addListener(final ETLCEventListener listener) {
    log.fine("Adding listener into event handling.");
    
    try {
      
      if(listeners.contains(listener)){
        log.info("Listener is already in listeners list.");
      } else {
        listeners.add(listener);
      }
        
    } catch (Exception e) {
      
      log.warning("Exception occured in adding a listener.");
      
    }
  }

  public void removeListener(final ETLCEventListener listener){
    log.fine("Removing listener from event handling listeners list.");
    
    try {
      
      listeners.remove(listener);
      
    } catch (Exception e) {
      
      log.warning("Exception occured in removing listener from event handling.");
      
    }
  }
  
  public void triggerListeners(final String key) {
    log.fine("Triggering listeners with key = " + key + " .");
    
    try {
      
      final Enumeration<ETLCEventListener> enumeration = listeners.elements();
  
      while (enumeration.hasMoreElements()) {
        final ETLCEventListener listener = enumeration.nextElement();
        listener.triggerEvent(key);
      }
      
    } catch (Exception e){
      
      log.warning("Exception occured in triggering listeners with key = " + key + " .");
      
    }
  }
  
}
