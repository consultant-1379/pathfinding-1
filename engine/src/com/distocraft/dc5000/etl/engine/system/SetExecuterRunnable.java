/**
 * A simple runnable class that is run within a thread
 * 
 * This class checks that if the engine profile must be changed.
 * If profile must be changed, we run the method and wait until 
 * the profile is changed. After that we execute normally the actual EngineThread,
 * which is some set of actions. The EngineThread is run in its own thread
 * so therefore we can return from this "run" method immediately. 
 * 
 * Nothing 
 */
package com.distocraft.dc5000.etl.engine.system;

import java.rmi.RemoteException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.distocraft.dc5000.etl.engine.main.EngineThread;
import com.distocraft.dc5000.etl.engine.main.TransferEngine;

/**
 * @author epetrmi
 *
 */
public class SetExecuterRunnable implements Runnable {
	
  private static final Logger log = Logger.getLogger(SetExecuterRunnable.class);
  private final SetListener setListener; 
  private final EngineThread et;
  private final TransferEngine tEngine;
  private final Properties props;
  
  private final static String PROPERTY_PROFILE = "engine.profile.before.execution";
  
  public SetExecuterRunnable(final Properties props, final SetListener setListener, final EngineThread et, final TransferEngine transferEngine) {
    this.props = props;
    this.setListener = setListener;
    this.et = et;
    this.tEngine = transferEngine;
  }
  
  public void run() {
    log.debug("Starting run()");

    //BEFORE
    log.debug("Using properties="+props);
    if(props!=null){
    	final String profile = props.getProperty(PROPERTY_PROFILE, "NO_VALUE");
      if(!"NO_VALUE".equals(profile)){
          try {
            log.debug("Trying to set engine.profile="+profile);
            setListener.addStatusEvent(StatusEvent.statusEventWithCurrentTime(this, "Trying to set engine.profile="+profile));            
            tEngine.setAndWaitActiveExecutionProfile(profile);
            setListener.addStatusEvent(StatusEvent.statusEventWithCurrentTime(this, "Succesfully set engine.profile="+profile));
            log.debug("Finished setting engine.profile="+profile);
          } catch (RemoteException e) {
            // This shouldnt happen because we run this on the same machine
            e.printStackTrace();
            setListener.addStatusEvent(StatusEvent.statusEventWithCurrentTime(this, "Failed to set engine.profile="+profile+". Still continuing execution."));
            log.debug("Failed to set engine.profile="+profile);
          }
      }
      
    }
    //RUN
    try {
      log.debug("Starting to run set");
      setListener.addStatusEvent(StatusEvent.statusEventWithCurrentTime(this, "Sending execute-signal to enginethread"));
      tEngine.executeEngineThreadWithListener(et);
      setListener.addStatusEvent(StatusEvent.statusEventWithCurrentTime(this, "Succesfully sent signal to enginethread"));
    } catch (Exception e) {
      e.printStackTrace();
    }
    log.debug("....finishing run()");
  }

}
