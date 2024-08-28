package com.distocraft.dc5000.etl.engine.common;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class for showing common messages and handling exceptions
 */
public class ExceptionHandler {

  private static final Logger log = Logger.getLogger("etlengine.engine.ExceptionHandler");

  private ExceptionHandler() {
  	
  }
  
  /**
   * Shows the correct error message for different exceptions
   * 
   * @param e
   *          exception to be shown
   * @param strOwnMessage
   *          own string to the end of the message
   */
  public static void handleException(final Exception e, final String strOwnMessage) {
    handle(e, strOwnMessage);
  }

  /**
   * Shows the correct error message for different exceptions
   * 
   * @param e
   *          exception to be shown
   * @param strOwnMessage
   *          own string to the end of the message
   */
  public static void handleException(final Exception e) {
    handle(e, "");
  }

  /**
   * Shows the correct error message for different exceptions
   * 
   * @param e
   *          exception to be shown
   * @param strOwnMessage
   *          own string to the end of the message
   */
  private static void handle(final Exception e, final String strOwnMessage) {
    
    if (e instanceof EngineBaseException) {
      
      final EngineBaseException moottorinPohjaPoikkeus = (EngineBaseException)e;

      log.log(Level.SEVERE,strOwnMessage,e);
      
      printEngineBaseException(moottorinPohjaPoikkeus);
      
    } else {
      log.log(Level.SEVERE,strOwnMessage,e);
    }
  }
  
  private static void printEngineBaseException(final EngineBaseException moottorinPohjaPoikkeus) {
    
    final Throwable ne = moottorinPohjaPoikkeus.getNestedException();
    final String message = moottorinPohjaPoikkeus.getErrorReasonMessage();
    final String method = moottorinPohjaPoikkeus.getMethodName();
    final String errortype = moottorinPohjaPoikkeus.getErrorType();
    final String reason = moottorinPohjaPoikkeus.getErrorReasonMessage();

    log.info("Message: "+message);
    log.info("Method: "+method);
    log.info("ErrorType: "+errortype);
    log.info("Reason: "+reason);
    
    if(ne != null) {
      log.log(Level.INFO,"Nested exception",ne);
      
      if(ne instanceof EngineBaseException) {
        printEngineBaseException((EngineBaseException)ne);
      }
      
    }
  }

}
