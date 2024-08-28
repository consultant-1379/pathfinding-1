/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2013 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.ericsson.eniq.common.CommonUtils;


/**
 * Command to lock or unlock users in Events UI. 
 * @author eciacah
 */
public class LockEventsUIusersCommand extends Command {
    
  private boolean lock;
  
  public LockEventsUIusersCommand(final String[] args) {
    super(args);
  }

  @Override
  void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
    final String onOrOff = arguments[1];
    
    if (onOrOff.equalsIgnoreCase("on")) {
      this.lock = true;
    } else if (onOrOff.equalsIgnoreCase("off")){
      this.lock = false;
    } else {
      throw new InvalidArgumentsException("Invalid option, usage: "
          + getUsageMessage());
    }
  }

  @Override
  String getUsageMessage() {
      return "lockEventsUIusers on | off";
  }

  @Override
  public void performCommand() throws Exception {
    final String serverType = getServerType();
    if (serverType.equalsIgnoreCase("events")) {
      final EngineAdmin admin = createNewEngineAdmin();
      admin.lockEventsUIusers(this.lock);      
    } else {
      // Stats server:
      System.out.println("lockEventsUIusers command can only be used on an Eniq Events deployment"); 
    }
  }
  
  protected String getServerType() {
    return CommonUtils.getServerType();
  }

  @Override
  protected int getCorrectArgumentsLength() {
    return 2;
  }
  
  // For unit tests:
  public boolean isLocked() {
    return lock;
  }
  
  public void setLocked(final boolean locked) {
    this.lock = locked;
  }
  

}
