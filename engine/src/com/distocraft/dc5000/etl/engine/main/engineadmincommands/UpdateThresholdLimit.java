/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eneacon
 * 
 *         This class is called from EngineAdmin.class . It takes a user
 *         specified input and sets it as the new threshold limit for the
 *         aggregations.
 * 
 * 
 */
public class UpdateThresholdLimit extends Command {

  public UpdateThresholdLimit(final String[] args) {
    super(args);
  }

  public UpdateThresholdLimit() {
    // Use for tests
    super();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * checkAndConvertArgumentTypes()
   */
  @Override
  public void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
    // nothing to do

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * getCorrectArgumentsLength()
   */
  @Override
  protected int getCorrectArgumentsLength() {
    return 2;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * getUsageMessage()
   */
  @Override
  public String getUsageMessage() {
    return "updatethresholdLimit";
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.distocraft.dc5000.etl.engine.main.engineAdmin.Command#performCommand()
   */
  @Override
  public void performCommand() throws NumberFormatException, Exception {
    final EngineAdmin admin = createNewEngineAdmin();
    int argument = 0;
    argument = Integer.parseInt(this.arguments[1]);
    admin.updateThresholdProperty(argument);
  }

}
