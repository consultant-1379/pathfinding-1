/**
 * -------------------------------------------------------------------------
 *     Copyright (C) 2013 LM Ericsson Ireland Limited.  All rights reserved.
 * -------------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import java.util.ArrayList;
import java.util.List;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * Engine command to kill running sets for a list of tech packs.
 * @author eciacah
 *
 */
public class KillRunningSetsCommand extends Command {

  private List<String> techPackNames;

  // For unit tests
  public List<String> getTechPackNames() {
    return techPackNames;
  }

  /**
   * 
   * @param args
   *          A list of tech packs to hold in the queue.
   */
  public KillRunningSetsCommand(final String[] args) {
    super(args);
    techPackNames = new ArrayList<String>();
  }

  @Override
  void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
    // Iterate through arguments and add them to list
    for (int index = 1; index < arguments.length; index++) {
      techPackNames.add(arguments[index]);      
    }       
  }

  @Override
  String getUsageMessage() {
    return "killRunningSets <tech pack 1> <tech pack 2> ...\n" +
        "At least one tech pack should be specified";
  }

  @Override
  public void performCommand() throws Exception {
    final EngineAdmin admin = createNewEngineAdmin();
    try {
        admin.killRunningSets(techPackNames);
    } catch (final Exception e) {
        System.out.println("Invalid ID entered ");
        System.exit(0);
    }
  }

  @Override
  protected int getCorrectArgumentsLength() {
    // Not needed, checkNumberOfArguments overridden instead.
    return 0;
  }

  @Override
  void checkNumberOfArguments() throws InvalidArgumentsException {
    if (arguments.length < 2) {
      throw new InvalidArgumentsException("Incorrect number of arguments supplied, usage: engine -e "
          + getUsageMessage());
    }
  }

}
