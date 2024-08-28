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
 * Command to remove all sets of a tech pack in the priority queue.
 * @author eciacah
 */
public class RemoveTechPacksInPriorityQueueCommand extends Command {

  private List<String> techPackNames;

  // For unit tests:
  public List<String> getTechPackNames() {
    return techPackNames;
  }

  /**
   * 
   * @param args
   *          A list of tech packs to remove from the queue.
   */
  public RemoveTechPacksInPriorityQueueCommand(final String[] args) {
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
    return "removeTechPackInPriorityQueue <tech pack 1> <tech pack 2> ...\n" +
        "At least one tech pack should be specified";
  }

  @Override
  public void performCommand() throws Exception {
    final EngineAdmin admin = createNewEngineAdmin();
    try {
        admin.removeTechPacksInPriorityQueue(techPackNames);
    } catch (final Exception e) {
        System.out.println("Failed to remove tech packs from priority queue");
        System.exit(0);
    }
  }

  @Override
  protected int getCorrectArgumentsLength() {
    // Not needed, checkNumberOfArguments overridden instead.
    // This is only called by checkNumberOfArguments.
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
