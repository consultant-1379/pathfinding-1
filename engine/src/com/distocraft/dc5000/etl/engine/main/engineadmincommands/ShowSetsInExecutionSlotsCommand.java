/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import java.util.ArrayList;
import java.util.List;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eemecoy
 *
 */
public class ShowSetsInExecutionSlotsCommand extends Command {
  
    private List<String> techPackNames;
    
    // For unit tests
    public List<String> getTechPackNames() {
      return techPackNames;
    }

    /**
     * @param args
     */
    public ShowSetsInExecutionSlotsCommand(final String[] args) {
        super(args);
        techPackNames = new ArrayList<String>();
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkAndConvertArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
      // Iterate through arguments and add them to list
      for (int index = 1; index < arguments.length; index++) {
        techPackNames.add(arguments[index]);   
      }    
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        return 0;
    }
    
    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkNumberOfArguments()
     */
    @Override
    void checkNumberOfArguments() throws InvalidArgumentsException {
        if (arguments.length < 1) {
            throw new InvalidArgumentsException("Incorrect number of arguments supplied, usage: engine -e "
                    + getUsageMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
     */
    @Override
    String getUsageMessage() {
        return "showSetsInExecutionSlots or engine -e slots" + 
            "To show sets only for specific tech packs: engine -e showSetsInExecutionSlots <tech pack 1> <tech pack 2>...";
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
     */
    @Override
    public void performCommand() throws Exception {
        final EngineAdmin admin = createNewEngineAdmin();
        admin.showSetsInExecutionSlots(techPackNames);
    }

}
