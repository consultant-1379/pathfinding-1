/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eemecoy
 *
 */
public class RemoveSetFromPriorityQueueCommand extends Command {

    private long setId;

    /**
     * @param args
     */
    public RemoveSetFromPriorityQueueCommand(final String[] args) {
        super(args);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        return 2;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
     */
    @Override
    public void performCommand() throws NumberFormatException {
        final EngineAdmin admin = createNewEngineAdmin();
        try {
            admin.removeSetFromPriorityQueue(setId);
        } catch (final Exception e) {
            System.out.println("Invalid ID entered ");
            System.exit(0);
        }

    }

    @Override
    String getUsageMessage() {
        return "removeSetFromPriorityQueue long";
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        setId = convertArgumentToLong(arguments[1]);
    }
}
