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
public class ChangeSetPriorityInPriorityQueueCommand extends Command {

    private long setId;

    private long newPriority;

    /**
     * @param args
     */
    public ChangeSetPriorityInPriorityQueueCommand(final String[] args) {
        super(args);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        return 3;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
     */
    @Override
    public void performCommand() throws NumberFormatException {
        final EngineAdmin admin = createNewEngineAdmin();
        try {
            admin.changeSetPriorityInPriorityQueue(setId, newPriority);
        } catch (final Exception e) {
            System.out.println("Invalid ID / New Priority entered ");
            System.exit(0);
        }

    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        setId = convertArgumentToLong(arguments[1]);
        newPriority = convertArgumentToLong(arguments[2]);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
     */
    @Override
    String getUsageMessage() {
        return "changeSetPriorityInPriorityQueue setId(long) newPriority(long)";
    }
}
