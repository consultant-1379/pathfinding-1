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
public class HoldSetInPriorityQueueCommand extends Command {

    private long setId;

    /**
     * @param args
     */
    public HoldSetInPriorityQueueCommand(final String[] args) {
        super(args);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        setId = convertArgumentToLong(arguments[1]);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        return 2;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
     */
    @Override
    String getUsageMessage() {
        return "holdSetInPriorityQueue setId(long)";
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
     */
    @Override
    public void performCommand() throws NumberFormatException {
        final EngineAdmin admin = createNewEngineAdmin();
        try {
            admin.holdSetInPriorityQueue(setId);
        } catch (final Exception e) {
            System.out.println("Invalid ID entered ");
            System.exit(0);
        }

    }

}
