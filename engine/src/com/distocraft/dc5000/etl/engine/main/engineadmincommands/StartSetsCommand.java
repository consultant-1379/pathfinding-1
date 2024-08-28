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
public class StartSetsCommand extends Command {

    private String techPacks;

    private String sets;

    private int times;

    /**
     * @param arguments
     */
    public StartSetsCommand(final String[] arguments) {
        super(arguments);
    }

    @Override
    protected int getCorrectArgumentsLength() {
        return 4;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.Command#performCommand()
     */
    @Override
    public void performCommand() throws NumberFormatException, Exception {
        final EngineAdmin admin = createNewEngineAdmin();
        admin.startSets(techPacks, sets, times);

    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        techPacks = arguments[1];
        sets = arguments[2];
        times = convertArgumentToInteger(arguments[3]);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
     */
    @Override
    String getUsageMessage() {
        return "startSets techPacks(string) sets(string) times(long)";
    }
}
