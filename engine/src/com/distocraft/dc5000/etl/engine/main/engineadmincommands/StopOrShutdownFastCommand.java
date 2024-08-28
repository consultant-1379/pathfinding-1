/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import java.io.File;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eemecoy
 *
 */
public class StopOrShutdownFastCommand extends Command {

    private final String commandName;

    /**
     * @param args
     */
    public StopOrShutdownFastCommand(final String[] args) {
        super(args);
        commandName = arguments[0];
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkAndConvertArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        //nothing to really check here
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        return 1;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
     */
    @Override
    String getUsageMessage() {
        return commandName;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
     */
    @Override
    public void performCommand() throws NumberFormatException, Exception {
        final File dwhdbFullFlagFile = createFileObject("/tmp/dwhdb_full");
        if (dwhdbFullFlagFile.exists()) {
            getRunTime().exec("engine stop");
        } else {
            final EngineAdmin admin = createNewEngineAdmin();
            admin.fastGracefulShutdown();
        }

    }

    Runtime getRunTime() {
        return Runtime.getRuntime();
    }

    File createFileObject(final String fileName) {
        return new File(fileName);
    }

}
