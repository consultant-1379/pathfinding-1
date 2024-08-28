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
public class RefreshDBLookupsCommand extends Command {

    private String tableName;

    /**
     * @param args
     */
    public RefreshDBLookupsCommand(final String[] args) {
        super(args);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkAndConvertArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        if (arguments.length == 2) {
            tableName = arguments[1];
        }
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        //not using this, special case, see overridden method checkNumberOfArguments()
        return 0;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkNumberOfArguments()
     */
    @Override
    void checkNumberOfArguments() throws InvalidArgumentsException {
        if (arguments.length < 1 || arguments.length > 2) {
            throw new InvalidArgumentsException("Incorrect number of arguments supplied, usage: engine -e "
                    + getUsageMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
     */
    @Override
    String getUsageMessage() {
        return "refreshDBLookups tableName(string) or engine -e refreshDBLookups";
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
     */
    @Override
    public void performCommand() throws Exception {
        final EngineAdmin admin = createNewEngineAdmin();
        admin.refreshDBLookups(tableName);
    }
}
