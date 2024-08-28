/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

/**
 * @author eemecoy
 *
 */
public abstract class BaseStartSetCommand extends Command {

    String set;

    String collection;

    String scheduleInfo;

    /**
     * @param args
     */
    public BaseStartSetCommand(final String[] args) {
        super(args);

    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkAndConvertArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        collection = arguments[1];
        set = arguments[2];
        if (arguments.length == 4) {
            scheduleInfo = createPropertyString(arguments[3]);
        } else {
            scheduleInfo = "";
        }

    }

    @Override
    void checkNumberOfArguments() throws InvalidArgumentsException {
        if (arguments.length < 3 || arguments.length > 4) {
            throw new InvalidArgumentsException("Incorrect number of arguments supplied, usage: engine -e "
                    + getUsageMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        //not used - this is a special case, as either 3 or 4 arguments is acceptable, see overridden method validateArguments
        return 0;
    }

}
