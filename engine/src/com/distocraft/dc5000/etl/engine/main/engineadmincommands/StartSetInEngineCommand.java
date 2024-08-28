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
public class StartSetInEngineCommand extends Command {

    private String ip;

    private String userName;

    private String password;

    private String driver;

    private String collection;

    private String set;

    /**
     * @param args
     */
    public StartSetInEngineCommand(final String[] args) {
        super(args);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkAndConvertArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        ip = arguments[1];
        userName = arguments[2];
        password = arguments[3];
        driver = arguments[4];
        collection = arguments[5];
        set = arguments[6];
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        return 7;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
     */
    @Override
    String getUsageMessage() {
        return "startSetInEngine ip(string) username(string) password(string) driver(string) collection(string) set(string)";
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
     */
    @Override
    public void performCommand() throws NumberFormatException, Exception {
        final EngineAdmin engineAdmin = createNewEngineAdmin();
        engineAdmin.startSet(ip, userName, password, driver, collection, set);

    }

}
