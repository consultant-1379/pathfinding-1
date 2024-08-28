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
public class ChangeProfileAndWaitCommand extends Command {

    private String profileName;

    /**
     * @param arguments
     */
    public ChangeProfileAndWaitCommand(final String[] arguments) {
        super(arguments);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.Command#performCommand()
     */
    @Override
    public void performCommand() throws NumberFormatException, Exception {
        final EngineAdmin admin = createNewEngineAdmin();
        final boolean succeed = admin.changeProfileAndWaitWtext(profileName);
        if (!succeed) {
            System.out.println("Changing Profile Failed");
        }
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        return 2;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        profileName = arguments[1];
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
     */
    @Override
    String getUsageMessage() {
        return "changeProfileAndWait profileName(string)";
    }

}
