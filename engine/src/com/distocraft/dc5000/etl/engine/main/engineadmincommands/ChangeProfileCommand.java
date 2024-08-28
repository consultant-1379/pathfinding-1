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
public class ChangeProfileCommand extends Command {

    private String profileName;

    private String messageText;

    /**
     * @param args
     */
    public ChangeProfileCommand(final String[] args) {
        super(args);
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkNumberOfArguments()
     */
    @Override
    void checkNumberOfArguments() throws InvalidArgumentsException {
        if (arguments.length < 2) {
            throw new InvalidArgumentsException("Incorrect number of arguments supplied, usage: engine -e "
                    + getUsageMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
     */
    @Override
    protected int getCorrectArgumentsLength() {
        //not used - special case, see overriden method checkAndConvertArgumentTypes()
        return 0;
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
     */
    @Override
    String getUsageMessage() {
        return "changeProfileCommand profileName(string) or engine -e changeProfileCommand profileName(string) messageText(string)";
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
     */
    @Override
    public void performCommand() throws NumberFormatException, Exception {
        final EngineAdmin admin = createNewEngineAdmin();
        if (messageText == null) {
            final boolean succeed = admin.changeProfileWtext(profileName);
            if (!succeed) {
                System.out.println("Changing Profile Failed");
            }
        } else {
            admin.changeProfileWtext(profileName, messageText);

        }
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkAndConvertArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
        profileName = arguments[1];

        if (arguments.length >= 3) {
            final StringBuilder messageTextStringBuilder = new StringBuilder();
            for (int i = 2; i < arguments.length; i++) {
                messageTextStringBuilder.append(arguments[i]);
                messageTextStringBuilder.append(" ");
            }
            messageText = messageTextStringBuilder.toString();
        }

    }

}
