/**
 * 
 */
package com.ericsson.eniq.enminterworking;

import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import com.ericsson.eniq.flssymlink.fls.FlsProfileHandler;
import com.ericsson.eniq.flssymlink.fls.Main;

/**
 * @author xnagdas
 *
 */
public class ChangeProfileCommand extends Command {

    private String profileName;
    private String ossId;


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
        if (arguments.length < 3) {
            throw new InvalidArgumentsException("Incorrect number of arguments supplied \n Usage: /eniq/admin/bin/fls -e "
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
        return "changeProfile -o <<valid_enm_host_name_alias>|ALL|all> -p <Normal|OnHold>";
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
     */
    @Override
    public void performCommand() throws NumberFormatException, Exception {
        final FlsProfileHandler admin = createNewFLSAdmin();
            final boolean succeed = admin.changeProfileWtext(ossId, profileName);
            if (!succeed) {
                System.out.println(FlsProfileHandler.RED+"Changing Profile Failed"+FlsProfileHandler.RESET);
            }
    }

    /* (non-Javadoc)
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkAndConvertArgumentTypes()
     */
    @Override
    void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
    	ossId = arguments[1];
    	profileName = arguments[2];
    	if( (!profileName.equals("Normal")) & (!profileName.equals("OnHold")) ){
    		throw new InvalidArgumentsException("Invalid arguments supplied \n Usage: /eniq/admin/bin/fls -e "
    				+ getUsageMessage());
    	}
    }

}
