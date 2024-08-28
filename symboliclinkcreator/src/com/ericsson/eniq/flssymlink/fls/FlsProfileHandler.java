/**
 * 
 */
package com.ericsson.eniq.flssymlink.fls;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.enminterworking.ChangeProfileCommand;
import com.ericsson.eniq.enminterworking.Command;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.IEnmInterworkingRMI;
import com.ericsson.eniq.enminterworking.InvalidArgumentsException;
import com.ericsson.eniq.enminterworking.InvalidSetParametersRemoteException;
import com.ericsson.eniq.enminterworking.NoSuchCommandException;
import com.ericsson.eniq.enminterworking.StatusCommand;

/**
 * @author xnagdas
 *
 */
public class FlsProfileHandler {

	private static final Map<String, Class<? extends Command>> CMD_TO_CLASS = new HashMap<String, Class<? extends Command>>();
	
	public static final String RESET = "\u001B[0m";
	public static final String BLACK = "\u001B[30m";
	public static final String RED = "\u001B[31m";
	public static final String GREEN = "\u001B[32m";
	public static final String YELLOW = "\u001B[33m";
	public static final String BLUE = "\u001B[34m";
	public static final String PURPLE = "\u001B[35m";
	public static final String CYAN = "\u001B[36m";
	
	public static final String ALL = "ALL";
		
	public static void main ( String args[] ){
		Logger log = Logger.getLogger("symboliclinkcreator.fls");
		try {
			System.setSecurityManager(new com.ericsson.eniq.enminterworking.ETLCSecurityManager());
			if (args.length < 1 || (args.length > 1 && args.length < 3)) {
				showUsage();
			} else {
				final String commandName = args[0];
				final Command command = createCommand(commandName, args);
				command.validateArguments();
				command.performCommand();
			}

		} catch (final java.rmi.UnmarshalException ume) {
			// Exception, cos connection breaks, when engine is shutdown
			System.exit(3);
		} catch (final java.rmi.ConnectException rme) {
			System.err.println("Connection to FLS refused");
			System.exit(2);
		} catch (final InvalidSetParametersRemoteException invalidParam) {
			System.err.println(invalidParam.getMessage());
			showUsage();
			System.exit(1);
		} catch (final NoSuchCommandException noSuchCommandEx) {
			System.err.println(noSuchCommandEx.getMessage());
			showUsage();
			System.exit(1);
		} catch (final InvalidArgumentsException invalidArgsEx) {
			System.err.println(invalidArgsEx.getMessage());
			System.exit(1);
		} catch (final RemoteException remoteEx) {
			System.err.println(remoteEx.getMessage());
			System.exit(1);
		} catch ( final NotBoundException notBoundEx) {
			// mainly for the case of initial install where engine is stopped but was never started
			System.err.println("Connection to FLS failed. (Not bound)");  // NOPMD
			System.exit(1); // NOPMD */
		} catch (final Exception e) {
			log.fine( "General Exception" + e);
      final String msg = e.getMessage();
      if (msg != null && msg.equals("FLS initialization has not been completed yet")) {
        System.err.println(msg);
        System.exit(4);
      }
      e.printStackTrace(System.err);
      System.exit(1);
		}
		System.exit(0);
		
	}
	
	public void status() throws MalformedURLException, NotBoundException, RemoteException {
		Logger log = Logger.getLogger("symboliclinkcreator.fls");
		log.info("Getting status...");
		IEnmInterworkingRMI multiEs = connect();
		System.out.println("Getting status...");
		System.out.println("\n");
		List<List<String>> status = multiEs.status();
		List<String> statusForCli = status.get(0);
		List<String> statusForLog = status.get(1);
		System.out.println(getPrintableString(statusForCli));
		log.info("Status : \n" + getPrintableString(statusForLog));
		System.out.println(GREEN+"Completed successfully"+RESET);
		log.info("Getting status completed successfully");
	}
	
	private String getPrintableString(List<String> lines) {
		StringBuffer buffer = new StringBuffer();
		for (String line : lines) {
			buffer.append(line);
			buffer.append("\n");
		}
		return buffer.toString();
	}
	
	public boolean changeProfileWtext(String ossId, final String profileName) throws MalformedURLException, NotBoundException,
	RemoteException {
		Logger log = Logger.getLogger("symboliclinkcreator.fls");
		System.out.println("Changing the FLS profile of "+ossId+" to: " + profileName);
		log.info("Changing the FLS profile of "+ossId+" to: " + profileName);
		List<Boolean> resultList = changeProfile(ossId, profileName);
		boolean exceptionallyFailed = resultList.get(0);
		boolean noValidInstance = resultList.get(1);
		boolean noMount=resultList.get(2);
		if (!exceptionallyFailed && !noValidInstance && !noMount) {
			System.out.println(GREEN+"FLS Profile of "+ossId+" successfully changed to " + profileName+RESET );
			log.info("FLS Profile of "+ossId+" successfully changed to " + profileName );
		} else if(exceptionallyFailed) {
			if (!ALL.equals(ossId)) {
				System.out.println("Could not activate profile (" + profileName + ") for "+ossId);
				log.info("Could not activate profile (" + profileName + ") for "+ossId);
			} else {
				System.out.println("Could not activate profile (" + profileName + ") globally");
				log.info("Could not activate profile (" + profileName + ") globally");
			}
		}  else if(noMount) {
			if (!ALL.equals(ossId)) {
				System.out.println("FLS Mounts are not up.Could not activate profile (" + profileName + ") for "+ossId);
				log.info("FLS Mounts are not up.Could not activate profile (" + profileName + ") for "+ossId);
			} else {
				System.out.println("FLS Mounts are not up.Could not activate profile (" + profileName + ") globally");
				log.info("FLS Mounts are not up.Could not activate profile (" + profileName + ") globally");
			}
		}   else if (noValidInstance) {
			System.out.println(RED + ossId + RESET+" does not have a valid instance. "
					+ "Please supply a valid ENM_HOSTNAME_ALIAS");
			log.warning(ossId+ " does not have a valid instance. Please supply a valid ENM_HOSTNAME_ALIAS");
		}
		return !exceptionallyFailed && !noValidInstance && !noMount;
	}
	
	//public boolean changeProfile(String ossId, final String profileName) throws MalformedURLException, NotBoundException,
	//RemoteException {
		//return changeProfile(ossId, profileName, true);
	//}

	public List<Boolean> changeProfile(String ossId, final String profileName) throws MalformedURLException, NotBoundException,
			RemoteException {
		IEnmInterworkingRMI multiEs = connect();
		return multiEs.changeProfile(ossId, profileName);
		
	}

	
	private static void showUsage() {
		System.out.println("Usage:");
		System.out.println("/eniq/admin/bin/fls start|stop|status");
		System.out.println("/eniq/admin/bin/fls -e changeProfile -o <<valid_oss_alias>|ALL|all> -p <Normal|OnHold>");
		System.out.println();
		System.out.println("    The following commands are not supported and shall not be used unless directed by Ericsson.");
		System.out.println("        start");
		System.out.println("        stop");
//		System.out.println("        restart");
		System.out.println();
		System.exit(1);
	}
	
	static Command createCommand(final String commandName, final String[] args) throws IllegalArgumentException,
	InstantiationException, IllegalAccessException, InvocationTargetException, SecurityException,
	NoSuchMethodException, NoSuchCommandException {
		
		final Class<? extends Command> classToUse = CMD_TO_CLASS.get(commandName);
		if (classToUse == null) {
			throw new NoSuchCommandException("Invalid command entered: " + commandName);
		}
		final Class<? extends String[]> class1 = args.getClass();
		final Constructor<? extends Command> constructor = classToUse.getConstructor(class1);
		final Object constArguments = args;
		return constructor.newInstance(constArguments);
		}

	/**
	 * Looks up the FLS
	 */
	public static IEnmInterworkingRMI connect() throws NotBoundException, MalformedURLException, RemoteException {

		IEnmInterworkingRMI termi = null;
		termi = (IEnmInterworkingRMI) Naming
				.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(EnmInterCommonUtils.getEngineIP()));
      
		if (termi == null){
			System.err.println("Could not connect to RMI Registry. ");
            System.exit(99);
		}
        return termi;
	}
	
	
	static {
		CMD_TO_CLASS.put("status", StatusCommand.class);
		CMD_TO_CLASS.put("changeProfile", ChangeProfileCommand.class);
	}
	
}

