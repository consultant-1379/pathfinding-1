package com.distocraft.dc5000.etl.engine.common;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.rmi.Naming;
import java.util.Properties;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.distocraft.dc5000.etl.engine.main.EngineAdminFactory;
import com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI;

/**
* @author eanubda
*         This class has helper methods to connect to engine.
*/
public class EngineConnect {

 private EngineConnect(){}

 private static final Logger log = Logger.getLogger("EngineConnect");

 private static int serverPort;

 private static String serverHostName;

 private static String serverRefName;
 
 static RockFactory etlRock = null;
 
 	public static boolean changeProfile(final String profile) {
	  EngineAdmin engineAdmin = EngineAdminFactory.getInstance();
	    log.info("Going to change engine execution profile to: " + profile);
	    try {
	      if (!engineAdmin.changeProfile(profile, null)) {
	        log.severe(profile + " execution profile could not be set");
	      }
	      log.info("Engine execution profile has been changed to: " + profile);
	      return true;
	    } catch (Exception e) {
	      log.severe("Could not put engine to " + profile + " execution profile." + e);
	      e.printStackTrace();
	      return false;
	    }
	  }

	/**
	 * Looks up the transfer engine
	 */
	public static ITransferEngineRMI connect() throws Exception {

		getEngineConnectionProperties();
		
		//final String rmiURL = "//" + serverHostName + ":" + serverPort + "/" + serverRefName;

		//log.fine("Connecting engine @ " + rmiURL);

		final ITransferEngineRMI termi = (ITransferEngineRMI) Naming
				.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl());

		return termi;
	}
 
	public static void getEngineConnectionProperties() throws Exception {
	try {

		String sysPropDC5000 = System.getProperty("dc5000.config.directory");
		if (sysPropDC5000 == null) {
			sysPropDC5000 = "/eniq/sw/conf";
		}

		if (!sysPropDC5000.endsWith(File.separator)) {
			sysPropDC5000 += File.separator;
		}

		final FileInputStream streamProperties = new FileInputStream(sysPropDC5000 + "ETLCServer.properties");
		final Properties appProps = new Properties();
		appProps.load(streamProperties);

		if (serverHostName == null	|| serverHostName.equalsIgnoreCase("")) {

			serverHostName = appProps.getProperty("ENGINE_HOSTNAME", null);
			if (serverHostName == null) { 
				serverHostName = "localhost";

				try {
					serverHostName = InetAddress.getLocalHost().getHostName();
				} catch (final java.net.UnknownHostException ex) {
					throw new Exception("Unable to connect to Host: " + serverHostName);
				}
			}
		}

		final String sporttmp = appProps.getProperty("ENGINE_PORT", "1200");
		try {
			serverPort = Integer.parseInt(sporttmp);
		} catch (final NumberFormatException nfe) {
			throw new Exception("Engine Port in a numerical format.");
		}

		serverRefName = appProps.getProperty("ENGINE_REFNAME",	"TransferEngine");

		streamProperties.close();

	} catch (final Exception e) {
		log.warning("Cannot read configuration from ETLCServer.properties: " + e.getMessage());
	}
}

}
