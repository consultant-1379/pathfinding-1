package com.ericsson.eniq.enminterworking.automaticNAT;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.IEnmInterworkingRMI;

public class RetainExistingNodes {
		
	private static Logger log;
		
	public static void main(String[] args) {
		try {
			log = Logger.getLogger("symboliclinkcreator.retention");
			log.info("Received node retention file : "+args[0]);
			if (args.length == 2) {
				log.info("Received enmHostName : "+args[1]);
				if (isValidHostName(args[1])) {
					getConnection().retainNodes(args[0], args[1].split("\\.")[0]);
				} else {
					log.warning("HostName is not valid");
					System.exit(99);
				}
				
			} else {
				log.info("Did not receive enmHostName, so automatic mapping will be performed ");
				getConnection().retainNodes(args[0],null);
			}
		} catch (Exception e) {
			log.log(Level.WARNING,"Exception when trying to retain nodes ",e);
		}
	}
	
	private static IEnmInterworkingRMI getConnection() throws NotBoundException, MalformedURLException, RemoteException {
		IEnmInterworkingRMI multiEs = null;
		multiEs = (IEnmInterworkingRMI) Naming
				.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(EnmInterCommonUtils.getEngineIP()));
      
		if (multiEs == null){
			log.warning("Could not connect to RMI Registry. ");
            System.exit(99);
		}
        return multiEs;
	}
	
	private static boolean isValidHostName(String hostName) {
		if(hostName == null ) {
			return false;
		}
		hostName = hostName.trim();
		if (("").equals(hostName) || hostName.indexOf(" ") != -1 || hostName.length()>253){
			return false;
		}
		if (hostName.lastIndexOf(".") == hostName.length()-1) {
			hostName = hostName.substring(0,hostName.lastIndexOf("."));
			log.info("hostname contains trailing dot");
		}
		Pattern p = Pattern.compile("^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])(\\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9]))*$");
		Matcher m = p.matcher(hostName);
		if (m.matches()) {
			return true;
		}
		return false;
		
	}
	
	

}
