package com.ericsson.eniq.enminterworking.automaticNAT;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.IEnmInterworkingRMI;
import com.ericsson.eniq.enminterworking.automaticNAT.HealthMonitor.Status;

import static com.ericsson.eniq.enminterworking.automaticNAT.HealthMonitor.*;

public class KeepAlive extends TimerTask {
	
	public static final String MASTER = "MASTER";
	
	private static Logger log = Logger.getLogger("symboliclinkcreator.fls");
			
	@Override
	public void run() {
		try {
			String myRole = EnmInterCommonUtils.getSelfRole();
			String masterIP = EnmInterCommonUtils.getEngineIP();
			if (MASTER.equals(myRole)) {
				List<Eniq_Role> roleTableList = NodeAssignmentCache.getRoleTableContents();
				for (Eniq_Role eniq : roleTableList) {
					enquireHealth(eniq, masterIP);
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING," Exception in keep Alive thread ",e);
		}
	}
	
	private static void enquireHealth(Eniq_Role eniq, String masterIP) {
		String eniqId = eniq.getEniq_identifier();
		if (!eniqId.equals(EnmInterCommonUtils.getEngineHostname())) {
			try {
				Status currentStatus = getStatus(eniqId);
				log.info("KeepAlive thread running - current status for eniq : "+eniqId+" is "+currentStatus);
				IEnmInterworkingRMI multiEs =  (IEnmInterworkingRMI) Naming.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(eniq.getIp_address()));
				if (multiEs.checkHealth()) {
					if (currentStatus != Status.UP) {
						log.info("setting status of eniq : "+eniqId+" to UP");
						setStatus(eniqId, Status.UP);
						if (currentStatus != Status.NOT_DETERMINED) {
							log.info("synching up NAT table");
							if (("FAIL").equals((multiEs.updateSlaveNAT(masterIP)))) {
								log.log(Level.WARNING,"failed to sync up NAT");
							}
							multiEs.refreshNodeAssignmentCache();
						}
					}
				} else {
					if (getStatus(eniqId) != Status.DOWN) {
						log.info("Health check failed : setting status of eniq : "+eniqId+" to DOWN");
						setStatus(eniqId, Status.DOWN);
					}
				}
			} catch (MalformedURLException | RemoteException | NotBoundException e) {
				log.log(Level.WARNING, "Not able to connect to "+eniq, e.getMessage());
				if (getStatus(eniqId) != Status.DOWN) {
					log.info("setting status of eniq : "+eniqId+" to DOWN");
					setStatus(eniqId, Status.DOWN);
				}
			}
		}
	}

}
