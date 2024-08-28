package com.ericsson.eniq.enminterworking.automaticNAT;


import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;

public class HealthMonitor {
	
private static Map<String, Status> statusMap = new ConcurrentHashMap<String, Status>();

private static long DELAY = 1000*60*2L;

private static long INTERVAL = 1000*60*2L;

private static Logger log = Logger.getLogger("symboliclinkcreator.fls");

private static boolean isKeepAliveStarted = false;

public static final String MASTER = "MASTER";

	private HealthMonitor() {
		
	}
	
	public static enum Status{
		NOT_DETERMINED,
		UP,
		DOWN
	}
	
	public static void setStatus(String eniq, Status status) {
		statusMap.put(eniq, status);
	}
	
	public static Status getStatus(String eniq) {
		return statusMap.get(eniq);
	}
	
	public static void init() {
		if(EnmInterCommonUtils.getSelfRole().equals("MASTER")) {
			if (!isKeepAliveStarted) {
				List<Eniq_Role> roleTableList = NodeAssignmentCache.getRoleTableContents();
				for (Eniq_Role eniq : roleTableList) {
					if (!(eniq.getEniq_identifier()).equals(EnmInterCommonUtils.getEngineHostname())) {
						statusMap.put(eniq.getEniq_identifier(), Status.NOT_DETERMINED);
					}
				}
				log.info("Health monitor status map initialized : "+statusMap.toString());
				Timer timer = new Timer();
				timer.scheduleAtFixedRate(new KeepAlive(), DELAY, INTERVAL);
				isKeepAliveStarted = true;
				log.info("Health monitor initialization completed , Keep Alive thread started");
			} else {
				log.info("HealthMonitor already initialized");
			}
		} 		
	}

}
