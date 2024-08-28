package com.ericsson.eniq.flssymlink.fls;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

public class FlsMaxIdMonitorTask implements Runnable {

	private String ossId;
	private String host;
	private FlsTask flsTask;
	private boolean isReset = false;
	private long currentMaxId;
	private Logger log;
	private int noRespAuditCount;
	private NoRespWatchDogState noRespWatchDogState = NoRespWatchDogState.STOPPED;

	private static final int MAX_NO_RESP_MON_AUDIT_COUNT = Main.getNoRespMonTimeout() / Main.getNoRespMonPeriod();
	private static final String DATA_TYPE = "dataType==PM_STATISTICAL";
	private static final String PATH = "///file/v1/files";
	private ScheduledExecutorService service;
	private ScheduledFuture<?> noRespwatchDogFuture;

	public FlsMaxIdMonitorTask(String ossId, String host, FlsTask flsTask, long currentMaxId, boolean isReset,
			Logger log) {
		this.ossId = ossId;
		this.host = host;
		this.flsTask = flsTask;
		this.currentMaxId = currentMaxId;
		this.isReset = isReset;
		log.log(Level.INFO,"Reset flag is set to : " + this.isReset);
		this.log = log;
		service = Executors.newScheduledThreadPool(1);
	}

	public long getCurrentMaxId() {
		return currentMaxId;
	}

	boolean isReset() {
		return isReset;
	}

	void setReset(boolean isReset) {
		this.isReset = isReset;
	}

	@Override
	public void run() {
		MaxIdToken maxIdToken = null;
		String activeProfile = Main.getEnmInterWorking(ossId).getActiveProfile();
		if (isReset) {
			log.info("Reset flag is set , so will not monitor MaxId");
			if (NoRespWatchDogState.STOPPED != noRespWatchDogState) {
				log.info("Stopping No Response watch dog");
				noRespWatchDog(NoRespWatchDogAction.STOP);
			}
			return;
		}
		if (FlsTask.ONHOLD_PROFILE.equalsIgnoreCase(activeProfile)) {
			log.log(Level.INFO, "Fls OnHold, so Max Id monitoring is not done");
			return;
		} else if (FlsTask.NORMAL_PROFILE.equalsIgnoreCase(activeProfile)) {
			RestClientInstance restClientInstance = new RestClientInstance(flsTask);
			try {
				MaxIdTokenArray maxIdTokenArray = getMaxId(restClientInstance);
				List<MaxIdToken> maxIdTokenArrayList = maxIdTokenArray.getFiles();
				if (maxIdTokenArrayList != null && !maxIdTokenArrayList.isEmpty()) {
					maxIdToken = maxIdTokenArrayList.get(0);
				}
				if (isMaxIdValueChange(maxIdToken)) {
					log.info("MaxId value updated to : " + currentMaxId);
				} else {
					log.info("Retaining the current max Id value ");
				}
			} finally {
				restClientInstance.closeSession();
			}
		}
	}

	private boolean isMaxIdValueChange(MaxIdToken maxIdToken) {
		boolean result = false;
		if (maxIdToken != null) {
			log.log(Level.INFO, "Current MaxId is : " + currentMaxId);
			long id = maxIdToken.getId();
			log.log(Level.INFO, "Received MaxId Token : " + id);
			if (currentMaxId == id) {
				log.info("No change in maxId");
			} else if (currentMaxId > id) {
				log.log(Level.WARNING, "MaxId received is less than current MaxId, " + "reset of MaxId will be done");
				resetMaxId();
				result = true;
			} else {
				currentMaxId = id;
				result = true;
			}
		} else {
			log.log(Level.INFO, "MaxId token is null");
		}
		return result;
	}
	
	private void resetMaxId() {
		isReset = true;
		currentMaxId = 0;
	}

	private MaxIdTokenArray getMaxId(RestClientInstance restClientInstance) {
		MaxIdTokenArray maxIdTokenArray = null;
		try {
			Client client = restClientInstance.getClient(flsTask.getCache(), log);
			if (restClientInstance.getSessionCheck()) {
				log.info("Sending the Query to Monitor Max Id");
				WebTarget webTarget = client.target(host).path(PATH).queryParam("filter", "(" + DATA_TYPE + ")")
						.queryParam("select", "id").queryParam("limit", 1).queryParam("offset", 0)
						.queryParam("orderBy", "id%20desc");
				Response response = webTarget.request("application/hal+json").get();
				log.log(Level.FINE, "Query to monitor maxId : " + webTarget);
				log.log(Level.INFO, "MaxIdTokenArray response: " + response);
				log.log(Level.INFO, "response status : " + response.getStatus());
				if (response.getStatus() == 200) {
					maxIdTokenArray = response.readEntity(MaxIdTokenArray.class);
				}
			} else {
				log.log(Level.WARNING, "Not able to get session to monitor Max id");
			}
		} catch (ProcessingException pe) {
			log.log(Level.WARNING, "Error processing the response for MaxId query :" + pe.getMessage());
		} catch (IllegalStateException ise) {
			log.log(Level.WARNING, "Encountered IllegalStateException on sending MaxId query :" + ise.getMessage());
		} catch (IOException ie) {
			log.log(Level.WARNING, "IOException when trying to get MaxId:" + ie.getMessage());
		} catch (Exception e) {
			log.log(Level.WARNING,"Unknown error : Not able to get MaxId:", e);
		}
		if (maxIdTokenArray == null) {
			if (NoRespWatchDogState.STARTED != noRespWatchDogState) {
				noRespWatchDog(NoRespWatchDogAction.START);
			}
			return new MaxIdTokenArray();
		} else {
			if (NoRespWatchDogState.STOPPED != noRespWatchDogState) {
				noRespWatchDog(NoRespWatchDogAction.STOP);
			}
			return maxIdTokenArray;
		}

	}

	enum NoRespWatchDogAction {
		START, STOP
	}

	enum NoRespWatchDogState {
		STARTED, STOPPED
	}

	private void noRespWatchDog(NoRespWatchDogAction action) {
		switch (action) {
		case START:
			startNoRespWatchDog();
			break;
		case STOP:
			stopNoRespWatchDog();
			break;
		default:
			log.log(Level.INFO, "Not a valid state : " + action);
		}
	}

	private void stopNoRespWatchDog() {
		log.log(Level.INFO, "No Response Watchdog Stop invoked");
		noRespwatchDogFuture.cancel(false);
		noRespWatchDogState = NoRespWatchDogState.STOPPED;
		log.log(Level.INFO, "No Response Watchdog Stopped");
	}

	private void startNoRespWatchDog() {
		log.log(Level.INFO, "No Response Watchdog Start invoked");
		noRespAuditCount = 0;
		Runnable monTask = () -> {
			log.log(Level.INFO, "No Response Watchdog Running");
			if (isReset) {
				return;
			}
			if (noRespAuditCount >= MAX_NO_RESP_MON_AUDIT_COUNT) {
				log.log(Level.INFO, "No Response Watchdog : Timeout reached ,"
						+ "Setting Reset flag to true ");
				resetMaxId();
				noRespAuditCount = 0;
			} else {
				log.log(Level.INFO,
						"No Response Watchdog audit : "
								+ (Main.getNoRespMonTimeout() - noRespAuditCount * Main.getNoRespMonPeriod())
								+ " minutes to timeout");
				noRespAuditCount++;
			}
		};
		noRespwatchDogFuture = service.scheduleAtFixedRate(monTask, 0, Main.getNoRespMonPeriod(), TimeUnit.MINUTES);
		noRespWatchDogState = NoRespWatchDogState.STARTED;
		log.log(Level.INFO, "No Response Watchdog Started");
	}

	String getHost() {
		return host;
	}

	void setHost(String host) {
		this.host = host;
	}

	void setCurrentMaxId(long currentMaxId) {
		this.currentMaxId = currentMaxId;
	}

}
