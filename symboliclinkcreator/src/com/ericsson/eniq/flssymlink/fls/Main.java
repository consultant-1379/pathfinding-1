package com.ericsson.eniq.flssymlink.fls;

import static com.ericsson.eniq.common.lwp.LwpFailureCause.UNKNOWN;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.common.lwp.LwpException;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.EnmInterworking;
import com.ericsson.eniq.enminterworking.automaticNAT.HealthMonitor;
import com.ericsson.eniq.enminterworking.automaticNAT.NodeAssignmentCache;
import com.ericsson.eniq.flssymlink.StaticProperties;
import com.ericsson.eniq.flssymlink.symlink.DirectoryConfigurationFileParser;

import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

public class Main {

	private static Main main = null;
	private static String eniqName = null;
	private static String timeFormat = null;
	private static int pm_queue_capacity;
	private static int topo_queue_capacity;
	private static int pm_queue_capacity_multi;
	private static int topo_queue_capacity_multi;
	private static int threadPoolCorePoolSizeStatic;
	private static int threadPoolMaxPoolSizeStatic;
	private static int pmThreadPoolCorePoolSizeStatic;
	private static int pmThreadPoolMaxPoolSizeStatic;
	private static int topoThreadPoolCorePoolSizeStatic;
	private static int topoThreadPoolMaxPoolSizeStatic;
	private static int natThreadPoolCorePoolSizeStatic;
	private static int natThreadPoolMaxPoolSizeStatic;
	private static int pmQueryIntervalStatic;
	private static int pmQueryStartDelayStatic;
	private static int topologyQueryIntervalStatic;
	private static int topologyQueryStartDelayStatic;
	private static int enmSupportMax;
	private static boolean isBulkcmInstalled = false;
	private static boolean isFlsEnabled = false;
	private static EnmInterworking enmInter;
	private static final String CONF_DIR = "CONF_DIR";
	private static final String CONF_DIR_DEFAULT = "/eniq/sw/conf";
	private static final String ETLCPROPERTIES_FILE = "ETLCServer.properties";
	private static int flsMaxIdMonitorStatic;
	private static int noRespMonTimeout;
	private static int noRespMonPeriod;
	
	private static final File ETLC_PROPERTIES = new File(System.getProperty(CONF_DIR, CONF_DIR_DEFAULT), ETLCPROPERTIES_FILE);
	private static Properties etlcProperties = null;
			
	private static ThreadPoolExecutor threadPoolForFlsTask;
		
	// To store FLS query responses
	private static LinkedBlockingQueue<Runnable> pmQueue;
	private static LinkedBlockingQueue<Runnable> topologyQueue;
	
				
	// to store ENM Server Details
	ENMServerDetails cache;
	
	public static LinkedBlockingQueue<Runnable> assignNodesQueue;
	
	//Stores to facilitate handling of Multiple ENMs
	private static HashMap<String, ENMServerDetails> multiEnmCache = new HashMap<>();
	private static HashMap<String, FlsTask> flsTaskMap = new HashMap<>();
	private static HashMap<String, String> hostToOssIdMap = new HashMap<>();
	private static HashMap<String, FlsQueryPm> flsQueryPmMap = new HashMap<>();
	private static HashMap<String, FlsQueryTopology> flsQueryTopoMap = new HashMap<>();
	private static HashMap<String, EnmInterworking> enmInterWorkingMap = new HashMap<>();
	private static HashMap<String, Object> slcLockMap = new HashMap<>();
	private static HashMap<String, String> ossIdToHostMap = new HashMap<>();
	private static HashMap<String, String> sHostNameToOssIdMap = new HashMap<>();
	private static Logger log = Logger.getLogger("symboliclinkcreator.fls");
	
	public static final String ACTIVE = "ACTIVE";
	public static final String MASTER = "MASTER";
	
	private static final String FLS_RMI_PROCESS_PORT = "FLS_RMI_PROCESS_PORT";
	private static final String FLS_RMI_PROCESS_PORT_DEFAULT = "60004";
	private static final String DEFAULT_FALL_BACK_INFO = "is not in correct format, so falling back to default value : ";
	private static String configErrorMsg = "";

	// Constructor
	public Main() {
			try {
				StaticProperties.reload();
			} catch (IOException e) {
				log.warning("Exception while loading symboliclinkcreator.properties:"+e.getMessage());
			}
			loadProperties();
	}
	

	public static Main getInstance() {

		if (main == null) {
			main = new Main();
		}
		return main;
	}
	
	public static EnmInterworking getEnmInter() {
		return enmInter;
	}
	

	private static void initThreadPools() {
		try{
			int pmQCapacity ;
			int topoQCapacity ;
			int pmCoreSize;
			int pmMaxSize ;
			int topoCoreSize ;
			int topoMaxSize ;
			if (multiEnmCache.size() > 1) {
				//Multiple ENMs are configured.
				pmQCapacity = pm_queue_capacity_multi;
				topoQCapacity = topo_queue_capacity_multi;
				pmCoreSize = pmThreadPoolCorePoolSizeStatic;
				pmMaxSize = pmThreadPoolMaxPoolSizeStatic;
				topoCoreSize = topoThreadPoolCorePoolSizeStatic;
				topoMaxSize = topoThreadPoolMaxPoolSizeStatic;
			} else {
				pmQCapacity = pm_queue_capacity;
				topoQCapacity = topo_queue_capacity;
				pmCoreSize = threadPoolCorePoolSizeStatic;
				pmMaxSize = threadPoolMaxPoolSizeStatic;
				topoCoreSize = threadPoolCorePoolSizeStatic;
				topoMaxSize = threadPoolMaxPoolSizeStatic;
			}
			log.info("values: pmQCapacity = "+pmQCapacity+" topoQCapacity = "+topoQCapacity+
			" pmCoreSize = "+pmCoreSize+" pmMaxSize = "+pmMaxSize+
			" topoCoreSize = "+topoCoreSize+" topoMaxSize = "+ topoMaxSize);
			pmQueue = new LinkedBlockingQueue<Runnable>(pmQCapacity);
			topologyQueue = new LinkedBlockingQueue<Runnable>(topoQCapacity);
			ThreadPoolExecutor threadPoolForTopolgySymlinkCreation = new ThreadPoolExecutor(
					topoCoreSize, topoMaxSize,100001, TimeUnit.MILLISECONDS, topologyQueue);
			threadPoolForTopolgySymlinkCreation.prestartAllCoreThreads();
			ThreadPoolExecutor threadPoolForPmSymlinkCreation = new ThreadPoolExecutor(
					pmCoreSize, pmMaxSize,100001, TimeUnit.MILLISECONDS, pmQueue);
			threadPoolForPmSymlinkCreation.prestartAllCoreThreads();
			threadPoolForFlsTask = new ScheduledThreadPoolExecutor(enmSupportMax);
		}
		catch(Exception e){
			log.log(Level.WARNING,"Exception while initializing thread pools:",e);
		}
	}
	
	public static LinkedBlockingQueue<Runnable> getPmQueue() {
		return pmQueue;
	}

	public static void setPmQueue(LinkedBlockingQueue<Runnable> pmQueue) {
		Main.pmQueue = pmQueue;
	}

	public static LinkedBlockingQueue<Runnable> getTopologyQueue() {
		return topologyQueue;
	}

	public static void setTopologyQueue(LinkedBlockingQueue<Runnable> topologyQueue) {
		Main.topologyQueue = topologyQueue;
	}

	public static HashMap<String, ENMServerDetails> getMultiEnmCache() {
		@SuppressWarnings("unchecked")
		HashMap<String, ENMServerDetails> cloneMap = (HashMap<String, ENMServerDetails>)multiEnmCache.clone();
		cloneMap.putAll(multiEnmCache);
		return cloneMap;
	}
	
	public static Set<String> getOssIdSet() {
		@SuppressWarnings("unchecked")
		HashMap<String, ENMServerDetails> cloneMap = (HashMap<String, ENMServerDetails>)multiEnmCache.clone();
		cloneMap.putAll(multiEnmCache);
		return cloneMap.keySet();
	}
	
		
	public static Set<String> getHostNameSet() {
		@SuppressWarnings("unchecked")
		HashMap<String, String> cloneMap = (HashMap<String, String>)ossIdToHostMap.clone();
		cloneMap.putAll(ossIdToHostMap);
		return new HashSet<String>(cloneMap.values());
	}
	
	public static Map<String, String> getOssIdToHostNameMap() {
		@SuppressWarnings("unchecked")
		HashMap<String, String> cloneMap = (HashMap<String, String>)ossIdToHostMap.clone();
		cloneMap.putAll(ossIdToHostMap);
		return cloneMap;
	}
	
	public static String getHost(String ossId) {
		return multiEnmCache.get(ossId).getHost();
	}
	
	public static String getOssAliasWithShostName(String enmShortHostName) {
		return sHostNameToOssIdMap.get(enmShortHostName);
	}
	

	public static String getEniqName() {
		return eniqName;
	}

	public static String getTimeFormat() {
		return timeFormat;
	}

	public static boolean isBulkcmInstalled() {
		return isBulkcmInstalled;
	}

	public static Map<String, EnmInterworking> getEnmInterWorkingMap() {
		return enmInterWorkingMap;
	}
	
	public static EnmInterworking getEnmInterWorking(String ossId) {
		return enmInterWorkingMap.get(ossId);
	}

	public static void updateEnmInterWorkingMap(String ossId, EnmInterworking enmInter) {
		enmInterWorkingMap.put(ossId, enmInter);
	}

	public static FlsQueryPm getflsQueryPm(String ossId, ENMServerDetails cache,Logger log, LinkedBlockingQueue<Runnable> pmQueue) {
		FlsQueryPm flsQueryPm;
		if ((flsQueryPm = flsQueryPmMap.get(ossId)) != null) {
			return flsQueryPm;
		} else {
			flsQueryPm = new FlsQueryPm(cache, log, pmQueue, ossIdToHostMap.get(ossId) );
			flsQueryPmMap.put(ossId, flsQueryPm);
			return flsQueryPm;
		}
	}
	
	public static FlsQueryTopology getflsQueryTopology(String ossId, ENMServerDetails cache,Logger log, LinkedBlockingQueue<Runnable> pmQueue) {
		FlsQueryTopology flsQueryTopo;
		if ((flsQueryTopo = flsQueryTopoMap.get(ossId)) != null) {
			return flsQueryTopo;
		} else {
			flsQueryTopo = new FlsQueryTopology(cache, log, pmQueue);
			flsQueryTopoMap.put(ossId, flsQueryTopo);
			return flsQueryTopo;
		}
	}
			
	public static String getOssIdForHost(String host) {
		return hostToOssIdMap.get(host);
	}

	public static FlsTask getFlsTask(String ossId) {
		return flsTaskMap.get(ossId);
	}

	public static int getPmQueueCapacity() {
		return pm_queue_capacity;
	}

	public static int getTopoQueueCapacity() {
		return topo_queue_capacity;
	}

	public static int getThreadPoolCorePoolSizeStatic() {
		return threadPoolCorePoolSizeStatic;
	}

	public static int getThreadPoolMaxPoolSizeStatic() {
		return threadPoolMaxPoolSizeStatic;
	}

	public static int getNatThreadPoolCorePoolSizeStatic() {
		return natThreadPoolCorePoolSizeStatic;
	}
	
	public static int getNatThreadPoolMaxPoolSizeStatic() {
		return natThreadPoolMaxPoolSizeStatic;
	}

	public static int getPmQueryIntervalStatic() {
		return pmQueryIntervalStatic;
	}

	public static int getPmQueryStartDelayStatic() {
		return pmQueryStartDelayStatic;
	}

	public static int getTopologyQueryIntervalStatic() {
		return topologyQueryIntervalStatic;
	}

	public static int getTopologyQueryStartDelayStatic() {
		return topologyQueryStartDelayStatic;
	}
	
	public static int getFlsMaxIdMonitorStatic(){
		return flsMaxIdMonitorStatic;
	}


	static int getNoRespMonTimeout() {
		return noRespMonTimeout;
	}

	static int getNoRespMonPeriod() {
		return noRespMonPeriod;
	}

	
	private static void loadProperties() {
		pm_queue_capacity = Integer.parseInt(StaticProperties.getProperty("PM_QUEUE_CAPACITY_SINGLE", "30000"));
		topo_queue_capacity = Integer.parseInt(StaticProperties.getProperty("TOPOLOGY_QUEUE_CAPACITY_SINGLE", "10000"));
		pm_queue_capacity_multi = Integer.parseInt(StaticProperties.getProperty("PM_QUEUE_CAPACITY_MULTI", "45000"));
		topo_queue_capacity_multi = Integer.parseInt(StaticProperties.getProperty("TOPOLOGY_QUEUE_CAPACITY_MULTI", "20000"));
		threadPoolCorePoolSizeStatic = Integer
				.parseInt(StaticProperties.getProperty("SYMBOLICLINK_THREADPOOL_COREPOOLSIZE", "15"));
		threadPoolMaxPoolSizeStatic = Integer
				.parseInt(StaticProperties.getProperty("SYMBOLICLINK_THREADPOOL_MAXPOOLSIZE", "20"));
		topoThreadPoolCorePoolSizeStatic = Integer
				.parseInt(StaticProperties.getProperty("SYMBOLICLINK_TOPO_THREADPOOL_COREPOOLSIZE", "15"));
		topoThreadPoolMaxPoolSizeStatic = Integer
				.parseInt(StaticProperties.getProperty("SYMBOLICLINK_TOPO_THREADPOOL_MAXPOOLSIZE", "20"));
		pmThreadPoolCorePoolSizeStatic = Integer
				.parseInt(StaticProperties.getProperty("SYMBOLICLINK_PM_THREADPOOL_COREPOOLSIZE", "20"));
		pmThreadPoolMaxPoolSizeStatic = Integer
				.parseInt(StaticProperties.getProperty("SYMBOLICLINK_PM_THREADPOOL_MAXPOOLSIZE", "30"));
		natThreadPoolCorePoolSizeStatic = Integer
				.parseInt(StaticProperties.getProperty("NAT_THREADPOOL_COREPOOLSIZE", "3"));
		natThreadPoolMaxPoolSizeStatic = Integer
				.parseInt(StaticProperties.getProperty("NAT_THREAD_POOL_MAXPOOLSIZE", "5"));
		pmQueryIntervalStatic = Integer.parseInt(StaticProperties.getProperty("SYMBOLICLINK_PMQUERY_INTERVAL", "3"));
		pmQueryStartDelayStatic = Integer
				.parseInt(StaticProperties.getProperty("SYMBOLICLINK_PMQUERY_START_DELAY", "1"));
		topologyQueryIntervalStatic = Integer
				.parseInt(StaticProperties.getProperty("SYMBOLICLINK_TOPOLOGYQUERY_INTERVAL", "15"));
		topologyQueryStartDelayStatic = Integer
				.parseInt(StaticProperties.getProperty("SYMBOLICLINK_TOPOLOGYQUERY_START_DELAY", "15"));
		enmSupportMax = Integer.parseInt(StaticProperties.getProperty("ENM_SUPPORT_MAX", "10"));
		try {
			flsMaxIdMonitorStatic = Integer.parseInt(StaticProperties.getProperty("SYMBOLICLINK_FLSMAXIDTHREAD_INTERVAL",
					StaticProperties.DEFAULT_SYMBOLICLINK_FLSMAXIDTHREAD_INTERVAL));
			log.log(Level.INFO, "SYMBOLICLINK_FLSMAXIDTHREAD_INTERVAL : " + flsMaxIdMonitorStatic);
		} catch (NumberFormatException nfe) {
			log.log(Level.WARNING, "the value given for the property SYMBOLICLINK_FLSMAXIDTHREAD_INTERVAL "
					+ DEFAULT_FALL_BACK_INFO
					+ StaticProperties.DEFAULT_SYMBOLICLINK_FLSMAXIDTHREAD_INTERVAL);
			flsMaxIdMonitorStatic = Integer.parseInt(StaticProperties.DEFAULT_SYMBOLICLINK_FLSMAXIDTHREAD_INTERVAL);
		}
		try {
			noRespMonTimeout = Integer.parseInt(StaticProperties.getProperty("NO_RESPONSE_MONITOR_TIMEOUT", 
					StaticProperties.DEFAULT_NO_RESPONSE_MONITOR_TIMEOUT));
			log.log(Level.INFO, "NO_RESPONSE_MONITOR_TIMEOUT : " + noRespMonTimeout);
		} catch (NumberFormatException nfe) {
			log.log(Level.WARNING, "the value given for the property NO_RESPONSE_MONITOR_TIMEOUT "
					+ DEFAULT_FALL_BACK_INFO
					+ StaticProperties.DEFAULT_NO_RESPONSE_MONITOR_TIMEOUT);
			noRespMonTimeout = Integer.parseInt(StaticProperties.DEFAULT_NO_RESPONSE_MONITOR_TIMEOUT);
		}
		try {
			noRespMonPeriod = Integer.parseInt(StaticProperties.getProperty("NO_RESPONSE_MONITOR_PERIOD", 
					StaticProperties.DEFAULT_NO_RESPONSE_MONITOR_PERIOD));
			log.log(Level.INFO, "NO_RESPONSE_MONITOR_PERIOD : " + noRespMonPeriod);
		} catch (NumberFormatException nfe) {
			log.log(Level.WARNING, "the value given for the property NO_RESPONSE_MONITOR_PERIOD "
					+ DEFAULT_FALL_BACK_INFO
					+ StaticProperties.DEFAULT_NO_RESPONSE_MONITOR_PERIOD);
			noRespMonPeriod = Integer.parseInt(StaticProperties.DEFAULT_NO_RESPONSE_MONITOR_PERIOD);
		}
	}
	
	private static void configErrorHandler() {
		ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
		Runnable configErrorNotifier = () -> {
			log.log(Level.SEVERE, "Configuration Invalid : Rectify and Restart FLS");
			log.log(Level.SEVERE, "Configuration error details : " + configErrorMsg);
		};
		service.scheduleAtFixedRate(configErrorNotifier, 0, 5, TimeUnit.MINUTES);
	}
	
	private static void initRmi() {
		try {
			enmInter = new EnmInterworking(Integer.parseInt(getEtlcServerProperties().getProperty(FLS_RMI_PROCESS_PORT,FLS_RMI_PROCESS_PORT_DEFAULT)));
			if (!enmInter.initRMI()) {
				log.severe("RMI Initialisation failed... Exiting");
				System.exit(0);
			} else {
				log.info("RMI initialisation done");
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "RMI Initialization failed exceptionally: ", e);
			System.exit(0);
		}
	}

	// main method
	public static void main(String[] args) throws Exception {
		try {
			log.info("Initializing FLS service!!!");
			//Checking whether Bulk CM techpack is active on the server or not
			main = getInstance();
			isBulkCmInstalled();
			CacheENMServerDetails.init(log);
			checkAndPopulateFlsEnabledEnm(CacheENMServerDetails.getCache());
			if (isFlsEnabled) {
				if(!isValidConfigurations()) {
					log.log(Level.SEVERE, "Configurations are not valid, FLS will not query");
					initRmi();
					configErrorHandler();
					return;
				}
				initThreadPools();
				System.setSecurityManager(new com.ericsson.eniq.enminterworking.ETLCSecurityManager());
				// Initializing and binding RMI methods
				initRmi();
				setEniqName();
				if (args.length == 1) {
					Main.timeFormat = args[0];
				}
				initNATThreadPool();
				String shortHostName = null;
				for (String ossId : multiEnmCache.keySet()){
					ENMServerDetails eCache = multiEnmCache.get(ossId); 
					hostToOssIdMap.put(eCache.getHost(), ossId);
					shortHostName = eCache.getHost().split("\\.")[0];
					ossIdToHostMap.put(ossId, shortHostName);
					sHostNameToOssIdMap.put(shortHostName, ossId);
					FlsTask task = new FlsTask(ossId, shortHostName, eCache);
					threadPoolForFlsTask.execute(task);
					flsTaskMap.put(ossId, task);
					log.info("flsTaskMap after update: "+flsTaskMap.toString());
					log.info("FlsTask triggered for :"+ossId);
					Object lock = new Object();
					slcLockMap.put(ossId, lock);
				}
				log.info("hostToOssIdMap after update: "+hostToOssIdMap.toString());
				try {
					NodeAssignmentCache.checkNoOfServers();
					if (EnmInterCommonUtils.getSelfRole().equals(MASTER)) {
						log.info("Starting health monitor from main");
						HealthMonitor.init();
					}
				} catch (SQLException e) {
					log.warning("Unable to initialize Node Assignment cache:"+e.getMessage());
				}
			} else {
				log.info("This server doesn't have FLS enabled");
			}
		}catch (Exception e) {
			log.log(Level.WARNING,"Failed to initialize FLS : ", e);
		}
		
	}
	
	private static boolean isValidConfigurations() {
		try {
			NodeAssignmentCache.init();
			log.log(Level.INFO, "Successfully parsed NodeTechnologyMapping.properties");
		} catch (Exception e) {
			configErrorMsg = "Not able to parse NodeTechnologyMapping.properties";
			log.log(Level.SEVERE, configErrorMsg, e);
			return false;
		}
		try {
			NodeTypeDataTypeCache.init(log);
			log.log(Level.INFO, "Successfully parsed NodeTypeDataTypeMapping.properties");
		} catch (Exception e) {
			configErrorMsg = "Not able to parse NodeTypeDataTypeMapping.properties";
			log.log(Level.SEVERE, configErrorMsg, e);
			return false;
		}
		if(!DirectoryConfigurationFileParser.getInstance(log).isInitSuccessful()) {
			configErrorMsg = "Not able to parse Eniq.xml";
			log.log(Level.SEVERE, configErrorMsg);
			return false;
		}
		
		return true;
	}
	
	private static Properties getEtlcServerProperties() throws LwpException {
	    if (etlcProperties == null) {
	      try {
	        etlcProperties = new Properties();
	        etlcProperties.load(new FileInputStream(ETLC_PROPERTIES));
	      } catch (FileNotFoundException e) {
	        log.log(Level.WARNING, e.getMessage(), e);
	      } catch (IOException e) {
	        throw new LwpException(e, UNKNOWN);
	      }
	    }
	    return etlcProperties;
	  }
	
	public static Object getSlcLock(String ossId) {
		return slcLockMap.get(ossId);
	}
	
	public static int computeDelayTime(final int execMin) {
		LocalTime time = LocalTime.now();
		int currentMinute = time.getMinute();
		int m = currentMinute%10;
		int q = currentMinute/10;
		int next;
		if (m >= execMin) {
			next = (q+1) > 6 ?(q*10)+execMin : ((q+1)*10)+execMin;
		}else {
			next = (q*10)+execMin;
		}
		return next - LocalTime.now().getMinute() ;
	}

	private static void initNATThreadPool() {
		assignNodesQueue = new LinkedBlockingQueue<>();
		ThreadPoolExecutor threadPoolExecutorforNAT = new ThreadPoolExecutor(Main.getNatThreadPoolCorePoolSizeStatic(),
				Main.getNatThreadPoolMaxPoolSizeStatic(), 3, TimeUnit.MILLISECONDS, assignNodesQueue);
		threadPoolExecutorforNAT.prestartAllCoreThreads();
	}
	
	private static void checkAndPopulateFlsEnabledEnm (Map<String, ENMServerDetails> enmServerDetailsMap) {
		BufferedReader inputFLS = null;
		try {
			File flsConfFile = new File(StaticProperties.getProperty("FLS_CONF_PATH", 
					"/eniq/installation/config/fls_conf"));
			inputFLS = new BufferedReader(new FileReader(flsConfFile));
			String line;
			ENMServerDetails enmServerDetails = null;
			String[] flsEnabledEnm = null;
			String ossId = null;
			while ((line = inputFLS.readLine()) != null) {
				flsEnabledEnm = line.split("\\s+");
				if (flsEnabledEnm.length > 0) {
					//Declare FLServiceEnabled as true even if one valid entry is found in fls_conf file.
					isFlsEnabled = true;
					ossId = flsEnabledEnm[0];
					enmServerDetails = enmServerDetailsMap.get(ossId);
					if(enmServerDetails == null){
						log.warning("FLS cache missing for oss_id :" + ossId+ ". Please check the FLS configuration!!");
					} else {
						multiEnmCache.put(ossId, enmServerDetails);
					}
				} else {
					log.warning("fls_enabled_enm server oss id not found");
				}
			} 
		}catch (Exception e) {
			log.log(Level.WARNING,"Exception while populating multiEnmCache :",e);
		}
		finally {
			if(inputFLS != null) {
				try {
					inputFLS.close();
				} catch (IOException e) {
					log.warning("Exception while closing the fls_conf file :"+e.getMessage());
				}
			}
		}
	}

	public static boolean isFlsEnabled() {
		return isFlsEnabled;
	}
	
	public static boolean isFlsEnabled(String ossId){
		return flsTaskMap.get(ossId) != null;
	}

	
	// to stop main process from FlsStopMain
	public void endProcess() {
		log.info("Shutting Down FLS  process");
		System.exit(0);
	}

	// To get ENIQ name of server
	private static void setEniqName() throws Exception {
		BufferedReader inputFLS = null;
		try{
			if (eniqName == null) {
				File serviceNames = new File(StaticProperties.getProperty("SERVICE_NAMES", 
						"/eniq/installation/config/service_names"));
				inputFLS = new BufferedReader(new FileReader(serviceNames));
				String line = null;
				while ((line = inputFLS.readLine()) != null) {
					if (line.matches(".*::.*::engine")) {
						Pattern pattern = Pattern.compile(".*::(.*)::engine");
						Matcher matcher = pattern.matcher(line);
						if (matcher.matches()) {
							log.fine("Server Engine name : " + matcher.group(1));
							eniqName = matcher.group(1);
						} else {
							log.warning("Not matching the Engine matcher patter .*::(.*)::engine");
						}
					}
				}
			}
		} finally {
			if(inputFLS != null) {
				inputFLS.close();
			}
		}
	}
	
	// To check whether Bulk-CM Techpack installed or not
	private static boolean isBulkCmInstalled() {
		RockFactory dwhrep = null;
		String selectString = null;
		Connection con = null;
		RockResultSet rockResultSet = null;
		try {
			dwhrep = DatabaseConnections.getDwhRepConnection();
			con = dwhrep.getConnection();
			selectString = "select STATUS from TPActivation where TECHPACK_NAME='DC_E_BULK_CM'";
			rockResultSet = dwhrep.setSelectSQL(selectString);
			ResultSet rs = rockResultSet.getResultSet();
			if(rs.next()) {
				String activeString = rs.getString(1);
				if (ACTIVE.equals(activeString)) {
					isBulkcmInstalled = true;
					return true;
				}
			}
		} catch (Exception e) {
			log.warning("Exception while checking for BulkCM : " + e.getMessage());
		}
		finally{
			try{
				if(rockResultSet != null) {
					rockResultSet.close();
				}
				if(con != null){
					con.close();
				}
			}
			catch(Exception e){
				log.warning("Exception while closing dwh_rep connection: " +e.getMessage());
			}
		}
		isBulkcmInstalled = false;
		return false;
	}

		
	public void pmCallbyAdminUi(String ossId, String flsUserStartDateTime){
		FlsTask task = flsTaskMap.get(ossId);
		task.pmCallbyAdminUi(flsUserStartDateTime);
	}
		
	public Logger getLog() {
		return log;
	}

	public void setLog(Logger logg) {
		log = logg;
	}
	
}
