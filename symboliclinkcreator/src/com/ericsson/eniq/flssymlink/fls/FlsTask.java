package com.ericsson.eniq.flssymlink.fls;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import javax.ws.rs.client.Client;

import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.EnmInterworking;
import com.ericsson.eniq.flssymlink.StaticProperties;
import com.ericsson.eniq.flssymlink.fls.NodeTypeDataTypeCache.NodeTypeDataTypeCacheException;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import ssc.rockfactory.RockResultSet;

public class FlsTask implements Runnable{
	
	private boolean flsAdminFlag=false;
	private boolean pmFlag = false;
	private boolean topologyFlag = false;
	private Client client=null;
	private RestClientInstance restClientInstance;
		
	public Map<String,Map<String, Object>> tokenMap = new HashMap<>();
	private String ossId;
	private String shortHostName;
	//to store file creation time for topology files
	private String fileCreationTime;
	//to store id's of files whose symbolic links have already been created
	private ArrayList<Long> ids;
				
	private long beforeTime = System.currentTimeMillis();
	private long afterTime = System.currentTimeMillis();
	
	
	private String flsStartDateTime=getCurrentTime();
	private String flsStartDateTimeAdminUi=getCurrentTime();
	//private String flsStartDateTimeAdminUi=getCurrentTimeTest();
	private Date lastDate = new Date();
	
	private ENMServerDetails cache;
	private MixedNodeCache mCache;
	private FlsMaxIdMonitorTask flsMaxIdMonitor;
	long currentMaxId = 0;
	private boolean isReset = true;
	private boolean isDisableReset = false;
		
	private Logger log;
	
	public static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
	public static final String VALID_DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
	
	public static final String NORMAL_PROFILE = "Normal";
	public static final String ONHOLD_PROFILE = "OnHold";
	public static final String DEFAULT_PROFILE = NORMAL_PROFILE;
	public static final String TIME = "time";
	public static final String ID = "id";
	private static final String COMMA = ",";
	private static final String GRANULARITY_SELECT_QUERY = "select distinct ENIQS_Node_Assignment.NETYPE,"
			+ " ISNULL(NodeTypeGranularity.CONFIGURED_GRANULARITY,NodeTypeGranularity.DEFAULT_GRANULARITY),"
			+ "NodeTypeGranularity.TECHNOLOGY from ENIQS_Node_Assignment INNER JOIN NodeTypeGranularity ON "
			+ "ENIQS_Node_Assignment.NETYPE = NodeTypeGranularity.NODE_TYPE";
	
				
	public FlsTask(String ossId, String shortHostName, ENMServerDetails cache) {
		this.ossId = ossId;
		this.shortHostName = shortHostName;
		this.cache = cache;
		log = Logger.getLogger("symboliclinkcreator.fls_"+ossId);
	}
			
	ENMServerDetails getCache() {
		return cache;
	}

	public long getBefore_time() {
		return beforeTime;
	}

	public void setBefore_time(long before_time) {
		this.beforeTime = before_time;
	}

	public long getAftertime() {
		return afterTime;
	}

	public void setAftertime(long after_time) {
		this.afterTime = after_time;
	}

	public Logger getLog() {
		return log;
	}

	public boolean isFlsAdminFlag() {
		return flsAdminFlag;
	}

	public void setFlsAdminFlag(boolean fls_admin_flag) {
		this.flsAdminFlag = fls_admin_flag;
	}

	public String getFlsStartDateTimeAdminUi() {
		return flsStartDateTimeAdminUi;
	}

	public MixedNodeCache getmCache() {
		return mCache;
	}

	public Date getLastDate() {
		return lastDate;
	}
	
	public void setLastDate(Date date) {
		this.lastDate = date;
	}

	@Override
	public void run() {
		try {
			if(Main.isFlsEnabled()) {
				initializeEnmInterWorking();
				handleUpgrade();
				restorePersistedFiles();
				handleMixedNodeReassignment();
				scheduleTopologyTimertask();
				scheduleFlsMaxIdMonitorTask();
				schedulePmTimerTask();
			} else {
				log.info("This server doesn't have FLS enabled");
			}
		} catch(Exception e) {
			log.log(Level.WARNING,"Failed to initialize FLS : ",e);
		}
	}
	
		
	private void handleMixedNodeReassignment(){
		StringBuffer inCondition = new StringBuffer();
		if ( mCache.getMixedNodesToReassign() != null) {
			for (String mixedNodeType : mCache.getMixedNodesToReassign()) {
				inCondition.append("'"+mixedNodeType+"'");
				inCondition.append(",");
			}
			String inConditionString = inCondition.substring(0, inCondition.lastIndexOf(","));
			String sql = "Select NETYPE,FDN,ENM_HOSTNAME from ENIQS_Node_Assignment where NETYPE IN ("+inConditionString+")";
			RockFactory dwhrep = null;
			RockResultSet rockResultSet = null;
			String fdn = null;
			String neType = null;
			String enmHostName = null;
			String mixedNodeTechnologies = null;
			String nodeName = null;
			try {
				dwhrep = DatabaseConnections.getDwhRepConnection();
				rockResultSet = dwhrep.setSelectSQL(sql);
				ResultSet rs = rockResultSet.getResultSet();
				while (rs.next()) {
					fdn = rs.getString(2);
					nodeName = PmQueueHandler.getNodeNameKey(fdn);
					mixedNodeTechnologies = mCache.getMixedNodeTechnologyType(nodeName, log);
					if (mixedNodeTechnologies != null && !mixedNodeTechnologies.equals("NO_KEY") && !mixedNodeTechnologies.equals("EMPTY_CACHE")) {
						neType = rs.getString(1);
						enmHostName = rs.getString(3);
						log.log(Level.FINEST, "handleMixedNodeReassignment: adding Fdn : "+fdn+" of type : "+neType+" to blocking queue");
						Main.getEnmInter().addingToBlockingQueue(neType, fdn, enmHostName, mixedNodeTechnologies);
					}
				}
			} catch (Exception e) {
				log.log(Level.WARNING,"Exception in handleMixedNodeReassignment method",e);
			}
			finally{
				try{
					if(rockResultSet != null) {
						rockResultSet.close();
					}
					if(dwhrep.getConnection() != null){
						dwhrep.getConnection().close();
					}
				}
				catch(Exception e){
					log.warning("Exception while closing dwh_rep connection: " +e.getMessage());
				}
			}
		}
		
	}
	private void initializeEnmInterWorking() {
		EnmInterworking enmInter = null;
		try {
			enmInter = new EnmInterworking();
		} catch (RemoteException e) {
			log.log(Level.WARNING, "Exception while initializing EnmInterworking", e);
		}
		if (enmInter != null && ossId != null ) {
			enmInter.setActiveProfile(DEFAULT_PROFILE);
			Main.updateEnmInterWorkingMap(ossId, enmInter);
		} else {
			log.warning("Unable to initialize EnmInterworking");
		}
		
	}
	
	private boolean handleUpgrade() {
		File pFile = new File(System.getProperty("CONF_DIR") + File.separator + "Persisted.ser");
		File dFile = new File(System.getProperty("CONF_DIR") + File.separator + "date_fls.txt");
		File mFile = new File(System.getProperty("CONF_DIR") + File.separator + "MixedNodeCachePersisted.ser");
		//Condition is true if only one ENM server configured for FLS.
		boolean success = true;
		if (Main.getOssIdSet().size() == 1) {
			if (pFile.exists()) {
				// This is an upgrade sceanrio, lets rename the persisted file
				if (!pFile.renameTo(new File(System.getProperty("CONF_DIR") + File.separator + "Persisted_"+ossId+".ser"))) {
					success = false;
				}
			}
			if (dFile.exists()) {
				if(!dFile.renameTo(new File(System.getProperty("CONF_DIR") + File.separator +"date_fls_"+ossId+".txt"))) {
					success = false;
				}
			}
			if (mFile.exists()) {
				if(mFile.renameTo(new File(System.getProperty("CONF_DIR") + File.separator + "MixedNodeCachePersisted_"+ossId+".ser"))) {
					success = false;
				}
			}
			
		}
		return success;
	}
		
	private void restorePersistedFiles() {
		
		mCache = new MixedNodeCache(ossId, log);
		mCache.restorePersistedMixedNodeCache(log);
		// Persisted Token restore
		File f = new File(System.getProperty("CONF_DIR") + File.separator + "Persisted_"+ossId+".ser");
		if (f.exists()) {
			try {
				restorePersistedToken();
			} catch(Exception e) {
				log.warning("Exception while trying to restore persisted token :" + e.getMessage());
				setFlags();
			}
		}else{
			try {
				restoreDateFls();
				isReset = false;
			} catch(Exception e) {
				log.warning("Exception while trying to restore date_fls.txt :" + e.getMessage());
				setFlags();
			}
			
		}
	}
				
	private void setFlags() {
		flsAdminFlag=true;
		log.info("fls_admin_flag is :"+flsAdminFlag);
		setRefreshFlags();
	}
	
	private void setRefreshFlags() {
		pmFlag = true;
		topologyFlag = true;
		isReset = false;
	}
	
	private void restorePersistedToken() throws IOException, ClassNotFoundException{
		ObjectInputStream in = null;
		try {
			PersistedToken persistedToken = new PersistedToken();
			FileInputStream fin = new FileInputStream(
					System.getProperty("CONF_DIR") + File.separator + "Persisted_"+ossId+".ser");
			if (fin.getChannel().size() > 0) {
				in = new ObjectInputStream(fin);
				persistedToken = (PersistedToken) in.readObject();
				
				if(persistedToken.getTokenMap().values().toArray()[0] instanceof HashMap){
					log.info("When the new data structure is present");
					tokenMap = persistedToken.getTokenMap();
					
				}else{
					isReset = false;
					log.info("When the old data structure is present");
					fileCreationTime = persistedToken.getFileCreationTime();
					Iterator<Entry<String, Map<String, Object>>> itr = persistedToken.getTokenMap().entrySet().iterator(); 
			          
			        while(itr.hasNext()) 
			        { 
			             Map.Entry<String, Map<String, Object>> entry = itr.next(); 
			             String key = entry.getKey();
			             Object value = entry.getValue();
			             Map<String,Object> insideMap = new HashMap<String, Object>();
			             insideMap.put("id",value);
			             insideMap.put("time", fileCreationTime);
			             tokenMap.put(key,insideMap); 
			        } 

				}
				
				
				fileCreationTime = persistedToken.fileCreationTime;
				ids = persistedToken.ids;
				if (tokenMap != null) {
					if (!tokenMap.isEmpty() && fileCreationTime != null && !ids.isEmpty()){
						log.info("Persisted values after restart is:"
								+ persistedToken.tokenMap.toString() + "," + persistedToken.fileCreationTime + "," + persistedToken.ids);
					} else {
						// code to query fls by user specified date and time
						setFlags();
					}
				} else {
					log.log(Level.WARNING, "restorePersistedToken : the token Map is null");
				}
					
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}
	
	private void restoreDateFls() throws IOException {
		File date_fls=new File(System.getProperty("CONF_DIR") + File.separator +"date_fls_"+ossId+".txt");
		BufferedReader in = null;
		if(date_fls.exists()){
			try {
				log.info("date_fls.txt file exists");
				in = new BufferedReader(new FileReader(date_fls));
				while ((flsStartDateTime = in.readLine()) != null) {
					break;
				}
				if (flsStartDateTime != null) {
					if (!isValidDateFormat(flsStartDateTime)) {
						flsStartDateTime = getCurrentTime();
						log.warning("date_fls_"+ossId+".txt does not contain entry in the valid format, "
								+ "hence FLS is starting with current date and time : "+flsStartDateTime);
					} else {
						log.info("Fls is starting with Date and Time:"+flsStartDateTime );
					}
				} else {
					flsStartDateTime = getCurrentTime();
					log.warning("date_fls_"+ossId+".txt does not contain any entry, "
							+ "hence FLS is starting with current date and time : "+flsStartDateTime);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
		}else{
			flsStartDateTime=getCurrentTime();
			log.info("date_fls_"+ossId+".txt file does not exist");
			log.info("Fls is staring with current date and time:"+flsStartDateTime);
		}
	}
	
	private boolean isValidDateFormat(String input) { 
		boolean result = false;
		SimpleDateFormat format =new SimpleDateFormat(DATE_TIME_PATTERN);
		SimpleDateFormat vFormat =new SimpleDateFormat(VALID_DATE_TIME_PATTERN);
		input = input.replaceAll("\\'T\\'", " ");
		if (input != null ) {
			try {
				Date date = format.parse(input);
				result =  input.equals(format.format(date)) ? true : false ;
				if (result) {
					flsStartDateTime = vFormat.format(getAllowedDate(date)).toString();
				}
				return result;
			} catch (ParseException e) {
				log.warning("Not a valid format for date string : expected format :"+VALID_DATE_TIME_PATTERN);
				return false;
			}
		} else {
			return false;
		}
	}
	
	private Date getAllowedDate (Date date) {
		Calendar minCal = Calendar.getInstance();
		Calendar maxCal = Calendar.getInstance();
		minCal.add(Calendar.DATE, -3);
		Date minAllowedDate = minCal.getTime();
		Date maxAllowedDate = maxCal.getTime();
		if (date.before(minAllowedDate)) {
			return minAllowedDate;
		} else if (date.after(maxAllowedDate)){
			return maxAllowedDate;
		} else {
			return date;
		}
	}
	
	private String getCurrentTime() {
		try {
				Date date = new Date(System.currentTimeMillis());
				SimpleDateFormat ft;
				if(Main.getTimeFormat() !=null){
					ft =new SimpleDateFormat(Main.getTimeFormat());
				} else {
					ft =new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				}
		    	String dateTime=ft.format(date).toString();
				return dateTime;
		} catch(Exception e){
			log.warning("Exception at getCurrentTime method"+e.getMessage());
		}
		return null;
	}
	
	//For testing - to create full load scenario.
	@SuppressWarnings("unused")
	private String getCurrentTimeTest() {
		try {
				Calendar cal = Calendar.getInstance();
				//Current date reduced by 3 days to mimic full load scenario.
				cal.add(Calendar.DATE, -3);
				Date date = cal.getTime();
				SimpleDateFormat ft;
				if(Main.getTimeFormat() !=null){
					ft =new SimpleDateFormat(Main.getTimeFormat());
				} else {
					ft =new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				}
		    	String dateTime=ft.format(date).toString();
				return dateTime;
		} catch(Exception e){
			log.warning("Exception at getCurrentTime method"+e.getMessage());
		}
		return null;
	}
	
	private void scheduleTopologyTimertask() {
		Timer topologyTimer = new Timer();
		int topologyQueryIntervalStatic = Main.getTopologyQueryIntervalStatic();
		final int topologyQueryInterval = topologyQueryIntervalStatic * 60 * 1000;
		final int topologyStartDelay = (topologyQueryIntervalStatic * 60 * 1000)
				- (int) (System.currentTimeMillis() % (topologyQueryIntervalStatic * 60 * 1000));
		log.info("scheduleTopologyTimertask: ossId before initializing timertask :"+ossId);
		
		topologyTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					if (Main.isFlsEnabled()) {
						String activeProfile = Main.getEnmInterWorking(ossId).getActiveProfile();
						if(NORMAL_PROFILE.equals(activeProfile)){
							FlsQueryTopology flsQueryTopology = Main.getflsQueryTopology(ossId, cache, log, 
									Main.getTopologyQueue());
							Main.setTopologyQueue(flsQueryTopology.topologyQueue);
							RestClientInstance restClientInstance = null;
							try{
								restClientInstance = new RestClientInstance(FlsTask.this);
								Client client = restClientInstance.getClient(cache, log);
								if(restClientInstance.getSessionCheck()){
									topologyTimerRun(restClientInstance,client);
								}
							} finally {
								if (restClientInstance != null) {
									restClientInstance.closeSession();
								}
							}
						}
						if(ONHOLD_PROFILE.equals(activeProfile)) {
							log.info("Current FLS Query Status is: OnHold. Will not be sending Topology query to ENM!");
						}
					} else {
						log.finest("Fls_conf file is empty");
					}
				} catch (Exception e) {
					log.warning("Exception occured while querying for Topology!!  ");
				}
			}
		}, topologyStartDelay, topologyQueryInterval);
	}
	
	private int getCurrentFLSMount(String ossId)
	{
		int mountCount=0;
        String cmdtoexecute="df -kh --output=target|grep importdata|grep "+ossId;
        String[] cmd = {"/bin/bash", "-c",cmdtoexecute};
        BufferedReader br=null;
		try
        {
            Process process = Runtime.getRuntime().exec(cmd);
            br=new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            while ((line = br.readLine()) != null) {
                 mountCount++;
                 log.finest("FLS Mount point : "+line);
            }
          
        }
		catch(Exception e)
		{
			log.warning("Error in running df -kh command to get the FLS mount point");
		}
		finally
		{
			try {
				if(br!=null)
				{
					br.close();
				}
			} catch (IOException e) {
				log.warning("Error in closing the bufferedreader "+e);
			}
		}
		
		return mountCount;
		
	}
	
	private int getExpectedFLSMount(String ossId)
	{
		int count=0;
		final String CONNECTDFILE="/eniq/connectd/mount_info/"+ossId+"/fs_mount_list";
		File connectdFile=new File(CONNECTDFILE);
		Path path=Paths.get(connectdFile.getPath());
		try(Stream<String> stream=Files.lines(path);)
	    {
		  count=(int) stream.filter(lineToAdd->!lineToAdd.contains("#")).count();
	    }
      	catch(Exception e)
      	{
      		 log.warning("Error while reading file " + connectdFile + " " +e);
      	}
		return count;
	}
	
	private void checkFLSMount(String ossId) {
        log.finest("Started mount point check..");
        String MOUNTFLAGFILE="/eniq/sw/conf/.Mount_"+ossId+"_flagfile";
        File flagFile=new File(MOUNTFLAGFILE);
		try
        {
			int currentMountCount=getCurrentFLSMount(ossId);  
			int expectedMountCount=getExpectedFLSMount(ossId);
			log.finest("Total number of FLS mount points expected for ossId "+ossId+" is "+expectedMountCount);
            log.finest("Total number of FLS mount points present currently for ossId "+ossId+" is "+currentMountCount);
            String activeProfile = Main.getEnmInterWorking(ossId).getActiveProfile();
            FlsProfileHandler flsProfileHandler=new FlsProfileHandler();
            if(currentMountCount<expectedMountCount)
            { 
            	log.info("FLS Mount points are not available");
            	if(!activeProfile.equals(ONHOLD_PROFILE))
            	{
            		log.finest("Moving FLS from "+activeProfile+" Profile to "+ONHOLD_PROFILE);
            		
            		boolean result=flsProfileHandler.changeProfileWtext(ossId,ONHOLD_PROFILE);
            		if(!result)
            		{
            			log.warning("Failed to change the "+ossId+" profile to "+ONHOLD_PROFILE);
            			return;
            		}
            		try{
            			flagFile.createNewFile();
            		}
            		catch(Exception e)
                	{
                		 log.warning("Error while creating the flag file " + flagFile + " " +e);
                	}
            		
            	}
            	else
            	{
            		log.finest("FLS profile is already "+activeProfile);
            	}
                	
            }
            else
            {
            	log.finest("FLS Mount points are available");
            	if(!activeProfile.equals(NORMAL_PROFILE)&& flagFile.exists())
            	{
            		flagFile.delete();
            		log.info("Moving FLS from "+activeProfile+" Profile to "+NORMAL_PROFILE);
            		boolean result=flsProfileHandler.changeProfileWtext(ossId,NORMAL_PROFILE);
            		if(!result)
            		{
            			log.warning("Failed to change the "+ossId+" profile to "+NORMAL_PROFILE);
            			try{
                			flagFile.createNewFile();
                		}
                		catch(Exception e)
                    	{
                    		 log.warning("Error while creating the flag file " + flagFile + " " +e);
                    	}
            			return;
            		}  
            	}
            	else
            	{
            		log.finest("FLS profile is already "+activeProfile);
            	}
            }
            
        }
        catch (Exception e)
        {
        	log.warning("Mount point check failed for the ossId "+ossId+ " " +e);
        }

	}
	
	private void schedulePmTimerTask() {
		Timer pmTimer = new Timer();
		int pmQueryIntervalStatic = Main.getPmQueryIntervalStatic();
		final int pmQueryInterval = pmQueryIntervalStatic * 60 * 1000;
		final int pmStartDelay = (pmQueryIntervalStatic * 60 * 1000)
				- (int) (System.currentTimeMillis() % (pmQueryIntervalStatic * 60 * 1000));
		log.info("Starting Pm Timer...");
		
		pmTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					if (Main.isFlsEnabled()) {
						checkFLSMount(ossId);
						String activeProfile = Main.getEnmInterWorking(ossId).getActiveProfile();
						if(NORMAL_PROFILE.equals(activeProfile)){
							FlsQueryPm flsQueryPM = Main.getflsQueryPm(ossId, cache, log, Main.getPmQueue());
							Main.setPmQueue(flsQueryPM.pmQueue);
							pmTimerRun();
						}
						if(ONHOLD_PROFILE.equals(activeProfile)) {
							log.info("Current FLS Query Status is: OnHold. Will not be sending PM query to ENM!");
						}
					} else {
						log.finest("Fls_conf file is empty");
					}
				} catch (Exception e) {
					log.log(Level.WARNING,"Exception occured while retrieving client object and querying for PM=" , e);
				}
			}
		}, pmStartDelay, pmQueryInterval);
		//scheduleReadOutTimer();
	}
	
	private void scheduleFlsMaxIdMonitorTask() throws IOException{
		String host = "https://" + cache.getHost();
		flsMaxIdMonitor = new FlsMaxIdMonitorTask(ossId, host, this, currentMaxId, isReset, log);
		int delay = Main.getFlsMaxIdMonitorStatic();
		int initialDelay = Main.computeDelayTime(StaticProperties.DEFAULT_MAX_ID_MONITOR_EXEC_TIME);
		log.log(Level.INFO,"MaxId Monitor will be secheduled to run every "+ delay + " Minutes");
		log.log(Level.INFO,"Intial delay for MaxId Monitor is: " + initialDelay + " Minutes");
		ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
		scheduler.scheduleAtFixedRate(flsMaxIdMonitor, initialDelay, delay, TimeUnit.MINUTES);
		Runtime.getRuntime().addShutdownHook( new Thread() {
		
			@Override
			public void run() {
				scheduler.shutdown();
				try {
					scheduler.awaitTermination(2000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					this.interrupt();
				}
			}
			
		});
		log.info("Sucessfully scheduled MaxId monitor task.");
		String testMode = StaticProperties.getProperty("TEST_MODE","false");
		if (testMode != null && "TRUE".equalsIgnoreCase(testMode.trim())) {
			testMaxIdMonitor();
		}
	}
	
	
	@SuppressWarnings("unused")
	private void scheduleReadOutTimer() {
		Timer readTimeOutTimer = new Timer();
		final int interval = 10000;
		int pmQueryIntervalStatic = Main.getPmQueryIntervalStatic();
		final int startDelay = (pmQueryIntervalStatic * 60 * 1000)
				- (int) (System.currentTimeMillis() % (pmQueryIntervalStatic * 60 * 1000));
		readTimeOutTimer.schedule(new TimerTask(){
			@Override
			public void run(){
				try{
					if (Main.isFlsEnabled()) {
						EnmInterworking eiw = Main.getEnmInterWorking(ossId);
						if (eiw != null) {
							String activeProfile = eiw.getActiveProfile();
							if(NORMAL_PROFILE.equals(activeProfile)){
								if (restClientInstance != null) {
									log.finest("ReadOutTimer running");
									if (restClientInstance.sessionCheck() == true && afterTime < beforeTime){
										long diff = ((System.currentTimeMillis() - beforeTime)/1000);
										if(diff > 30){
											restClientInstance.session();
										}
									}
								} else {
									log.log(Level.FINEST, 
											"scheduleReadOutTimer : RestClient not yet initialized [or] already closed for :"+ossId);
								}
							}
						} else {
							log.log(Level.WARNING, "EnmInterworking for : "+ossId+" is null");
						}
					}else {
						log.finest("Fls_conf file is empty");
					}
				} catch (Exception e) {
					log.log(Level.WARNING, "Exception occurred while running the query read time out timer :",e);
				}
			}
		}, startDelay , interval);
	}
	
	private void topologyTimerRun(RestClientInstance restClientInstance,Client client) {
		try{
			log.finest("topologyTimerRun: ossId before creating queryTopology :"+ossId);
			FlsQueryTopology flsQueryTopology = Main.getflsQueryTopology(ossId, cache, log,	Main.getTopologyQueue());
			String postTime,preTime;
			ArrayList<Long> idList = new ArrayList<Long>();
			if (!topologyFlag) {
				if (fileCreationTime != null && !ids.isEmpty()) {
					// topology fls request when last file creation time is persisted along with the ids 
					postTime = preTime = fileCreationTime;
					idList = ids;
					log.info("File Creation time for  Topology before querying: " + preTime);
					try{
						Topology topology = flsQueryTopology.queryTopology("TOPOLOGY", preTime, idList , null, 1,restClientInstance,client);
						postTime = topology.getTime();
						idList = topology.getIds();
					}
					catch(Exception e){
						log.warning("Exception while calling FlsQueryTopology "+e.getMessage());	
					}
					// storing last time stamp and list of ids for persistence
					log.finest("File Creation Time for topology after querying: " + postTime);
					if (postTime != null) {
						if(!postTime.equals(preTime)) {
							fileCreationTime = postTime;
							ids = idList;
						} else {
							log.info("Topology request has not received any new file entries!!");
						}
					}
				} 
				else {
					// topology fls request for the first time
					postTime=preTime = null;
					log.info("Topology request for the first time");
					try{
						Topology topology = flsQueryTopology.queryTopology("TOPOLOGY", preTime, idList , flsStartDateTime, 2,restClientInstance,client);
						postTime = topology.getTime();
						idList = topology.getIds();
					}
					catch(Exception e){
						log.warning("Exception while calling FlsQueryTopology "+e.getMessage());	
					}
					// storing last time stamp and list of ids for persistence
					log.finest("File Creation Time for topology after querying: " + postTime);
					if (postTime != null) {
						if(!postTime.equals(preTime)) {
							fileCreationTime = postTime;
							ids = idList;
						} else {
							log.info("Topology request has not received any new file entries!!");
						}
					}
				}
			} 
			else {
				topologyFlag=false;
				log.info("Topology request by User inputed Date and Time : "+flsStartDateTimeAdminUi);
				postTime=preTime=flsStartDateTimeAdminUi;
				
				try{
					Topology topology=flsQueryTopology.queryTopology("TOPOLOGY",preTime, idList , null,2,restClientInstance,client);
					postTime = topology.getTime();
					idList = topology.getIds();
				}
				catch(Exception e){
					log.log(Level.WARNING,"Exception while calling FlsQueryTopology: ",e);	
				}
				//storing last time stamp and list of ids for persistence
				log.finest("File Creation Time for topology after querying with current Date and Time: "+postTime);
				if (postTime != null) {
					if(!postTime.equals(preTime)) {
						fileCreationTime = postTime;
						ids = idList;
					} else {
						log.info("Topology request has not received any new file entries!!");
					}
				}
			}
			String role = EnmInterCommonUtils.getSelfRole();
			log.finest("Role= "+role);
		}
		catch(Exception e){
			log.warning("Exception at topology Quering: "+e);
		}
	}
	
	private void pmTimerRun() {
		FlsQueryPm flsQueryPm = Main.getflsQueryPm(ossId, cache, log, Main.getPmQueue());
		// to store list of node types for subscription
		Main.setPmQueue(flsQueryPm.pmQueue);
		ArrayList<String> nodeTypeList = getNodeList();
		// to do subscription for every node type
		isReset = flsMaxIdMonitor.isReset();
		if (isReset) {
			log.log(Level.INFO, "Reset flag is set to true");
			isDisableReset = false;
		}
		if (nodeTypeList.isEmpty()) {
			log.info("Either Node assignment table is empty or REPDB is offline");
			return;
		}
		Map<String, Set<String>> nodeGranularityMap = new HashMap<>();
		try {
			nodeGranularityMap = getNodeTypeGranularities(log);
		} catch (NodeTypeDataTypeCacheException e) {
			log.log(Level.WARNING, "Error getting granularities, will fall back to default granularity for all node types");
		}
		
		try {
			restClientInstance = new RestClientInstance(FlsTask.this);
			client = restClientInstance.getClient(cache, log);
			if (restClientInstance.getSessionCheck()) {
				log.log(Level.INFO, "ids before querying : " + tokenMap);
				for (String name : nodeTypeList) {
					if (nodeGranularityMap.isEmpty()) {
						handleDefaultGran(flsQueryPm, name);
					} else {
						handleNonDefaultGran(flsQueryPm, name, nodeGranularityMap.get(name));
					}
				}
				if (pmFlag) {
					pmFlag = false;
				}
				if (isReset && isDisableReset) {
					log.log(Level.INFO, "Reverting the reset flag");
					flsMaxIdMonitor.setReset(false);
				}
			}
		} catch (IOException e) {
			log.log(Level.WARNING, "Execption while sending pm query : " + e.getMessage());
		} finally {
			if (restClientInstance != null) {
				restClientInstance.closeSession();
			}
		}

		savePersistedToken();
	}
	
	private void sendPmQueries(FlsQueryPm flsQueryPm, String name, Set<String> dataTypes) {
		dataTypes.forEach(dataType -> {
			int repeatCount = NodeTypeDataTypeCache.getRepeatCount(name, dataType, log);
			for (int i = 1; i <= repeatCount; i++) {
				log.log(Level.INFO, "Query No = " + i + " : NodeType = " + name);
				sendPmQuery(flsQueryPm, name, dataType);
			}
		});
	}
	
	private void handleDefaultGran(FlsQueryPm flsQueryPm, String name) {
		Set<String> granularities = new HashSet<>();
		granularities.add(NodeTypeDataTypeCache.FIFTEEN_MIN);
		sendPmQueries(flsQueryPm, name, NodeTypeDataTypeCache.getDataTypes(name, granularities));
	}
	
	private void handleNonDefaultGran(FlsQueryPm flsQueryPm, String name, Set<String> granularities) {
		if (granularities != null) {
			sendPmQueries(flsQueryPm, name, NodeTypeDataTypeCache.getDataTypes(name, granularities));
		} else {
			log.log(Level.INFO, "Granularity information is not present for the node type : "+name);
		}
	}

	private void savePersistedToken() {
		try(FileOutputStream fOut = new FileOutputStream(
				System.getProperty("CONF_DIR") + File.separator + "Persisted_"+ossId+".ser");
				ObjectOutputStream objOut = new ObjectOutputStream(fOut)) {
			PersistedToken persistedToken = new PersistedToken();
			persistedToken.setTokenMap(tokenMap);
			persistedToken.setFileCreationTime(fileCreationTime);
			persistedToken.setIds(ids);
			log.log(Level.INFO, "Persisting token map value : " + persistedToken.getTokenMap() + "," + persistedToken.getFileCreationTime() + "," + persistedToken.getIds());
			objOut.writeObject(persistedToken);
			objOut.flush();
		} catch (IOException e) {
			log.log(Level.WARNING, "Encountered Exception while trying to save persisted token : "+e.getMessage());
		}
	}
	
	
	
	private Map<String, Object> sendPmQuery(FlsQueryPm flsQueryPm, String nodeType,	String dataType) {
		Map<String, Object> resultMap;
		long preId = -1;
		String dateTime = flsStartDateTime;
		FlsQueryPm.QueryType queryType;
		if (pmFlag) {
			if (!flsAdminFlag) {
				log.log(Level.INFO, "Querying with date and time from user input");
				dateTime = flsStartDateTimeAdminUi;
			}
			queryType = FlsQueryPm.QueryType.WITH_TIME;
		} else if (isReset) {
			Map<String, Object> nodeMap = getPmNodeMap(nodeType,dataType);
			String time = (String)nodeMap.get(TIME);
			if (time != null) {
				dateTime = time;
			}
			queryType = FlsQueryPm.QueryType.WITH_TIME;
		} else {
			Map<String, Object> nodeMap = getPmNodeMap(nodeType,dataType);
			Long id = (Long) nodeMap.get(ID);
			if (id != null) {
				preId = id;
				dateTime = null;
				queryType = FlsQueryPm.QueryType.WITH_ID;
			} else {
				queryType = FlsQueryPm.QueryType.WITH_TIME;
			}
		}
		String nodeTypeDataType = NodeTypeDataTypeCache.getNodeTypeDataTypeKey(nodeType, dataType);
		log.log(Level.INFO, "id for : "+ nodeTypeDataType + " before querying is : " + preId);
		resultMap = flsQueryPm.queryPM(nodeType, preId, dateTime, queryType, restClientInstance, client, dataType);
		long postId = resultMap.get(ID) != null ? (Long) resultMap.get(ID) : preId;
		Main.setPmQueue(flsQueryPm.pmQueue);
		log.log(Level.INFO, "id for : "+ nodeTypeDataType + " after querying is : " + postId);
		if (postId != preId) {
			tokenMap.put(nodeTypeDataType, resultMap);
			if (!isDisableReset) {
				isDisableReset = true;
			}
		}
		return resultMap;
	}
	
	
	private Map<String, Object> getPmNodeMap(String nodeType, String dataType) {
		String key = NodeTypeDataTypeCache.getNodeTypeDataTypeKey(nodeType, dataType);
		Map<String, Object> nodeMap = getPmNodeMap(key);
		if(nodeMap.isEmpty()) {
			nodeMap = getPmNodeMap(nodeType);
		}
		return nodeMap;
	}
	
	private Map<String, Object> getPmNodeMap(String nodeType) {
		Map<String, Object> nodeMap = new HashMap<>();
		if(tokenMap != null) {
			nodeMap = tokenMap.get(nodeType);
			if (nodeMap == null) {
				nodeMap = new HashMap<>();
			}
		}
		return nodeMap;
	}
	
	private ArrayList<String> getNodeList(){
		RockFactory dwhrep = null;
		String selectString = null;
		RockResultSet rockResultSet = null;
		ArrayList<String> returnNodeList = new ArrayList<>();
		try {
			dwhrep = DatabaseConnections.getDwhRepConnection();
			selectString = "select distinct NETYPE from ENIQS_Node_Assignment where ( (ENIQ_IDENTIFIER='" + Main.getEniqName()
					+ "' or ENIQ_IDENTIFIER like '%" + Main.getEniqName() + "%') AND ENM_HOSTNAME='"+shortHostName+"') and NETYPE != ''";
			rockResultSet = dwhrep.setSelectSQL(selectString);
			ResultSet rs = rockResultSet.getResultSet();
			log.finest("Querying for NodeType ");
			if (rockResultSet.getResultSet().isBeforeFirst()) {
				while (rs.next()) {
					String node_type = rs.getString(1);
					returnNodeList.add(node_type);
					log.info("Netype value : " + node_type);
				}
			} else {
				log.info(
						"ENIQS_Node_Assignment table is empty or NETYPE column is empty. Will not be querying for PM Files!!");
			}
			
		} catch (Exception e) {
			log.warning("Exception in getNodeList method" + e.getMessage());
		}
		finally{
			try{
				if(rockResultSet != null) {
					rockResultSet.close();
				}
				if(dwhrep != null && dwhrep.getConnection() != null){
					dwhrep.getConnection().close();
				}
			}
			catch(Exception e){
				log.warning("Exception while closing dwh_rep connection: " +e.getMessage());
			}

		}
		return returnNodeList;
	}
	
	public Map<String, Set<String>> getNodeTypeGranularities(Logger log) throws NodeTypeDataTypeCacheException {
		RockFactory dwhrep = null;
		RockResultSet rockResultSet = null;
		Map<String, Set<String>> nodeGranularityMap = new HashMap<>();
		try {
			dwhrep = DatabaseConnections.getDwhRepConnection();
			rockResultSet = dwhrep.setSelectSQL(GRANULARITY_SELECT_QUERY);
			ResultSet rs = rockResultSet.getResultSet();
			log.finest("Querying for granularity for each NodeType");
			if (rockResultSet.getResultSet().isBeforeFirst()) {
				while (rs.next()) {
					String nodeTypeName = rs.getString(1);
					String granularities = rs.getString(2);
					extractGranularities(nodeTypeName, granularities, nodeGranularityMap);
				}
			} else {
				log.info("No Data Retrieved for Node Type granularity mapping");
			}
		} catch (SQLException | RockException e) {
			log.log(Level.WARNING, "Not able to read the granularity information from db");
			throw new NodeTypeDataTypeCacheException(e);
		} finally {
			try {
				if (rockResultSet != null) {
					rockResultSet.close();
				}
				if (dwhrep != null && dwhrep.getConnection() != null) {
					dwhrep.getConnection().close();
				}
			} catch (Exception e) {
				log.warning("Exception while closing dwh_rep connection: " + e.getMessage());
			}
		}
		return nodeGranularityMap;
	}
	
	private void extractGranularities(String nodeTypeName, String granularities, 
			Map<String, Set<String>> nodeGranularityMap) {
		if (granularities == null || granularities.isEmpty()) {
			return;
		}
		Set<String> granularityList = nodeGranularityMap.get(nodeTypeName);
		if (granularityList == null) {
			granularityList = new HashSet<>();
			nodeGranularityMap.put(nodeTypeName, granularityList);
		}
		String[] tokens = granularities.split(COMMA);
		for (int i = 0 ; i < tokens.length ; i++) {
			granularityList.add(tokens[i].trim());
		}
	}
	
	
	public void pmCallbyAdminUi (String flsUserStartDateTime) {
		flsStartDateTimeAdminUi = EnmInterCommonUtils.formatDate(flsUserStartDateTime);
		flsAdminFlag = false;
		setRefreshFlags();
	}
	
	
	
	
	/**
	 * only for testing
	 */
	private ScheduledFuture<?> testFuture = null;
	private int testCount = 0;
	private void testMaxIdMonitor() {
		//Testing MaxId Monitor
		log.log(Level.INFO, "Testing initated for maxId monitor");
		ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
		int maxIdInterval = Main.getFlsMaxIdMonitorStatic();
		final int revertCount = (Main.getNoRespMonTimeout()/maxIdInterval)+1;
		
		Runnable task = () -> {
			log.log(Level.INFO, "Max Id monitor test thread running");
			if (testCount == 5) {
				//testing max id change
				flsMaxIdMonitor.setCurrentMaxId(flsMaxIdMonitor.getCurrentMaxId() + 20000);
				log.log(Level.INFO, "max id incremented to " 
				+ flsMaxIdMonitor.getCurrentMaxId() +" to test the restore scenario");
			}
			//testing no response monitor
			if (testCount == 8) {
				flsMaxIdMonitor.setHost("https://1.1.1.1");
				log.log(Level.INFO, "host name in max id monitor modified to 1.1.1.1 "
						+ "to  test the no response scenario");
			}
			
			if (testCount > (8+revertCount)) {
				flsMaxIdMonitor.setHost("https://" + cache.getHost());
				log.log(Level.INFO,"host name reverted back ");
				log.log(Level.INFO,"Testing complete");
				stopTesting(testFuture);
				
			}
			testCount++;
		};
		testFuture = service.scheduleAtFixedRate(task, 1, maxIdInterval, TimeUnit.MINUTES);
	}
	
	
	private void stopTesting(ScheduledFuture<?> future) {
		future.cancel(true);
		log.log(Level.INFO,"Max Id monitor test thread stopped");
	}
	

}
