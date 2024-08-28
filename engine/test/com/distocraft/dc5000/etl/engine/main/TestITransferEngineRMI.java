package com.distocraft.dc5000.etl.engine.main;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.system.SetStatusTO;

/**
 * 
 * @author ejarsok
 * 
 */

public class TestITransferEngineRMI extends UnicastRemoteObject implements ITransferEngineRMI {

  private boolean throwsE = false;
  
  private boolean isCacheRefreshed = false;
  
  private String DBLookup;
  
  private String activeExecutionProfile;
  
  public TestITransferEngineRMI(final boolean ee) throws RemoteException {
    super();
    this.throwsE = ee;
    Registry registry = LocateRegistry.getRegistry(1200);
    if(registry != null){
      try {
        registry.list();
      } catch (RemoteException e) {
        registry = null;
      }
    }

    if(registry == null){
      LocateRegistry.createRegistry(1200);
    }
    try {
      Naming.rebind("//localhost:1200/TransferEngine", this);
      // System.out.println("Server registered to already running RMI naming");
    } catch (Throwable e) {

      try {

        LocateRegistry.createRegistry(1200);
        // System.out.println("RMI-Registry started on port " + 1200);

        Naming.bind("//localhost:1200/TransferEngine", this);
        // System.out.println("Server registered to started RMI naming");

      } catch (Exception exception) {
        exception.printStackTrace();
        // System.out.println("Unable to initialize LocateRegistry" +
        // exception);

      }
    }
  }

  @Override
  public void activateScheduler() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }

  }

  @Override
  public void activateSetInPriorityQueue(Long ID) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void addWorkerToQueue(String name, String type, Object wobj) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void changeAggregationStatus(String status, String aggregation, long datadate) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public boolean changeSetPriorityInPriorityQueue(Long ID, long priority) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return false;
  }
  

  @Override
  public boolean changeSetTimeLimitInPriorityQueue(final Long ID, final long queueTimeLimit) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return false;
  }
  

  public void execute(RockFactory rockFact, String collectionSetName, String collectionName) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  	@Override
	  public boolean isCacheRefreshed() throws RemoteException {
	    return (this.isCacheRefreshed);
	  }
  
  @Override
  public void execute(String url, String userName, String password, String dbDriverName, String collectionSetName,
      String collectionName) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }


  public void execute(String url, String userName, String password, String dbDriverName, String collectionSetName,
      String collectionName, String ScheduleInfo) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void execute(String collectionSetName, String collectionName, String ScheduleInfo) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public String executeAndWait(String collectionSetName, String collectionName, String ScheduleInfo)
      throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public void fastGracefulShutdown() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void forceShutdown() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public Set getAllActiveSetTypesInExecutionProfiles() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    HashSet hs = new HashSet();
    hs.add("type");
    hs.add("type0");
    
    return hs;
  }

  @Override
  public Set getAllRunningExecutionSlotWorkers() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public List getExecutedSets() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public List getFailedSets() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public String getPluginConstructorParameterInfo(String pluginName) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public String getPluginConstructorParameters(String pluginName) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public String getPluginMethodParameters(String pluginName, String methodName) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public String[] getPluginMethods(String pluginName, boolean isGetSetMethods, boolean isGetGetMethods)
      throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public String[] getPluginNames() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public List getQueuedSets() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public List getRunningSets() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }
  
  @Override
  public List getRunningSets(final List<String> techPackNames) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public void giveEngineCommand(String com) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }

  }

    @Override
    public void disableTechpack(String techpackName) throws RemoteException {
    }

    @Override
    public void disableSet(String techpackName, String setName) throws RemoteException {
    }

    @Override
    public void disableAction(String techpackName, String setName, Integer actionOrder) throws RemoteException {
    }

    @Override
    public void enableTechpack(String techpackName) throws RemoteException {
    }

    @Override
    public void enableSet(String techpackName, String setName) throws RemoteException {
    }

    @Override
    public void enableAction(String techpackName, String setName, Integer actionNumber) throws RemoteException {
    }

    @Override
    public ArrayList<String> showDisabledSets() throws RemoteException {
        return null;
    }

    @Override
    public void holdExecutionSlot(int ExecutionSlotNumber) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void holdPriorityQueue() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void holdSetInPriorityQueue(Long ID) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public boolean isInitialized() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return false;
  }

  @Override
  public boolean isSetRunning(Long techpackID, Long setID) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return false;
  }

  @Override
  public void lockExecutionprofile() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public List loggingStatus() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public void reaggregate(String aggregation, long datadate) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void reloadDBLookups(String tableName) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    DBLookup = tableName;
  }

  @Override
  public void reloadExecutionProfiles() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void reloadLogging() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void reloadProperties() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }
  
  @Override
  public void refreshCache() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }
  
  @Override
  public void reloadAggregationCache() throws RemoteException {
	    // TODO Auto-generated method stub
	    if(throwsE) {
        throw new RemoteException();
      }
  }

  @Override
  public void reloadAlarmConfigCache() throws RemoteException {
    if(throwsE)
      throw new RemoteException();
  }
  
  @Override
  public void reloadTransformations() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public boolean removeSetFromPriorityQueue(Long ID) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return false;
  }

  @Override
  public void restartExecutionSlot(int ExecutionSlotNumber) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void restartPriorityQueue() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void clearCountingManagementCache(String storageId) throws RemoteException {
    // TODO Auto-generated method stub
    if (throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public boolean setActiveExecutionProfile(String profileName, boolean resetConMon) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    if(profileName != null && profileName.length() > 0) {
      activeExecutionProfile = profileName;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean setActiveExecutionProfile(String profileName) throws RemoteException {
    return setActiveExecutionProfile(profileName, true);
  }

  @Override
  public boolean setActiveExecutionProfile(String profileName, String messageText) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return false;
  }

  @Override
  public boolean setAndWaitActiveExecutionProfile(String profileName) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return false;
  }

  @Override
  public void slowGracefulShutdown() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public List status() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
    
    return null;
  }

  @Override
  public void unLockExecutionprofile() throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void updateTransformation(String tpName) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }

  @Override
  public void writeSQLLoadFile(String fileContents, String fileName) throws RemoteException {
    // TODO Auto-generated method stub
    if(throwsE) {
      throw new RemoteException();
    }
  }
  
  public String getDBLookup() {
    return DBLookup;
  }
  
  public String getActiveExecutionProfile() {
    return activeExecutionProfile;
  }

@Override
public SetStatusTO executeSetViaSetManager(String collectionSetName,
		String collectionName, String ScheduleInfo, Properties props)
		throws RemoteException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public String executeWithSetListener(String collectionSetName,
		String collectionName, String ScheduleInfo) throws RemoteException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public SetStatusTO getSetStatusViaSetManager(String collectionSetName,
		String collectionName, int beginIndex, int count)
		throws RemoteException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public SetStatusTO getStatusEventsWithId(String statusListenerId,
		int beginIndex, int count) throws RemoteException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public List<Map<String, String>> slotInfo() throws RemoteException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void releaseSet(long setID) {
	
}

@Override
public void releaseSets(String tp, String setType) {
	
}

@Override
public List<String> getMeasurementTypesForRestore(String techPackName, String regex) throws RemoteException {
  // TODO Auto-generated method stub
  if(throwsE) {
    throw new RemoteException();
  }
  return null;
}

@Override
public boolean isTechPackEnabled(String techPackName, String techPackType) throws RemoteException {
  // TODO Auto-generated method stub
  if(throwsE) {
    throw new RemoteException();
  }
  return false;
}

@Override
public void restore(String techPackName, List<String> measureMentTypes, String fromRestoreDate, String toRestoreDate)
    throws RemoteException {
  // TODO Auto-generated method stub
  if(throwsE) {
    throw new RemoteException();
  }
  
}

  @Override
  public List<String> getTableNamesForRawEvents(final String viewName, final Timestamp startTime,
      final Timestamp endTime) throws java.rmi.RemoteException {
    List<String> viewNames = new ArrayList<String>();
    viewNames.add(viewName + "_10");
    return viewNames;
  }

  @Override
  public void manualCountReAgg(final String techPackName, final Timestamp minTimestamp, final Timestamp maxTimestamp,
      final String intervalName, final boolean isScheduled) throws RemoteException {

  }

  @Override
  public boolean isIntervalNameSupported(final String intervalName) {
    return false;
  }

  @Override
  public long getOldestReAggTimeInMs(final String techPackName) {
    return 0;
  }

  @Override
  public Map<String, String> serviceNodeConnectionInfo() throws RemoteException {
    return new HashMap<String, String>(0);
  }

  @Override
  public List<String> getLatestTableNamesForRawEvents(String viewName) throws RemoteException {
    return null;
  }
  
  @Override
  public ArrayList<String> showActiveInterfaces() throws RemoteException {
  	// TODO Auto-generated method stub
  	return null;
  }

  @Override
  public String currentProfile() throws RemoteException {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void slowGracefulPauseEngine() throws RemoteException {
	  // TODO Auto-generated method stub	}
  }
	  

  @Override
  public boolean init() throws RemoteException{	
	  // TODO Auto-generated method stub
	  return false;
  }

  @Override
  public void removeTechPacksInPriorityQueue(List<String> techPackNames) throws RemoteException {    
    // This method is tested by the TransferEngineTest class where the actual code is run.
  }

  @Override
  public void killRunningSets(List<String> techPackNames) throws RemoteException {
    // This method is tested by the TransferEngineTest class where the actual code is run.
  }

  @Override
  public void lockEventsUIusers(boolean lock) throws RemoteException {
    // This method is tested by the TransferEngineTest class where the actual code is run.
  }
      
}
