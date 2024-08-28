/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2010 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.priorityqueue;

import com.distocraft.dc5000.etl.engine.common.ExceptionHandler;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlot;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfile;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfileHandler;
import com.distocraft.dc5000.etl.engine.main.EngineThread;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.ericsson.eniq.common.Constants;

/**
 * PriorityQueue stores queued ETL sets.
 * 
 * @author savinen
 * @author etuolem
 */
public class PriorityQueue extends Thread {

  private final static Logger LOG = Logger.getLogger("etlengine.PriorityQueue");

  private transient final List<EngineThread> queue = new ArrayList<EngineThread>();
  
  // Contains used queueIDs
  private transient final List<Long> idPool = new ArrayList<Long>();
  private transient long lastQueueID = -1L;
  
  private long pollIntervall = 10000;

  private boolean active = true;

  private transient int maxPriorityLevel = 15;

  private transient final int maxLoadersForType;

  private transient final Set<String> unremovableTypes = new HashSet<String>();
  private transient final Set<String> dubCheckedTypes = new HashSet<String>();
  
  private transient final PersistenceHandler persHandler;
  private transient final ExecutionSlotProfileHandler profileHolder;
  
  public PriorityQueue(final long pollIntervall, final int maxPriorityLevel, final PersistenceHandler persHandler,
  										 final ExecutionSlotProfileHandler profileHolder) {
  	super("Priority Queue");
  	
  	this.persHandler = persHandler;
  	this.profileHolder = profileHolder;
    
    if (pollIntervall != 0) {
      this.pollIntervall = pollIntervall;
    }

    if (maxPriorityLevel != 0) {
      this.maxPriorityLevel = maxPriorityLevel;
    }

    final int amount = Integer.parseInt(StaticProperties.getProperty("PriorityQueue.maxAmountOfLoadersForSameTypeInQueue", "5"));
    this.maxLoadersForType = (amount <= 0) ? -1 : amount;
    
    final String setTypes = StaticProperties.getProperty("PriorityQueue.unremovableSetTypes", "");
    final String[] setTypesArray = setTypes.split(",");

    for (String type : setTypesArray) {
    	if(type.length() > 0) {
    		unremovableTypes.add(type.toLowerCase(Locale.getDefault()));
    		LOG.info("Unremovable set type \"" + type + "\"");
    	}
    }
    
    final String dCTypes = StaticProperties.getProperty("PriorityQueue.DuplicateCheckedSetTypes","Count,Count_day,Backup");
    final String[] dctArr = dCTypes.split(",");

    for(String setType : dctArr) {
    	if(setType.length() > 0) {
    		dubCheckedTypes.add(setType.toLowerCase(Locale.getDefault()));
    		LOG.info("Duplicate checked set type \"" + setType.toLowerCase(Locale.getDefault()) + "\"");
    	}
    }
    
  }

	/**
	 * PriorityQueue housekeeping thread. This thread will handle queueing and
	 * assign Sets to ExecutionSlots.
	 */
	@Override
	public void run() {
		
		while (true) {
			try{

      if(interrupted()) {
        LOG.finer("Skipped sleep (interrupted during exec)");
      } else {
        try {
          sleep(pollIntervall); // Sleep between polls
        } catch(InterruptedException ie) {
          LOG.finer("Interrupted");
        }
      }
      
      final ExecutionSlotProfile execProfile = profileHolder.getActiveExecutionProfile();
      
      LOG.finer("Running. Slots: " + execProfile.getNumberOfFreeExecutionSlots() + "/" +
                execProfile.getNumberOfExecutionSlots() + " PriorityQueue: " + queue.size());
      
      if (active) {
        int count = 0;
        
        profileHolder.cleanActiveProfile();
        
        houseKeep();

        final long loopStart = System.currentTimeMillis();
        
        for(final Iterator<ExecutionSlot> freeSlots = execProfile.getAllFreeExecutionSlots() ; freeSlots.hasNext() ; ) { 
          final ExecutionSlot exeSlot = freeSlots.next();
          
          LOG.finest(exeSlot.getName() + " found free.");
          
          synchronized(queue) {
            
            for(final Iterator<EngineThread> i = queue.iterator() ; i.hasNext() ; ) {           
              final EngineThread set = i.next();
              
              if (isSetAccepted(set, exeSlot, execProfile)) {                   
                set.setSlotId(exeSlot.getSlotId());
                set.setPersistenceHandler(persHandler);
                set.setSlotType(exeSlot.getSlotType());
                exeSlot.execute(set);

                i.remove();
                pushID(set.getQueueID());

                count++;
                LOG.fine("Set " +set.getSetName() + " accepted by " + exeSlot.getName() + " and removed from queue");   
                    
                break;
              }
            } // foreach set in queue
          }
        } // foreach execSlot

        LOG.finer("Delegation loop took " + (System.currentTimeMillis() - loopStart) + " ms. Started " + count + " sets. Sleeping " + pollIntervall + " ms.");
        
      } else {
        LOG.fine("PRIORITY QUEUE ON HOLD");
      }
			}catch(Exception e){
				ExceptionHandler.handleException(e, "Error in Priority Queue");
			}
		}

	}
	
	/**
	 * Check if specified set is accepted by specified ExecutionSlot and ExecutionProfile.
	 */
	private boolean isSetAccepted(final EngineThread set, final ExecutionSlot exeSlot, final ExecutionSlotProfile execProfile) {
		boolean ret = false;
		
		if (set.isActive()) {
			if(set.getEarliestExecution() == null) {
				if (exeSlot.isAccepted(set)) {
					if(execProfile.notInExecution(set)) {
						if(execProfile.checkTable(set)) {
							LOG.finest(set.getSetName() + " is not accepted (tableName)");
						} else if(execProfile.hasMaxMemoryUsageExceeded(set)) {		
							LOG.finest(set.getSetName() + " is not accepted (maxMemory)");
						} else {	
							ret = true;
						}
					} else {
						LOG.finest(set.getSetName() + " is not accepted (already executing)");
					}
				} else {
					LOG.finest(set.getSetName() + " is not accepted (setType)");
				}				
			} else {
				LOG.finest(set.getSetName() + " is not accepted (earliest execution)");
			}
		} else {
			LOG.finest(set.getSetName() + " is not accepted (disabled)");
		}
		
		return ret;
	}
  
	/**
   * Adds a List of sets to priority queue. Method <code>isSetAllowes</code> is used
   * to check if set can be accepted into queue.
   */
	public void addSets(final List<EngineThread> sets) {
		for(EngineThread set : sets) {
			addSetPrivate(set);
		}
		LOG.finer("Interrupt addSets");
		interrupt();
		
	}
	
	/**
   * Adds a set to priority queue. Method <code>isSetAllowes</code> is used
   * to check if set can be accepted into queue.
   */
	public void addSet(final EngineThread set) {
		if(set == null) {
			LOG.info("Tried to insert null set into queue");
  		return;
		}
		
		addSetPrivate(set);
		
		LOG.finer("Interrupt addSet");
		interrupt();
		
	}
  
  private void addSetPrivate(final EngineThread set) {
  	final Long queueId = popID();

  	if(set.getQueueTimeLimit() == null || set.getSetPriority() == null) {       
  		LOG.info(set + " was rejected because queueTimeLimit or setPriority is missing. " + set.getQueueTimeLimit() + " " + set.getSetPriority());
  	} else if (isSetAllowed(set)) {
      	
  		synchronized(queue) {
  			queue.add(set);
  			set.setQueueID(queueId);
  			set.setPriorityQueue(this);
  		}

  		LOG.finer(set + " with ID \"" + queueId + "\" added to the queue.");
      	
  		if(set.isPersistent()) {
  			persHandler.newSet(set);
  		}
  			
  	} else {
  		LOG.fine(set + " was rejected as duplicate.");
  	}
    
  }
  
  /**
   * Removes a set from priority queue to execution slots
   */
  @Deprecated
  protected void executeSet(final EngineThread set) {
  	if(set == null) {
  		LOG.info("Tried to execute null set from the queue.");
  	} else {
  		boolean ret;
  		
  		synchronized (queue)  {
  			ret = queue.remove(set);
  		}
  		
  		if(ret) {
  			pushID(set.getQueueID());
  			LOG.finer(set.toString() + " removed from the queue for execution.");
  		}
  		
  	}
  	
  }
  
  /**
   * Removes a set from priority queue
   */
  public boolean removeSet(final EngineThread set) {
  	
  	boolean ret;
  	
  	if(set == null) {
  		LOG.info("Tried to remove null set from the queue.");
  		ret = false;
  	} else {
  		
  		persHandler.droppedSet(set);
    
  		if(set.setListener != null) {
  			set.setListener.dropped();
  		}
  		
  		synchronized (queue)  {
  			ret = queue.remove(set);
  		}
  		
  		if(ret) {
  			pushID(set.getQueueID());
  			LOG.finer(set.toString() + " removed from the queue.");
  		}
  		
  	} 
    
  	return ret;
  }
  
  
  /**
   * Returns priority of a set in the queue
   * 
   * @return true if priority was changed false otherwise
   */
  public synchronized boolean changePriority(final EngineThread set, final long priority) {
  	boolean ret;
  	
  	if (set != null && priority <= maxPriorityLevel && priority > 0) {
  		LOG.info(set + "\" changing priority to " + priority + ".");
  		set.setSetPriority(Long.valueOf(priority));
  		ret = true;
  	} else {
  		ret = false;
  	}

  	LOG.finer("Interrupt changePriority");
  	interrupt();
  	
  	return ret;
    
  }

  /**
   * Returns a list of sets from the queue that can be executed.
   * Disabled (on hold) and delayed (earliestExecution) are not returned.
   */
  @Deprecated
  public Iterator<EngineThread> getAvailable() {

  	final List<EngineThread> available = new ArrayList<EngineThread>();

  	synchronized(queue) {
  		for (EngineThread set : queue) {
  			if (set.isActive() && set.getEarliestExecution() == null) {
  				available.add(set);
  			}
  		}
  	}
    
    return available.iterator();

  }
  
  /**
   * Returns a list of all sets in the queue.
   */
  public Iterator<EngineThread> getAll() {

  	final List<EngineThread> all = new ArrayList<EngineThread>();

  	synchronized(queue) {
  		for (EngineThread set : queue) {
  			all.add(set);
  		}
  	}
    
    return all.iterator();

  }

  /**
   * Returns List of sets which have specified tablename set.
   */
  public List<EngineThread> getSetsForTableName(final String tableName) {
  	final List<EngineThread> ret = new ArrayList<EngineThread>();
  			
  	synchronized(queue) {
  		for (EngineThread set : queue) {
  			  			
  			if(set.toString().contains("locktable " + tableName)){
  				ret.add(set);
  			}
  		}
  	}
  	
  	return ret;
  }

  /**
   * Returns the number of sets in queue. Held up and postponed sets are not returned.
   */
  @Deprecated
  public int getNumberOfAvailable() {

  	int count = 0;
  	
  	synchronized(queue) {
  		for(EngineThread set : queue) {
  			if (set.isActive() && set.getEarliestExecution() == null) {
  				count++;
  			}
  		}
  	}
  	
    return count;
    
  }

  /**
   * Returns the number of sets in queue.
   */
  public int getNumberOfSetsInQueue() {
  	return queue.size();
  }

  /**
   * Release sets that are postponed to be executed now.
   */
  public void releaseSet(final long queueId) {

  	final EngineThread set = find(queueId);
  	if(set != null && set.getEarliestExecution() != null) {
  		set.setEarliestExection(null);
  		set.setChangeDate(new Date());
  	}
  	
  	LOG.finer("Interrupt releaseSet");
  	interrupt();
  }
  
  /**
   * Releases sets that are postponed to be executed now.
   */
  public void releaseSets(final String techpackName, final String setType) {
  	synchronized(queue) {
  		for(EngineThread set : queue) {
  			if (set.getEarliestExecution() != null && set.getSetType().equalsIgnoreCase(setType) &&
  				 ((set.getTechpackName() != null && set.getTechpackName().equalsIgnoreCase(techpackName)) ||
  				 (set.getTechpackName() == null && techpackName == null))) {
  			  set.setEarliestExection(null);
  			  set.setChangeDate(new Date()); //NOPMD
        }
  		}
  	}

  	LOG.finer("Interrupt releaseSets");
  	interrupt();
  }
  
  /**
   * Finds the EngineThread from Queue and return it
   */
  public EngineThread find(final long queueID) {
  	
  	EngineThread found = null;
  	
  	synchronized(queue) {
  		for(EngineThread set : queue) {
  			if (set.getQueueID().equals(queueID)) {
  			  found = set;
  			  break;
        }
  		}
  	}
     
  	return found;
  	
  }
  
  /**
   * Finds the EngineThread from Queue and return it
   */
  public EngineThread find(final long techpackId, final long setId) {
  	
  	EngineThread found = null;
  	
  	synchronized(queue) {
  		for(EngineThread set : queue) {
  			if (set.getSetID().longValue() == setId && set.getTechpackID().longValue() == techpackId) {
  			  found = set;
  			  break;
        }
  		}
  	}
     
  	return found;
  	
  }
  
  /**
   * Performs housekeeping for the priority queue.
   * <ul>
   * <li>Checks if postpone of execution is gone</li>
   * <li>Checks if latest execution time is exceeded. If so set is dropped from queue.</li>
   * <li>Raise the priority of the set after <code>queueTimeLimit</code> spend in queue</li> 
   * <li>Drop the sets which priority has reached maximum, unless unremovable</li>
   * </ul>
   * Held up set are ignored by housekeeping.
   */
  public void houseKeep() {

  	final long start = System.currentTimeMillis();
  	
  	// Because removeSet will cause Iterator to fail
		final List<EngineThread> toBeRemoved = new ArrayList<EngineThread>();
  	
  	synchronized(queue) {  		
  		
  		for(EngineThread set : queue) {
  			final Date now = new Date(); //NOPMD
  			
  			if(checkLimitTimes(set, now)) {
  				toBeRemoved.add(set);
  				continue;
  			}
  			
  			if (set.isActive()) {				

  				// Upgrade priority every <queueTimeLimit> minutes	
  				final long nextUpgradeTime = set.getChangeDate().getTime() + set.getQueueTimeLimit().longValue() * 60000;
  				
  				if (nextUpgradeTime <= now.getTime()) {
  					set.setChangeDate(now);

  					if (set.getSetPriority() < this.maxPriorityLevel) {
  						changePriority(set, set.getSetPriority() + 1);
  					} else if (!unremovableTypes.contains(set.getSetType().toLowerCase())) {
  						toBeRemoved.add(set);
  						LOG.warning(set + "\" reached priority " + set.getSetPriority() + " and dropped from the queue.");
  					}
  				}
  			} 
  		}
  		
  		for(EngineThread set : toBeRemoved) {
        this.removeSet(set);
        if (set.getSetType().equalsIgnoreCase("Aggregator")) {
          updateAggregatorStatus(set, Constants.AGG_FAILED_DEPENDENCY_STATUS);
          LOG.finer("Status of Aggregator set " + set.getName() + " is changed to "+
            Constants.AGG_FAILED_DEPENDENCY_STATUS+". Set will re-run upon ReAggregation");
        }
      }
  		
  		Collections.sort(queue, new Comparator<EngineThread>() {
  			@Override
        public int compare(final EngineThread set1, final EngineThread set2) {
  	      return set2.getSetPriority().compareTo(set1.getSetPriority());
  	    }
  		});
  		
  	}	
  	
  	LOG.finer("Housekeep took " + (System.currentTimeMillis() - start) + " ms");
  	
  }

  /**
   * Updates the status of an aggregator (of specified engine thread) in table Log_AggregationStatus.
   *
   * @param et        The EngineThread for the aggregator.
   * @param newStatus The status to which it will be set, e.g. "FAILEDDEPENDENCY".
   */
  private void updateAggregatorStatus(final EngineThread et, final String newStatus) {

    //Get the Set's scheduling info in the form of a Properties object
    final String schedulingInfo = et.getSchedulingInfo();
    LOG.finer("Sheduling info of " + et.getSetName() + "is: \n" + schedulingInfo);
    final Properties schedInfoProps = new Properties();
    if (schedulingInfo != null && schedulingInfo.length() > 0) {
      final ByteArrayInputStream bais = new ByteArrayInputStream(schedulingInfo.getBytes());
      try {
        schedInfoProps.load(bais);
        bais.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    //Get the aggregation name and date from scheduling info
    final String aggregation = schedInfoProps.getProperty("aggregation");
    final String dataDate = schedInfoProps.getProperty("aggDate");
    if (null == aggregation || null == dataDate) {
      LOG.finer("Could not get scheduling info of " + et.getSetName() + ". It will not have status updated.");
      return;
    }
    final SimpleDateFormat sdfshort = new SimpleDateFormat("yyyy-MM-dd");
    String shortDate = null;
    shortDate = sdfshort.format(new Date(Long.valueOf(dataDate)));

    //Look up aggregation in cache and get its AggregationStatus object
    AggregationStatus aggSta = null;
    try {
      aggSta = AggregationStatusCache.getStatus(aggregation, Long.valueOf(dataDate));
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (null == aggSta) {
      LOG.finer("Aggregation " + aggregation + " for " + shortDate + " not found in AggregationStatusCache. " +
        "No status update will be done.");
    } else {
      //Update status of set
      LOG.finer("Putting Aggregation " + aggregation + " for " + shortDate + " to " + newStatus);
      aggSta.STATUS = newStatus;
      try {
        AggregationStatusCache.setStatus(aggSta);
        LOG.finer("Status of Aggregator set "+aggregation+" is changed to "+newStatus+". Set will re-run upon ReAggregation");
      } catch (Exception e) {
        LOG.finer("Exception when setting status of Aggregation " + aggregation + " for " + shortDate);
      }
    }
  }
  
  /**
   * Checks EarliestExecution and LatestExecution times for defined set.
   * Returns <code>true</code> if set has to be dropped <code>false</code>
   * otherwise.
   */
  private boolean checkLimitTimes(final EngineThread set, final Date now) {
  	boolean ret;
  
  	if (set.getEarliestExecution() != null) {	  				
  		if (set.getEarliestExecution().before(now)) {
  			LOG.fine(set + " can now be executed.");
  			set.setEarliestExection(null);
  		}
  		
  		// This will prevent priority raise of this set
  		set.setChangeDate(now);
  	}

  	if (set.getLatestExecution() != null && set.getLatestExecution().before(now)) {
      LOG.info(set + " dropped because latest execution (" + set.getLatestExecution().getTime() + ") has passed now ("
          + now.getTime() + ")");
  		ret = true;
  	} else {
  		ret = false;
  	}
  	
  	return ret;
  }

  /**
   * Return id to the pool
   */
  private void pushID(final Long queueID) {
  	synchronized (idPool) {
  		if (!this.idPool.contains(queueID)) {
  			this.idPool.add(queueID);
  		}
  	}
  }

  /**
   * Returns id from pool
   */
  private Long popID() {

  	Long ret;
  	
  	synchronized (idPool) {
  	
  		// is there id in pool
  		if (idPool.isEmpty()) {
  			// create new id	
  			lastQueueID++;

  			ret = Long.valueOf(lastQueueID);
    	
  		} else  {
  			ret = this.idPool.remove(0);
  		} 
    
  	}

  	return ret;
  	
  }

  /**
   * Checks that set entering to queue can be added.
   * Makes duplicate checks accodring to following
   * static.properties parameters:
   * <ul>
   * <li>PriorityQueue.maxAmountOfLoadersForSameTypeInQueue</li>
   * <li>PriorityQueue.DuplicateCheckedSetTypes</li>
   * </ul>
   */
  private boolean isSetAllowed(final EngineThread set) {
    boolean returnValue = true;
    
    if ("Loader".equalsIgnoreCase(set.getSetType()) && maxLoadersForType > -1) {
    	int amount = 0;
    	
    	synchronized(queue) {
      	for(EngineThread queuedSet : queue) {
      		if (queuedSet.getName().equals(set.getName())) {
      			amount++;
      			if(amount >= maxLoadersForType) {
      				returnValue = false;
      				break;
      			}
      		}
      	}
      }
    	
    } else if (dubCheckedTypes.contains(set.getSetType().toLowerCase())) {
    	
    	final String setString = set.toString();
    	
    	synchronized(queue) {
    		for(EngineThread queuedSet : queue) {
    			if(setString.equals(queuedSet.toString())) {
    				returnValue = false;
    				break;
    			}
    		}
    	}
    }

    return returnValue;
  }

  public PersistenceHandler getPersistenceHandler() {
  	return this.persHandler;
  }
  
  /**
   * Reset priority queues pollIntervall and max priorityLevel
   * 
   * @param pollIntevall
   * @param maxPriorityLevel
   */
  public void resetPriorityQueue(final long pollIntevall, final int maxPriorityLevel) {

    if (pollIntevall != 0) {
      this.pollIntervall = pollIntevall;
    }
      
    if (maxPriorityLevel != 0) {
      this.maxPriorityLevel = maxPriorityLevel;
    }
      
  }
  
  // Setters and getters
  
  public long getPollIntervall() {
    return this.pollIntervall;
  }

  public void setPollIntervall(final long intervall) {
    this.pollIntervall = intervall;
  }

  public boolean isActive() {
    return this.active;
  }

  public void setActive(final boolean active) {
  	this.active = active;
  }
  
}
