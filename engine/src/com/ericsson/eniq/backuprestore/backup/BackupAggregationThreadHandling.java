package com.ericsson.eniq.backuprestore.backup;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.StaticProperties;



import ssc.rockfactory.RockFactory;

public class BackupAggregationThreadHandling {


	private static ThreadPoolExecutor threadpoolExecutor;
	 private static LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
	private static BackupAggregationThreadHandling instance = null;

	
	private static Logger log;

	private BackupAggregationThreadHandling() {

	}

	/**
	 * This function returns the AlarmThreadHandling class instance and creates
	 * ThreadPoolExecutor.
	 * 
	 * @return Returns the instance of AlarmThreadHandling class.
	 */
	public static BackupAggregationThreadHandling getinstance() {
		if (instance == null) {
			instance = new BackupAggregationThreadHandling();

			final int coreSize = Integer.parseInt(StaticProperties.getProperty("BackupConsumerThreadPoolCoreSize", "15"));
			final int maxSize = Integer.parseInt(StaticProperties.getProperty("BackupConsumerThreadPoolMaxSize", "30"));
			threadpoolExecutor = new ThreadPoolExecutor(coreSize, maxSize, 100001, TimeUnit.MILLISECONDS, queue);
			threadpoolExecutor.prestartAllCoreThreads();
			// ShutdownHelper.register(instance);
			
		}

		return instance;
	}

	/**
	 * This function gets the Client instance and adds the alarm information to
	 * the BlockingQueue.
	 * 
	 * @param alarmMessage
	 *            alarmMessage object containing the alarm information.
	 * @param log
	 *            contains log instance
	 * @param cache
	 *            contains the object details of ENMServerDetails.
	 * 
	 */

	public void processMessage(final String table_name,final String date_id,final String timelevel, final String typename,final Logger log) {

		try {
			
			this.log = log;
			log.fine("Adding into queue during backup ..");
			
			queue.offer(new DataUnloadThread(table_name,date_id,timelevel,typename, log));
			log.fine("Currently running threads in threadpoolexecutor ::" + threadpoolExecutor.getActiveCount());
			log.fine("Blocking queue size ::" + queue.size());
			

		} catch (final Exception e) {

			log.info("Exception in AggregationTableThread class::"+e);

		}

	}

	
	


}
