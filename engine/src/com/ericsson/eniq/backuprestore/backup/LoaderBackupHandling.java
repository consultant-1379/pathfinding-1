package com.ericsson.eniq.backuprestore.backup;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.StaticProperties;

/**
 * LoaderBackupHandling class creates a ThreadPoolExecutor for transferring the 
 * files from internal filesystem to backup filesystem if 2 week backup and
 * restore is enabled.
 * 
 * @author xmriahu
 * 
 */
public class LoaderBackupHandling {

	private static ThreadPoolExecutor threadpoolExecutor;
	private static LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
	private static LoaderBackupHandling instance = null;
	private static Logger log;
	
	private LoaderBackupHandling() {

	}

	/**
	 * This function returns the LoaderBackupHandling class instance and creates
	 * ThreadPoolExecutor.
	 * 
	 * @return Returns the instance of LoaderBackupHandling class.
	 */
	
	public static LoaderBackupHandling getinstance() {
		if (instance == null) {
			instance = new LoaderBackupHandling();
			final int coreSize = Integer.parseInt(StaticProperties.getProperty("jmsConsumerThreadPoolCoreSize", "15"));
			final int maxSize = Integer.parseInt(StaticProperties.getProperty("jmsConsumerThreadPoolMaxSize", "30"));
			threadpoolExecutor = new ThreadPoolExecutor(coreSize, maxSize, 100001, TimeUnit.MILLISECONDS, queue);
			threadpoolExecutor.prestartAllCoreThreads();
			// ShutdownHelper.register(instance);
			
		}

		return instance;
	}

	/**
	 * This function adds the measurement type and file path to the BlockingQueue.
	 * 
	 * 
	 * @param tablename
	 *            conatins the measurement type for which file is being copied.
	
	 * @param filename
	 *            contains the path of the file in the internal filesystem.
	 * 
	 */

	public  void processMessage(String tablename, Logger log, String filename ) {

		try {
			
			log.fine("Adding into queue..");
			queue.offer(new LoaderBackupProcessing(tablename, log, filename));
			log.fine("Currently running threads in threadpoolexecutor ::" + threadpoolExecutor.getActiveCount());
			log.fine("Blocking queue size ::" + queue.size());
			
		} 
		
		catch (final Exception e) {

			log.info("Exception in LoaderBackupHandling class::"+e);

		}

	}

	
	

}
