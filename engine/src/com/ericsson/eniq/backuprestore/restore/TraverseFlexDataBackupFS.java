package com.ericsson.eniq.backuprestore.restore;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.ServicenamesHelper;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.ExceptionHandler;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.backuprestore.backup.BackupAggregationThreadHandling;
import com.ericsson.eniq.backuprestore.backup.Utils;
import com.ericsson.eniq.common.DatabaseConnections;

import com.ericsson.eniq.repository.ETLCServerProperties;

import ssc.rockfactory.RockFactory;

public class TraverseFlexDataBackupFS implements Runnable {

	private static ThreadPoolExecutor threadpoolExecutorRestore;
	private static LinkedBlockingQueue<Runnable> queue_restore = new LinkedBlockingQueue<Runnable>();
	public static String LAStatus_MO = "/eniq/flex_data_bkup/Log_AggregationStatus";
	// static String BACKUP_FS="H:/WDP/Desktop2k8R2/eniq/flexdata";
	// static String AGG_FLAG="H:/WDP/Desktop2k8R2/var/tmp";
	static String AGG_FLAG = Utils.AggregationFlagPath;
	static String BACKUP_FS = Utils.backupDir;
	static File dir = new File(BACKUP_FS);
	static File agg_flag_dir = new File(AGG_FLAG);
	static File Log_AggregationStatus_MO = new File(LAStatus_MO);
	private static boolean failedToRestore = false;
	private int noOfRetries = 0;
	
	// LinkedHashMap<String, ArrayList<String>> MoFileMap = null;
	private  ArrayList<String> Bfiles = null;

	long flag_epoch = 0L;
	long file_epoch = 0L;
	String tablename = "";
	String backupfile = "";
	Connection conn_dwhdb = DatabaseConnections.getDwhDBConnection()
			.getConnection();
	boolean flag = false;

	final Logger log = Logger.getLogger("etlengine.Engine");
	//BackupAggregationThreadHandling restoreAggregation = null;
	
	private void traverse(File backup_fs_dir) {
		if (backup_fs_dir.isDirectory()) {
			
			
			if (Log_AggregationStatus_MO.isDirectory()){


				
					log.log(Level.INFO, "inside LOG_Aggregationstatus mo. ");

					File[] files = Log_AggregationStatus_MO.listFiles();
					for (File file : files) {
					//	log.info("File available is "+file.getAbsolutePath());
						if (file.isFile()) {
							if (checkAggregationRestoreflag(file, agg_flag_dir)) {
								// String backupfile = file.getName();
								// System.out.print("\n last modified time is less then flag time.doing the operation")
								// ;
								log.log(Level.FINEST,
										"last modified time is less than flag time. doing the operation ");

								// conn_dwhdb =
								// DatabaseConnections.getDwhDBConnection().getConnection();
								try {
									
									Restore_LOG_AggregationStatus rla = new Restore_LOG_AggregationStatus(conn_dwhdb, log);
									rla.execute();								
									
								} catch (Exception e) {
									// TODO Auto-generated catch block
									log.severe("exception while accessing Restore_LOG_AggregationStatus  "+e);
									e.printStackTrace();
								}
							} else
								// System.out.println("\n last modified time is greater then flag time.not doing the operation ")
								// ;
								log.log(Level.INFO,
										"last modified time is greater then flag time.not doing the operation ");
						}
					}

				
			}
			else {
				log.log(Level.WARNING, "Could not able to restore Log_AggregationStatus table as "+LAStatus_MO+" is not available.");
			}
			
			String[] mo = backup_fs_dir.list();
			Arrays.sort(mo);
			log.log(Level.INFO, "list of sorted MO " + Arrays.toString(mo));
		
			for (int i = 0; i < mo.length; i++) {
				Bfiles = new ArrayList<String>();
				File backed_file = new File(backup_fs_dir, mo[i]);
				tablename = mo[i];
				if (backed_file.isDirectory()
						&& mo[i].startsWith("DC_")
						&& (mo[i].endsWith("DAY") || mo[i].endsWith("DAYBH")
								|| mo[i].endsWith("RANKBH") || mo[i]
									.endsWith("COUNT")))

				{
					// System.out.println(" the full path is :"+
					// backed_file.getAbsolutePath());
					log.log(Level.FINEST, "the absolute pathe for:" + mo[i]
							+ "is :" + backed_file.getAbsolutePath());
					// Bfiles = new
					// ArrayList<String>(Arrays.asList(dir1.listFiles()));
					File[] files = backed_file.listFiles();
					// System.out.println("printing the list of files "+files.toString());
					for (File file : files) {
						if (file.isFile()) {

							if (checkAggregationRestoreflag(file, agg_flag_dir)) {
								log.log(Level.FINEST,
										"last modified time is less then flag time.doing the operation");
								// System.out.println("last modified time is less then flag time.doing the operation")
								// ;
								Bfiles.add(file.getAbsolutePath());
								//
							} else
								log.log(Level.INFO,
										"last modified time is greater then flag time.not doing the operation ");

						}

						/**
						 * System.out.print("printing arraylist:"+"\n"); for(int
						 * j = 0; j < Bfiles.size(); j++) {
						 * System.out.print(Bfiles.get(j)+"\n"); }
						 **/
					}
					if (Bfiles != null) {
						
						try {
							queue_restore.offer(new AggreagatorRestoreWorker(
									tablename, Bfiles, log));
							log.info("Blocking queue size ::"
									+ queue_restore.size());
							flag = true;
							
							
						}

						catch (final Exception e) {

							log.warning("Exception in AggregatorRestoreWorker class::"
									+ e);

						}
					}
					
					

				}

			}

			
		}

		// if flex data backup file system does not have any data to be restored
		// ,then delete aggregation flagfile

		if (flag == false) {
			log.log(Level.INFO,
					"No Files to be restored for any of the mo's .deleting  flag file for aggregation. ");

			for (File f : agg_flag_dir.listFiles()) {
				if (f.getName().startsWith("flag_aggregationrestore")) {
					log.log(Level.INFO, "deleting file :" + f.getName());
					f.delete();
				}
			}

		}
		

	}

	private boolean checkAggregationRestoreflag(File file, File agg_flag_dir2) {

		log.log(Level.FINEST, "inside checkAggregationRestoreflag.. ");
		// System.out.println("inside checkAggregationRestoreflag ");
		Date date = null;

		String Flagfile = null;
		for (File flag_file : agg_flag_dir2.listFiles()) {
			//log.info("flag file check"+flag_file.getAbsolutePath());
			if (flag_file.getName().startsWith("flag_aggregationrestore")) {
				//log.log(Level.INFO, "hurree got the flag file .. "); 
				Flagfile = flag_file.getName();
			} 
		//	else	log.severe("aggregation flagfile does not exists . !!! ");
		}

		if (Flagfile != null && !Flagfile.isEmpty())

		{
			
			String[] parts = Flagfile.split("_");
			String flagtimestamp = parts[2];
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
			try {
				date = df.parse(flagtimestamp);
			} catch (ParseException e) {
				log.severe("could not parse flagtimestamp ");
				e.printStackTrace();
			}
			flag_epoch = date.getTime();
			// System.out.println("\n flag epoch is:"+flag_epoch);
			log.log(Level.INFO, "\n flag epoch is: " + flag_epoch);

			file_epoch = file.lastModified();

			// System.out.println("\n file epoch is:"+file_epoch);
			log.log(Level.INFO, "\n file epoch is: " + file_epoch);

			if (file_epoch < flag_epoch) {
				return true;
			}

			else
				return false;
		} else
			return false;
	}

	private void blockingQueueImplementation() {

		final int coreSize = Integer.parseInt(StaticProperties.getProperty(
				"jmsConsumerThreadPoolCoreSize", "15"));
		final int maxSize = Integer.parseInt(StaticProperties.getProperty(
				"jmsConsumerThreadPoolMaxSize", "30"));
		threadpoolExecutorRestore = new ThreadPoolExecutor(coreSize, maxSize,
				100001, TimeUnit.MILLISECONDS, queue_restore);
		threadpoolExecutorRestore.prestartAllCoreThreads();

	}

	private void deleteAggregationFlagFile() {

		log.log(Level.INFO,"entering deleteAggregationFlagFile... " );
		outerloop: while (true) {
			if ( queue_restore.isEmpty()&& threadpoolExecutorRestore.getActiveCount() == 0 )
			
			{
				for (File f : agg_flag_dir.listFiles()) {
					if (f.getName().startsWith("flag_aggregationrestore")) {
						log.log(Level.INFO,
								"deleting flag file because queue_restore is empty :"
										+ f.getName());
						try {
							f.delete();
							
								
								log.log(Level.INFO,"closing dwh database connection " );
							conn_dwhdb.close();
							
							
						
							break outerloop;
						} catch (Exception e) {
							log.info("Exception while deleting flag file " + e);

						}
					}
					
					
				}

			}
			
			//dont log any thing here ,
			else
				
				log.log(Level.FINEST,"threadpoolExecutorRestore getActiveCount is not zero yet . size: "+threadpoolExecutorRestore.getActiveCount());
		}

	}
	
	public static boolean isFailedToRestore() {
		return failedToRestore;
	}

	public static synchronized void setFailedToRestore(boolean failedToRestore) {
		TraverseFlexDataBackupFS.failedToRestore = failedToRestore;
	}

	@Override
	public void run() {
		blockingQueueImplementation();
		while(noOfRetries < 3){
		traverse(dir);
		
		log.info("waitng till the queue and threadpool is empty");
		while(true)
		{
			if ( queue_restore.isEmpty()&& threadpoolExecutorRestore.getActiveCount() == 0 )
			{
				break;
					
			}
			
		}
		
		
		if(!failedToRestore){
			break;
		}
		noOfRetries++;
		log.info("Could not able to load all the files into the DB. Retrying for "+noOfRetries+" time after 5sec.");
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		log.info("exiting traverse .");
		deleteAggregationFlagFile();

	}
}