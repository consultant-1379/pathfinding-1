package com.ericsson.eniq.backuprestore.backup;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is a thread class that contains functionality to copy
 * Loader files from internal filesystem to backup filesytem if
 * 2 week backup and restore is enabled.
 * 
 * @author xmriahu
 * 
 */
	
public class LoaderBackupProcessing implements Runnable {

	Logger log;
	String tablename;
	String filename;
		
	/**
	* This function is the constructor of this LoaderBackupProcessing class.
	* 
	* @param tablename
	* @param log
	* @param filename
	* 
	 */
		
	public LoaderBackupProcessing(String tablename, final Logger log, String filename) {

		this.tablename = tablename;
		this.log = log;
		this.filename = filename;
	
	}

	/**
	 * Copying file from internal filesytem to backup filesystem.
	 */
	
	@Override
	
	public void run() {
		File inputFile = new File(filename);
		String backup="/eniq/flex_data_bkup/"+tablename+"/raw/";
		if (!new File(backup).exists()) {
			log.log(Level.INFO, "Creating directory " + backup);
			new File(backup).mkdirs();
		}
		final File targetFile = new File(backup + inputFile.getName());
		String target=targetFile.getPath();
		String gzip=Utils.compression(filename,target,log);
		if(gzip!=null){
			log.finest("Compressed and simultaneously copied the file to backup file system");
		}
		else{
			log.info("Compression failed,will continue copying the file");
			File outputFile=new File(filename);
			log.finest("Moving file " + outputFile.getName() + " to " + backup);
			try {
				final InputStream in = new FileInputStream(outputFile);
				final OutputStream out = new FileOutputStream(targetFile);
				final byte[] buf = new byte[1024];
				int len;
				while ((len = in.read(buf)) > 0) {
					out.write(buf, 0, len);
				}
				in.close();
				out.close();
				outputFile.delete();
			}
			catch (Exception e) {
				log.log(Level.WARNING, "Move with memory copy failed", e);
			}
		}
	}
}