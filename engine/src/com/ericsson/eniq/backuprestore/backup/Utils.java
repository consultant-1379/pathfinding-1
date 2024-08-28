package com.ericsson.eniq.backuprestore.backup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.repository.cache.BackupConfigurationCache;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

/**
 * Implementation for the common methods for topology and aggregation backup
 * 
 * @author xhussho
 *
 */

public class Utils {

    public static String backupDir = "/eniq/flex_data_bkup";
	public static String savePath = "/var/tmp/table_info";
	public static String FilePath = "/var/tmp/tablebackedup.txt";
	public static String Script = "/eniq/sw/bin/backupdata.bsh";
	public static String AggregationFlagPath = "/var/tmp";
    private static List<String> DIMtblList = new ArrayList<String>();
	private List<String> finalDIMtblList = new ArrayList<String>();
	Logger log = null;
	Connection conn = null;
	Statement stmt = null;
	ResultSet rs = null;

	public Utils() {
	}
	
	public Utils(final Logger parentlog) {
		log = Logger.getLogger(parentlog.getName() + ".Utils");

	}
	
	
	public Utils(final Logger parentlog, Connection dwh_conn) {
		conn = dwh_conn;
		log = Logger.getLogger(parentlog.getName() + ".Utils");

	}

	public boolean checkData(String tableName) {
		boolean check = false;
		try {
			final String sql = "Select count(*) from " + tableName;
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String count = rs.getString(1);
				if (!(Integer.parseInt(count) == 0)) {
					check = true;
				}
			}
		} catch (SQLException e) {
			if (!e.getSQLState().equals("42S02"))
				log.warning("SQL Exception caught while fetching data from " + tableName + e.getMessage());
		} catch (Exception e) {
			log.warning("Exception caught while checking data " + e.getMessage());
		}
		finally{
			try{
				if(rs != null)
				rs.close();
			}
			catch(Exception e){
				log.warning("Could not close ResultSet" + e.getMessage());
			}
			try{
				if(stmt != null)
				stmt.close();
			}
			catch(Exception e){
				log.warning("Could not close Statement" + e.getMessage());
			}
			
		}
		return check;
	}

	/**
	 * Method for checking for which all DIM MO's topology 2 weeks backup is
	 * enabled.
	 * 
	 * @author xhussho
	 * @throws IOException
	 */

	public List<String> preCheck(){
		String DIMTable = null;
		DIMtblList = BackupConfigurationCache.getBackupTypeNames();
		for (String s : DIMtblList) {		
			if (s.contains("_CURRENT_DC")) {
				DIMTable = s.replaceAll("_CURRENT_DC", "");
				log.log(Level.FINEST, "Checking if data available in the table : " + DIMTable);
				boolean chk = (checkData(DIMTable));
				

				if (chk) {
					if ((!finalDIMtblList.contains(DIMTable))) {
						finalDIMtblList.add(DIMTable);
					}
				}
			}
		}
		log.log(Level.INFO, "Number of Topology Tables with data : " + finalDIMtblList.size());
		FileWriter writer = null;
		try {
			writer = new FileWriter(savePath);
			for (String str : finalDIMtblList) {
				writer.write(str);
				writer.write("\n");
			}
		} catch (IOException e) {
			log.warning("Error while writing data into Dim File" + e.getMessage());
		}
		finally{
		try {
			if(writer != null)
			writer.close();
		} catch (IOException e) {
			log.warning("Could not close Writer" + e.getMessage());
		}
		}
		return finalDIMtblList;
	}

	/**
	 * Method for creating directories for topology backup if already not
	 * created
	 * 
	 * @author xhussho
	 * @param dimTableList
	 * @throws IOException
	 */

	public void createDir(List<String> dimTableList){

		for (String mo : dimTableList)

		{
			String backupDirfull = backupDir + File.separator + mo;
			if (!new File(backupDirfull).exists()) {
				boolean result = false;

				try {
					result = new File(backupDirfull).mkdirs();

				} catch (SecurityException se) {

					log.log(Level.WARNING, "Exception while creating directories", se);
				}
				if (result) {
					log.log(Level.FINE, "Directory created for : " + mo);

				} else {
					log.log(Level.WARNING, "Error while creating directory for : " + mo);
				}
			}
		}
	}

	// }

	/**
	 * Method for zipping the backed up files
	 * 
	 * @author xhussho
	 */

	public void gzipIt(List<String> ziplist) {

		GZIPOutputStream gzos = null;
		FileInputStream in = null;
		for (String s : ziplist) {
			String timeStamp = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
			String OUTPUT_GZIP_FILE = backupDir + File.separator + s + File.separator + s + "_" + timeStamp + ".gz";
			String SOURCE_FILE = backupDir + File.separator + s + File.separator + s + "_" + timeStamp + ".txt";
			File file = new File(SOURCE_FILE);
			// String SOURCE_FILE = s + ".txt";
			byte[] buffer = new byte[1024];

			try {

				gzos = new GZIPOutputStream(new FileOutputStream(OUTPUT_GZIP_FILE));

				in = new FileInputStream(SOURCE_FILE);
				
				int len;
				if(file.exists()){
				while ((len = in.read(buffer)) > 0) {
					gzos.write(buffer, 0, len);

				}		
				}else{
					log.log(Level.INFO, "File does not exist " + s);
				}
			} catch (IOException ex) {
				log.log(Level.WARNING, "Exception while zipping the files for " + s + ex.getMessage());
				ex.printStackTrace();
			} finally {
				try {
					in.close();
					gzos.finish();
					gzos.close();
				} catch (IOException e) {
					log.warning("Could not close GZIPOutputStream" + e.getMessage());
				}

			}
			//Deletion of previous day files
			try{
				file.delete();
			}catch(Exception e){
				log.log(Level.WARNING, "Exception caught while deleting files " + e.getMessage());
			}
		}
	}
	
	public static String compression(String filename,String target,Logger log)
	{
		File inputFile = new File(filename);
		String gzip = target +".gz";
		byte[] buffer = new byte[1024];
        String def=null;
	    try{

	    	GZIPOutputStream gzos =
	    		new GZIPOutputStream(new FileOutputStream(gzip)){
	    		{
	    			this.def.setLevel(Deflater.BEST_COMPRESSION);
	    		}
	    	};

	        FileInputStream in =
	            new FileInputStream(filename);

	        int len;
	        while ((len = in.read(buffer)) > 0) {
	        	gzos.write(buffer, 0, len);
	        }

	        in.close();
	        gzos.finish();
	    	gzos.close();
	    	inputFile.delete();
	    	return gzip;
	    	

	    }catch(IOException ex){
	    	log.warning("File compression and simultaneous copying in the backup filesystem failed,but will still continue copying the file"+ex);
	    	return def;
	    }
	     
	}
	 public String extractHeader(final String fileName) throws IOException {
	        String Header = null;
	        FileInputStream fin = null;
	        GZIPInputStream gzis = null;
	        InputStreamReader xover = null;
	        BufferedReader is = null;
	        try {
	            fin = new FileInputStream(fileName);
	            gzis = new GZIPInputStream(fin);
	            xover = new InputStreamReader(gzis);
	            is = new BufferedReader(xover);
	            Header = is.readLine();
	        }
	        catch (IOException e) {
	            e.printStackTrace();
	            log.severe("error" + e);
	        }
	        catch (Exception e2) {
	            e2.printStackTrace();
	            log.severe("error" + e2);
	        }finally {
	        	try{
	        		if(is!=null)
	        	is.close();
	        		if(xover!=null)	
				xover.close();
	        		if(gzis!=null)
				gzis.close();
	        		if(fin!=null)
				fin.close();
	        	} catch(Exception e){
	        		log.warning("Could not close IO Resources" + e.getMessage());
	        	}
	        }
	        return Header;
	    }
	    
	    public int LoadTable( String MoName,  String columnHeader,  String fileName,  Connection dwhdb_conn1) {
	        final StringBuilder queryBuilder = new StringBuilder();
	        Statement stmt = null;
	        if(MoName.startsWith("DIM") || MoName.startsWith("DC_E_FFAX") ){
	        	queryBuilder.append("delete from ");
	        	queryBuilder.append(MoName);
	        	queryBuilder.append(";\n");
	        }
	        queryBuilder.append("load table ");
	        queryBuilder.append(MoName);
	        queryBuilder.append("( ");
	        queryBuilder.append(columnHeader);
	        queryBuilder.append(") from '");
	        queryBuilder.append(fileName);
	        queryBuilder.append("' ");
	        queryBuilder.append("ESCAPES OFF \n");
	        queryBuilder.append("DELIMITED BY ',' \n");
	        queryBuilder.append("FORMAT BCP \n");
	        queryBuilder.append("IGNORE CONSTRAINT UNIQUE 1000 \n");
	        queryBuilder.append("IGNORE CONSTRAINT NULL 1000 \n");
	        queryBuilder.append("IGNORE CONSTRAINT DATA VALUE 1000 \n");
	        queryBuilder.append("HEADER SKIP 1 ;");
	        try {
	            stmt = dwhdb_conn1.createStatement();
	            log.finest("queryBuilder : "+queryBuilder.toString());
	            int row = stmt.executeUpdate(queryBuilder.toString());
	            return row;
	        }
	        catch (SQLException e) {
	        	log.warning("Couldn't restore table " + MoName + " due to Error  Code " + e.getErrorCode() + " " + e.getSQLState() + "  " + e.getMessage() );
	            return 0;
	        }
	        finally {
	            if (stmt != null) {
	                try {
	                    stmt.close();
	                }
	                catch (SQLException e2) {
	                    e2.printStackTrace();
	                }
	            }
	        }
	    }
	    
		public boolean listContainsIgnoreCase(final List<Map<String, String>> theList, final String theString) {
			boolean found = false;
			if (theList == null && theString == null) {
//				log.warning("checkSetIsRunning(): Error checking if sets are running");
				found = false;
			} else {
				final Iterator<Map<String, String>> iter = theList.iterator();
				while (iter.hasNext()) {
					final Map<String, String> map = iter.next();
					if (map.get("setName").trim().equalsIgnoreCase(theString.trim())) {
						found = true;
						break;
					}
				}
			}
			return found;
		}
		
		
		public String getTechpackName(final String loader_set, final RockFactory rockFact) {

			try {
				final Meta_collections whereObj1 = new Meta_collections(rockFact);
				whereObj1.setEnabled_flag("Y");
				whereObj1.setCollection_name(loader_set);
				// log.finest("whereObj1.getCollection_name : *" +
				// whereObj1.getCollection_name() + "*");
				Meta_collections collection;

				collection = new Meta_collections(rockFact, whereObj1);
				final Meta_collection_sets whereObj = new Meta_collection_sets(rockFact);
				whereObj.setCollection_set_id(collection.getCollection_set_id());
				whereObj.setEnabled_flag("Y");

				final Meta_collection_sets set = new Meta_collection_sets(rockFact, whereObj);

				return set.getCollection_set_name();
			} catch (SQLException | RockException | NullPointerException e) {
				log.severe("could not retrieve collection set name from repdb " + e.getMessage());
				e.printStackTrace();
			}

			return null;
		}

}
