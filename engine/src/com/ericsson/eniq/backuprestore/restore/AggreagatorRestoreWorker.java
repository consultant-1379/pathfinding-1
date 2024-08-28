package com.ericsson.eniq.backuprestore.restore;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.backuprestore.backup.Utils;
import com.ericsson.eniq.common.DatabaseConnections;

public class AggreagatorRestoreWorker implements Runnable {



	private Connection dwhdb_con = null;
	private String partitionTable = null;
	private Logger log;
	private String baseTableName = null;
	private String columnHeader = null;
	private List<String> fileNames = null;
	private File file = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public AggreagatorRestoreWorker(String baseTableName, List<String> fileNames,Logger log){

		this.baseTableName = baseTableName;
		this.fileNames = new ArrayList<String>();
		this.fileNames = fileNames;
		this.log = log;	

	}
	
	public void run() {
		initailze();
		Utils utilObj = new Utils(log);
		for(String filename : fileNames){
			Date partitionDate = null;
			file = new File(filename);
			String fileDate = extractDate(filename);
			if(fileDate != null ){
				try {
					partitionDate = sdf.parse(fileDate);
				} catch (ParseException e) {
					log.log(Level.SEVERE,"unparsable date format("+fileDate+") from the filename:"+filename+".");
					if(FileDelete(file)){
						log.log(Level.INFO,filename+" deleted successfully!");
					}else {
						log.log(Level.WARNING,"Unable to delete the file - "+filename);
					}
					continue;
				}
			
				partitionTable = partitionFetch(partitionDate.getTime());
				//log.log(Level.INFO," partitionTable is "+partitionTable);
				try {
					columnHeader = utilObj.extractHeader(filename);
				} catch (IOException e1) {
					
					log.log(Level.SEVERE,"Error while extracting the filename!");
				}
				log.log(Level.FINEST,"FileName -"+filename+" and columns -"+columnHeader);
				int rows = 0;
				if(partitionTable != null){
					rows = LoadAggDataToTable(partitionTable, columnHeader, filename);
					if(rows != -1)
						log.log(Level.FINE,filename+":- rows loaded are "+rows);
					else{
						//log.info("set flag one ");
						TraverseFlexDataBackupFS.setFailedToRestore(true);
						log.warning("setting failedToRestore flag as "+TraverseFlexDataBackupFS.isFailedToRestore());
					}
						
}
				else {
					log.log(Level.WARNING,"Could not able to find partition for "+baseTableName+" for the date "+partitionDate);
				}
				if( rows != -1 ){
					if(FileDelete(file)){
						log.log(Level.FINEST,filename+" deleted successfully!");
					}else {
						log.log(Level.WARNING,"Unable to delete the file - "+filename);
						log.info("set flag two ");
						TraverseFlexDataBackupFS.setFailedToRestore(true);
						log.info("set flag two with "+TraverseFlexDataBackupFS.isFailedToRestore());
					}
				}
			}	
		}
		cleanUp();
	}
	

	public void cleanUp(){
		//log.info("Cleanup method has been called and starting the connection cleanup.");


	if(dwhdb_con != null){
			try {
				dwhdb_con.close();
				log.info("Is Connection closed: "+dwhdb_con.isClosed());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.warning("Could not able to close the connection due to Error  Code " + e.getErrorCode() + " " + e.getSQLState() + "  " + e.getMessage());
			}
		}
		
	}








	
	public boolean FileDelete(File fileToDelete){
		if(fileToDelete.delete()){
			return true;
		}
		return false;
	}

	public void initailze(){

		dwhdb_con = DatabaseConnections.getDwhDBConnection().getConnection();
	}

	
	public String extractDate(String filename){
		String regex ="((19|20)\\d\\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01]))";
		Pattern pattern = Pattern.compile(regex);
	    Matcher matcher = pattern.matcher(filename);
	    if(matcher.find()){
	    	return matcher.group(1);
	    }
	    return null;
		
	}
	
	public String partitionFetch(long fileDate) {
		String storageId = baseTableName.replaceFirst("(?s)_(?!.*?_)", ":");
		return PhysicalTableCache.getCache().getTableName(storageId, fileDate);		
	}
	
	public int LoadAggDataToTable( String tablePartition,  String columnHeader,  String fileName) {
        final StringBuilder queryBuilder = new StringBuilder();
        Statement stmt = null;
        int rowLoaded =0;
        queryBuilder.append("load table ");
        queryBuilder.append(tablePartition);
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
      //  log.log(Level.INFO,"sql - "+queryBuilder.toString());
        try {
            stmt = dwhdb_con.createStatement();
            rowLoaded = stmt.executeUpdate(queryBuilder.toString());


        }
        catch (SQLException e) {
        	log.warning("Couldn't restore table " + tablePartition + " due to Error  Code " + e.getErrorCode() + " " + e.getSQLState() + "  " + e.getMessage() );
        	if (stmt != null) {
                try {
                    stmt.close();
                }
                catch (SQLException e2) {
                	log.warning("Could not able to close the Statement due to Error  Code " + e2.getErrorCode() + " " + e2.getSQLState() + "  " + e2.getMessage()); 
                }
            }
            rowLoaded = -1;
        }
        catch (Exception e1){
        	log.log(Level.SEVERE,"Couldn't restore table " + tablePartition + " due to " + e1.getMessage() );
        	if (stmt != null) {
                try {
                    stmt.close();
                }
                catch (SQLException e2) {
                	log.warning("Could not able to close the Statement due to Error  Code " + e2.getErrorCode() + " " + e2.getSQLState() + "  " + e2.getMessage()); 
                }
            }
            rowLoaded = -1;
        }
        finally {
            if (stmt != null) {
                try {
                    stmt.close();
                }
                catch (SQLException e2) {
                	log.warning("Could not able to close the Statement due to Error  Code " + e2.getErrorCode() + " " + e2.getSQLState() + "  " + e2.getMessage()); 
                }
            }
        }
        return rowLoaded;
    }
	
	

}
