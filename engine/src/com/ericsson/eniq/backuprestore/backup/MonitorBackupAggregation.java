package com.ericsson.eniq.backuprestore.backup;



import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


import java.util.logging.Logger;



/**
 * This class connects to dwhrep and dwhdb tables to take the backup 
 * 
 * @author xsarave
 * 
 */
public class MonitorBackupAggregation {
	
	
	
	private static MonitorBackupAggregation monitorTable;
	private MonitorBackupAggregation() {
	}

	public synchronized static MonitorBackupAggregation getConnect() {
	    if (monitorTable == null) {
	    	monitorTable = new MonitorBackupAggregation();
	    }
	    
	    return monitorTable;
	}
	synchronized void deleteTableEntry(Connection dwhdb,String tableName,String dateId,Logger log)
	{
		

		String deleteQuery="Delete from LOG_BackupAggregation where target_table='"+tableName+"' and date_id='"+dateId+"'";
		Statement stmt=null;
		
		String tablename_partition=null;
		int queryCheck=0;
		try {
			stmt=dwhdb.createStatement();
			queryCheck=stmt.executeUpdate(deleteQuery);
			
			if(queryCheck>0)
			{
				log.info("Successfully deleted the entry from the LOG_BackupAggregation table for tablename:" +tableName+" and dateid:"+dateId+"::");
			}
			else{
				log.info("No such entry in the table LOG_BackupAggregation with tablename:" +tableName+" and dateid:"+dateId+"::");
			}
			} catch (SQLException e) {
				log.info("Exception while deleting the entry from the LOG_BackupAggregation table for tablename:" +tableName+" and dateid:"+dateId+"::"+ e);
			e.printStackTrace();
		}
		
		finally{
			try {
				if(stmt!=null){
					stmt.close();
					}
				} catch (SQLException e) {
				log.info("Exception while closing the resultset of unloadQueryCreation:"+e);
				e.printStackTrace();
			}
		
		}
		
		
		
	
	}
	
	
	
	
	
}
