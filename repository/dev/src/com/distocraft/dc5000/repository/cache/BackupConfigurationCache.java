/**
 * 
 */
package com.distocraft.dc5000.repository.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;

import ssc.rockfactory.RockFactory;

/**
 * Cache implementation for checking active statuses
 * 
 * @author xthobob
 *
 */
public class BackupConfigurationCache {
	
	private static Logger log = Logger.getLogger("etlengine.repository.BackupConfiguration");
	    
	private String dburl = null;

	private String dbusr = null;

	private String dbpwd = null;
	
	private final String GET_TPMEASUREMENTS_SQL="select distinct d.TYPENAME from versioning as v,"
			+ "dwhtype as d, BackupConfiguration as b WHERE v.VERSIONID in "
			+ "(select distinct VERSIONID from tpActivation where STATUS='ACTIVE') and v.TECHPACK_NAME = d.TECHPACK_NAME "
			+ "and b.ENABLED_FLAG = 'Y' group by v.LICENSENAME,d.TYPENAME,b.LICENSEID "
			+ "having v.LICENSENAME like '%'+b.LICENSEID+'%'";
	
	private final String GET_DCB_SQL="select LICENSEID,BACKUPLEVEL from backupConfiguration where ENABLED_FLAG ='Y'";
	
	private static List<String> backupTypeNames = null;
	
	private static List<String> licenseIDs = null;

	private String backupLevel = "";
	
	private static boolean backupStatus=false;
	
	private static BackupConfigurationCache bcc = null;
	
	private BackupConfigurationCache(){
		
	}
	
	public static void initialize(final RockFactory rock){
		
		bcc=new BackupConfigurationCache();
		
		try{
			
			log.info("Initializing...");
			final Meta_databases selO = new Meta_databases(rock);
		    selO.setConnection_name("dwhrep");
		    selO.setType_name("USER");

		    final Meta_databasesFactory mdbf = new Meta_databasesFactory(rock, selO);

		    final List<Meta_databases> dbs = mdbf.get();

		    if (dbs == null || dbs.size() != 1) {
		    	log.severe("dwhrep database not correctly defined in etlrep.Meta_databases.");
		      }

		    final Meta_databases repdb = dbs.get(0);

		    Class.forName(repdb.getDriver_name());

		    bcc.dburl = repdb.getConnection_string();
		    bcc.dbusr = repdb.getUsername();
		    bcc.dbpwd = repdb.getPassword();

		    bcc.revalidate();
		      
		} catch(Exception e){
			log.log(Level.SEVERE, "Fatal initialization error", e);
		}
	}
	
	public static BackupConfigurationCache getCache(){
		return bcc;
	}
	
	public void revalidate(){
		
		log.info("Revalidating the cache...");
		
		Connection con=null;
		PreparedStatement ps=null;
		PreparedStatement ps1=null;
		ResultSet rset=null;
		ResultSet rset1=null;
		backupLevel = "";
		licenseIDs = new ArrayList<String>();
		backupTypeNames = new ArrayList<String>();

		try{			
			con=DriverManager.getConnection(dburl, dbusr, dbpwd);
			ps=con.prepareStatement(GET_DCB_SQL);
			rset = ps.executeQuery();
			while(rset.next()){
				licenseIDs.add(rset.getString(1));
				backupLevel=rset.getString(2);
			}
			
			if(!backupLevel.isEmpty()){
				setBackupStatus(true);
				ps1=con.prepareStatement(GET_TPMEASUREMENTS_SQL);
				rset1=ps1.executeQuery();
				while(rset1.next()){
					backupTypeNames.add(rset1.getString(1));
					}
				log.log(Level.INFO,"Backup is enabled.");
			}else{
				setBackupStatus(false);
				log.log(Level.INFO,"Backup is not enabled.");
			}
			
		}catch(SQLException e){
			setBackupStatus(false);
			log.log(Level.SEVERE,"Failed to populate the cache",e);
		}catch (Exception e) {
			log.log(Level.SEVERE,"Exception: ",e);
		}
		finally {
			try {
				if(rset != null){
					rset.close();
					}
				if(rset1 != null){
					rset1.close();
					}
				if(ps != null){
					ps.close();
					}
				if(ps1 != null){
					ps1.close();
				}
				if(con != null){
					con.close();
				}
			} catch (SQLException e) {
				log.log(Level.WARNING,"CLEANUP ERROR: Unable to close the sql connections -",e);
			}catch (Exception e) {
				log.log(Level.WARNING,"CLEANUP ERROR: ",e);
			}

		}
	}
	
	public synchronized boolean isBackupActive(String typeName){
		try{
			if(backupTypeNames.contains(typeName)){
				return true;
				}
			}catch(NullPointerException e){
				log.log(Level.SEVERE,"backup configuration is not validated properly !, Calling cache updator.",e);
				bcc.revalidate();
				try{
					if(backupTypeNames.contains(typeName)){
						return true;
					}else{
						return false;
					}					
				}catch(Exception e1){
					return false;
				}
				
			}catch(Exception e){
				log.log(Level.SEVERE,"Backup Configuration cache is not vaildated properly.",e);
			}
		
		return false;
	}

	/**
	 * @return the backupStatus
	 */
	public static boolean isBackupStatus() {
		return backupStatus;
	}

	/**
	 * @param backupStatus the backupStatus to set
	 */
	public void setBackupStatus(boolean backupStatus) {
		BackupConfigurationCache.backupStatus = backupStatus;
	}
	/**
	 * @return the backupTypeNames
	 */
	public static List<String> getBackupTypeNames() {
		return backupTypeNames;
	}

	/**
	 * @param backupTypeNames the backupTypeNames to set
	 */
	public static void setBackupTypeNames(List<String> backupTypeNames) {
		BackupConfigurationCache.backupTypeNames = backupTypeNames;
	}

	/**
	 * @return the licenseIDs
	 */
	public static List<String> getLicenseIDs() {
		return licenseIDs;
	}

	/**
	 * @param licenseIDs the licenseIDs to set
	 */
	public static void setLicenseIDs(List<String> licenseIDs) {
		BackupConfigurationCache.licenseIDs = licenseIDs;
	}

	/**
	 * @return the backupLevel
	 */
	public String getBackupLevel() {
		return backupLevel;
	}

	/**
	 * @param backupLevel the backupLevel to set
	 */
	public void setBackupLevel(String backupLevel) {
		this.backupLevel = backupLevel;
	}

}
