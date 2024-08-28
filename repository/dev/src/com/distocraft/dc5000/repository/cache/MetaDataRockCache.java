package com.distocraft.dc5000.repository.cache;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.distocraft.dc5000.repository.dwhrep.Alarminterface;
import com.distocraft.dc5000.repository.dwhrep.AlarminterfaceFactory;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class MetaDataRockCache {
	
	private static final Logger LOG = Logger.getLogger("etlengine.repository.MetaDataRockCache");
	
	private static MetaDataRockCache mdrc = null;
	
	private static final String MCOLLECTIONS_SQLINJECTION = " where collection_set_id in (select distinct collection_set_id from META_COLLECTION_SETS where ENABLED_FLAG = 'Y')";
	
	private static final String MTRANSFER_SQLINJECTION = " where collection_set_id in (select distinct collection_set_id from etlrep.META_COLLECTION_SETS where ENABLED_FLAG = 'Y') order by collection_id, order_by_no";
	
	private List<Meta_collection_sets> metaCollectionSetsList = null;
	
	private List<Meta_collections> metaCollectionsList = null;
	
	private List<Meta_transfer_actions> metaTransferActionsList = null;
	
	private List<Alarminterface> alarmInterfaceList = null;
	
	private static final String USER = "USER";
	
	private static final boolean FAIRLOCK = true;
	
	private static ReadWriteLock metaCollectionsRWLock = new ReentrantReadWriteLock(FAIRLOCK);
	
	private static ReadWriteLock metaCollectionSetsRWLock = new ReentrantReadWriteLock(FAIRLOCK);
	
	private static ReadWriteLock metaTransferActionsRWLock = new ReentrantReadWriteLock(FAIRLOCK);
	
	private static ReadWriteLock alarmInterfacesRWLock = new ReentrantReadWriteLock(FAIRLOCK);

	private String etlrepUrl = null;
	
    private String etlrepDrv = null;
    
    private String etlrepUsr = null;
    
    private String etlrepPwd = null;
    
    private String dwhrepUrl = null;
    
    private String dwhrepDrv = null;
    
    private String dwhrepUsr = null;
    
    private String dwhrepPwd = null;

	/**
	 * Initializing the cache with etlrep connection object to update the cache.
	 *  
	 * @param etlrep
	 */
	public static void initalizer(final RockFactory etlrep){
		mdrc = new MetaDataRockCache();
		
		try {
			
			LOG.fine("Initializing the MetaDataRockCache.!");
			final Meta_databases etlrepRock = mdrc.getMetaDBInfoForConName(etlrep, "etlrep");
			
			mdrc.etlrepUsr = etlrepRock.getUsername();
			mdrc.etlrepPwd = etlrepRock.getPassword();
			mdrc.etlrepUrl = etlrepRock.getConnection_string();
			mdrc.etlrepDrv = etlrepRock.getDriver_name();
		
			final Meta_databases dwhrepRock = mdrc.getMetaDBInfoForConName(etlrep, "dwhrep");
		
			mdrc.dwhrepUsr = dwhrepRock.getUsername();
			mdrc.dwhrepPwd = dwhrepRock.getPassword();
			mdrc.dwhrepUrl = dwhrepRock.getConnection_string();
			mdrc.dwhrepDrv = dwhrepRock.getDriver_name();
		
			mdrc.revalidate();
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Fatal error while initializating MetaDataRockCache", e);
		}
		
	}
	
	/**
	 * Updating the cache.
	 * 
	 * @throws Exception
	 */
	public void revalidate() throws Exception{
		
		LOG.fine("Revalidating...");
		
		RockFactory ETLdc = null;
		RockFactory DWHdc = null;
		try {
			ETLdc = new RockFactory(mdrc.etlrepUrl, mdrc.etlrepUsr, mdrc.etlrepPwd, mdrc.etlrepDrv, "MetaDataRockCache", true);
			setupMetaCollectionSetsCache(ETLdc);
			setupMetaCollectionsCache(ETLdc);
			setupMetaTransferActionsCache(ETLdc);
			DWHdc = new RockFactory(mdrc.dwhrepUrl, mdrc.dwhrepUsr, mdrc.dwhrepPwd, mdrc.dwhrepDrv, "MetaDataRockCache", true);
			setupAlarmInterfacesCache(DWHdc);
		} catch (Exception e) {
			throw new Exception("Error while revalidating the MetaDataRockCache!",e);
		} finally {
			if(ETLdc != null && ETLdc.getConnection() != null){
				ETLdc.getConnection().close();
				
			}
			if(DWHdc != null && DWHdc.getConnection() != null){
				DWHdc.getConnection().close();
			}
		}
		LOG.info("Revalidation succesfully performed. Meta_collection_sets-"+mdrc.getMetaCollectionSets().size()+", Meta_collections-" + mdrc.getMetaCollections().size()+", Meta_transfer_actions-"+mdrc.getMetaTransferActions().size()+" AlarmInterfaces-"+mdrc.getAlarmInterfaceList().size());
		
	}
	
	/**
	 * Setting up the META_COLLECTION_SETS table cache. This will be writing the enabled sets to cache.
	 * 
	 * @param etlRock
	 * @throws SQLException
	 * @throws RockException
	 */
	private void setupMetaCollectionSetsCache(final RockFactory etlRock) throws SQLException, RockException{
		final Meta_collection_sets whereCollSet = new Meta_collection_sets(etlRock);
        whereCollSet.setEnabled_flag("Y");
        final Meta_collection_setsFactory collSet = new Meta_collection_setsFactory(etlRock, whereCollSet);
        mdrc.setMetaCollectionSets(collSet.get());
        LOG.log(Level.FINE,"Loaded "+mdrc.getMetaCollectionSets().size()+" meta_collection_sets data!");
		
	}
	
	/**
	 * Setting up the META_COLLECTIONS table cache. This will be writing only the enabled collection sets to the cache.
	 * 
	 * @param etlRock
	 * @throws SQLException
	 * @throws RockException
	 */
	private void setupMetaCollectionsCache(final RockFactory etlRock) throws SQLException, RockException {
		Meta_collections whereClause = new Meta_collections(etlRock);
    	Meta_collectionsFactory mcf = new Meta_collectionsFactory(etlRock, whereClause,MCOLLECTIONS_SQLINJECTION);
    	mdrc.setMetaCollections(mcf.get());
    	LOG.log(Level.FINE,"Loaded "+mdrc.getMetaCollections().size()+" meta_collections data!");
	}
	
	/**
	 * Setting up the META_TRANSFER_ACTIONS table cache. This will be writing only the enabled collection sets to the cache.
	 * 
	 * @param etlRock
	 * @throws SQLException
	 * @throws RockException
	 */
	private void setupMetaTransferActionsCache(final RockFactory etlRock) throws SQLException, RockException{
		Meta_transfer_actions whereClause = new Meta_transfer_actions(etlRock);
    	Meta_transfer_actionsFactory mtaF = new Meta_transfer_actionsFactory(etlRock, whereClause,MTRANSFER_SQLINJECTION);
    	mdrc.setMetaTransferActions(mtaF.get());
    	LOG.log(Level.FINE,"Loaded "+mdrc.getMetaTransferActions().size()+" meta_transfer_actions data!");
	}
	
	/**
	 * Setting up the AlarmInterfaces table cache. This will be writing only the enabled collection sets to the cache.
	 * 
	 * @param dwhRock
	 * @throws SQLException
	 * @throws RockException
	 */
	private void setupAlarmInterfacesCache(final RockFactory dwhRock) throws SQLException, RockException {
		Alarminterface whereClause = new Alarminterface(dwhRock);
    	AlarminterfaceFactory aiF = new AlarminterfaceFactory(dwhRock, whereClause);
    	mdrc.setAlarmInterfaceList(aiF.get());
    	LOG.log(Level.FINE,"Loaded "+mdrc.getAlarmInterfaceList().size()+" AlarmInterfaces data!");
	}

	/**
	 * Getting the active cache object!
	 * 
	 * @return class object
	 */
	public static MetaDataRockCache getCache(){
			return mdrc;
	}
	
	/**
	 * Updating the temporary cache object to main.
	 * 
	 * @param rmdc
	 */
	public static void setCache(final MetaDataRockCache rmdc) {
		MetaDataRockCache.mdrc = rmdc;
	}
	
	/**
	 * Getting the list of Meta_Collection_sets rock objects
	 * 
	 * @return metaCollectionSetsList
	 */
	private List<Meta_collection_sets> getMetaCollectionSets() {
		metaCollectionSetsRWLock.readLock().lock();
		try {
			return mdrc.metaCollectionSetsList;
		} finally {
			metaCollectionSetsRWLock.readLock().unlock();
		}
		
	}
	
	/**
	 * Fetching META_COLLECTIONS_SET data only for a particular collection_id and collection_set_id. 
	 * 
	 * @param collectionSetName
	 * @return rockObj
	 */
	public Meta_collection_sets getMetaCollectionSets(String collectionSetName){
		for(Meta_collection_sets eachObj : mdrc.getMetaCollectionSets()){
			if(eachObj.getCollection_set_name().equalsIgnoreCase(collectionSetName)){
				return eachObj;
			}
		}
		
		return null;
	}

	/**
	 * Writing the META_COLLECTION_SETS data list.
	 * 
	 * @param mcsl
	 */
	private void setMetaCollectionSets(List<Meta_collection_sets> mcsl) {
		metaCollectionSetsRWLock.writeLock().lock();
		try {
			mdrc.metaCollectionSetsList = mcsl;
		} finally {
			metaCollectionSetsRWLock.writeLock().unlock();
		}		
	}
	
	/**
	 * Writing the META_COLLECTIONS data list.
	 * 
	 * @param mcl
	 */
	private void setMetaCollections(List<Meta_collections> mcl) {
		metaCollectionsRWLock.writeLock().lock();
		try {
			mdrc.metaCollectionsList = mcl;
		} finally {
			metaCollectionsRWLock.writeLock().unlock();
		}
		
	}
	
	/**
	 * Fetching the cached META_COLLECTIONS data.
	 * 
	 * @return metaCollectionsList
	 */
	private List<Meta_collections> getMetaCollections() {
		metaCollectionsRWLock.readLock().lock();
		try {
			return mdrc.metaCollectionsList;
		} finally {
			metaCollectionsRWLock.readLock().unlock();
		}
	}
	
	/**
	 * Fetching AlarmInterface data only for a particular collection_id and collection_set_id.
	 * 
	 * @param versionNumber
	 * @param COLLECTION_ID
	 * @param COLLECTION_SET_ID
	 * @return
	 */
	public Meta_collections getMetaCollections(String versionNumber, String collectionName, long collectionSetId) {
		for(Meta_collections eachObj : mdrc.getMetaCollections()){
			if(eachObj.getCollection_name().equalsIgnoreCase(collectionName) && eachObj.getCollection_set_id() == collectionSetId && eachObj.getVersion_number().equals(versionNumber)) {
				return eachObj;
			}
		}
		return null;
	}
	
	/**
	 * Writing the META_TRANSFER_ACTIONS data list.
	 * 
	 * @param mta
	 */
	private void setMetaTransferActions(List<Meta_transfer_actions> mta) {
		metaTransferActionsRWLock.writeLock().lock();
		try {
			mdrc.metaTransferActionsList = mta;			
		} finally {
			metaTransferActionsRWLock.writeLock().unlock();
		}
	}
	
	/**
	 * Fetching the cached META_TRANSFER_ACTIONS data.
	 * 
	 * @return metaTransferActionsList
	 */
	private List<Meta_transfer_actions> getMetaTransferActions() {
		metaTransferActionsRWLock.readLock().lock();
		try {
			return mdrc.metaTransferActionsList;		
		} finally {
			metaTransferActionsRWLock.readLock().unlock();
		}
	}
	
	/**
	 * Fetching META_TRANSFER_ACTIONS data only for a particular collection_id and collection_set_id.
	 * 
	 * @param COLLECTION_ID
	 * @param COLLECTION_SET_ID
	 * @return mtaTemp
	 */
	public List<Meta_transfer_actions> getMetaTransferActions(long collId,long collSetId) {
		List<Meta_transfer_actions> mtaTemp = new ArrayList<Meta_transfer_actions>();
		for(Meta_transfer_actions eachObj : mdrc.getMetaTransferActions()) {
			if(eachObj.getCollection_id() == collId && eachObj.getCollection_set_id() == collSetId){
				mtaTemp.add(eachObj);
			}
		}
		return mtaTemp;
	}
	
	/**
	 * Fetching the cached AlarmInterfaces data.
	 * 
	 * @return alarmInterfaceList
	 */
	private List<Alarminterface> getAlarmInterfaceList() {
		alarmInterfacesRWLock.readLock().lock();
		try {
			return mdrc.alarmInterfaceList;
		} finally {
			alarmInterfacesRWLock.readLock().unlock();
		}
	}

	/**
	 * Writing the AlarmInterfaces data list.
	 * 
	 * @param alarmInterfaceList
	 */
	private void setAlarmInterfaceList(List<Alarminterface> alarmInterfaceList) {
		alarmInterfacesRWLock.writeLock().lock();
		try {
			mdrc.alarmInterfaceList = alarmInterfaceList;
		} finally {
			alarmInterfacesRWLock.writeLock().unlock();
		}
	}
	
	/**
	 * Fetching AlarmInterface data only for a particular collection_id and collection_set_id.
	 * 
	 * @param COLLECTION_ID
	 * @param COLLECTION_SET_ID
	 * @return aiObj/null
	 */
	public Alarminterface getAlarminterfaceList(long collSetId, long collId){
		for(Alarminterface aiObj : mdrc.getAlarmInterfaceList()){
			if(aiObj.getCollection_id() == collId && aiObj.getCollection_set_id() == collSetId){
				return aiObj;
			}
		}
		return null;
	}
	
	/**
     * Get the Meta database info for a particular connection name
     * 
     * @param etlrep
     *            Db connection
     * @param conName
     *            connection name
     * @throws SQLException
     *             SQL Errors
     * @throws RockException
     *             Error connection to db
     * @throws ClassNotFoundException
     *             Failed to load sql driver
     * @return Connecion details to use to connect as <code>conName</code>
     */
    private Meta_databases getMetaDBInfoForConName(final RockFactory etlrep, final String conName) throws SQLException, RockException,
            ClassNotFoundException {
        final Meta_databases dbCond = new Meta_databases(etlrep);
        dbCond.setConnection_name(conName);
        dbCond.setType_name(USER);

        final Meta_databasesFactory dbFact = new Meta_databasesFactory(etlrep, dbCond);

        final List<Meta_databases> dbs = dbFact.get(); // NOPMD

        if (dbs == null || dbs.size() != 1) {
            LOG.severe(conName + " database not correctly defined in etlrep.Meta_databases.");
            throw new SQLException(conName + " database not correctly defined in etlrep.Meta_databases.");
        }

        final Meta_databases metaDatabase = dbs.get(0);

        Class.forName(metaDatabase.getDriver_name());
        return metaDatabase;
    }
	
	
	
}
