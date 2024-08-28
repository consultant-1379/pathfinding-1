package com.distocraft.dc5000.repository.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.repository.dwhrep.Partitionplan;
import com.distocraft.dc5000.repository.dwhrep.PartitionplanFactory;
import com.distocraft.dc5000.repository.dwhrep.Typeactivation;
import com.distocraft.dc5000.repository.dwhrep.TypeactivationFactory;

/**
 * Cache implementation for checking active statuses
 * 
 * @author lemminkainen
 * 
 */
public class ActivationCache {

  private static Logger log = Logger.getLogger("etlengine.repository.ActivationCache");

  private static final String GET_ACTS = "SELECT tp.techpack_name,tp.status,ty.typename,ty.tablelevel,ty.status,ty.storagetime FROM tpactivation tp LEFT JOIN typeactivation ty ON (tp.techpack_name = ty.techpack_name)";

  private static final String GET_DEFAULT_STORAGE_TIME = "SELECT pp.defaultstoragetime FROM TypeActivation tp, PartitionPlan pp WHERE tp.techpack_name=? AND tp.typename=? AND tp.tablelevel=? AND tp.partitionplan = pp.partitionplan";
  
  private static final String ACTIVE = "ACTIVE";

  private String dburl = null;

  private String dbusr = null;

  private String dbpwd = null;

  private Map<String, ATechPack> tps = null;

  private static ActivationCache acc = null;

  private ActivationCache() {
  }

  public static void initialize(final RockFactory rock) {

    acc = new ActivationCache();

    try {

      log.fine("Initializing...");

      final Meta_databases selO = new Meta_databases(rock);
      selO.setConnection_name("dwhrep");
      selO.setType_name("USER");

      final Meta_databasesFactory mdbf = new Meta_databasesFactory(rock, selO);

      final List<Meta_databases> dbs = mdbf.get();

      if (dbs == null || dbs.size() != 1) {
        log.severe("dwhrep database not correctly defined in etlrep.Meta_databases.");
      }

      final Meta_databases dwhdb = dbs.get(0);

      Class.forName(dwhdb.getDriver_name());

      acc.dburl = dwhdb.getConnection_string();
      acc.dbusr = dwhdb.getUsername();
      acc.dbpwd = dwhdb.getPassword();

      log.config("Repository: " + acc.dburl);

      acc.revalidate();

    } catch (final Exception e) {
      log.log(Level.SEVERE, "Fatal initialization error", e);
    }

  }
  
  /**
   * Populates the activation cache at initialization time using the dwhrep connection passed by Generic SIU.
 * @param rock Db connection used to read
 * @param afj Is this instance being created in hte AFJ Manager
 *
 */
public static void initialize(final RockFactory rock, final boolean afj) {
	  
	  log.info("Used for AFJ:"+afj);
	  
	  
	    acc = new ActivationCache();
	    
	    try {

	      log.fine("Initializing...");

	      acc.revalidate(rock);

	    } catch (final Exception e) {
	      log.log(Level.SEVERE, "Fatal initialization error", e);
	    }

	  }
  
  public static void initialize(final RockFactory rock, final String dburl, final String dbusr, final String dbpwd) {

    acc = new ActivationCache();

    try {

      log.fine("Initializing...");

      final Meta_databases selO = new Meta_databases(rock);
      selO.setConnection_name("dwhrep");
      selO.setType_name("USER");

      final Meta_databasesFactory mdbf = new Meta_databasesFactory(rock, selO);

      final List<Meta_databases> dbs = mdbf.get();

      if (dbs == null || dbs.size() != 1) {
        log.severe("dwhrep database not correctly defined in etlrep.Meta_databases.");
      }

      final Meta_databases dwhdb = dbs.get(0);

      Class.forName(dwhdb.getDriver_name());

      acc.dburl = dburl;
      acc.dbusr = dbusr;
      acc.dbpwd = dbpwd;

      log.config("Repository: " + acc.dburl);

      acc.revalidate();

    } catch (Exception e) {
      log.log(Level.SEVERE, "Fatal initialization error", e);
    }

  }

  public static ActivationCache getCache() {
    return acc;
  }

  /**
   * Checks weather specified techpack is active. If techpacks is not found
   * returns false.
   * @param techpack The tech pack to check
   * @return TRUE is the techpack is active, FALSE otherwise
   */
  public boolean isActive(final String techpack) {
    final ATechPack atp = tps.get(techpack);
    return atp != null && atp.active;
  }

  /**
   * Checks weather specified type of specified techpack is active (Without
   * specifying level the type is active if any of levels is active). If
   * techpack or type is not found returns false.
   * @param techpack The tech pack to check
   * @param type The techpack type
   * @return TRUE is the techpack is active, FALSE otherwise
   */
  public boolean isActive(final String techpack, final String type) {
    final ATechPack atp = tps.get(techpack);
    if (atp == null) {
      return false;
    }
    if (!atp.active) { // TP not active -> all MTypes not active
      return false;
    }
    final AType at = atp.types.get(type);
    return at != null && at.active;
  }

  /**
   * Checks weather specified type of specified techpack is active. If techpack
   * or type is not found returns false.
   * @param techpack The tech pack to check
   * @param type The techpack type
   * @param level The tech pack level
   * @return TRUE is the techpack is active, FALSE otherwise
   */
  public boolean isActive(final String techpack, final String type, final String level) {
    final ATechPack atp = tps.get(techpack);

    if (atp == null) {
      return false;
    }

    if (!atp.active) { // TP not active -> all MTypes not active
      return false;
    }

    final AType at = atp.types.get(type);

    if (at == null) {
      return false;
    }
    final ALevel al = at.levels.get(level);

    return al != null && al.active;

  }

  /**
   * Returns storage time of specified type. If not found returns -1.
   * @param techPack The tech pack to check
   * @param type The techpack type
   * @param level The tech pack level
   * @return The storage time or -1 is nothing found
   */
  public long getStorageTime(final String techPack, final String type, final String level) {

    final ATechPack atp = tps.get(techPack);

    if (atp == null) {
      return -1;
    }

    if (!atp.active) { // TP not active -> all MTypes not active
      return -1;
    }

    final AType at = atp.types.get(type);

    if (at == null) {
      return -1;
    }

    final ALevel al = at.levels.get(level);

    if (al == null) {
      return -1;
    }

    if (al.storageTime == -1) {
      // StorageTime not defined. Get the default value from the table
      // PartitionPlan.

      Connection con = null;
      PreparedStatement st = null;
      ResultSet rs = null;

      try {
        con = DriverManager.getConnection(dburl, dbusr, dbpwd);

        st = con.prepareStatement(GET_DEFAULT_STORAGE_TIME);
        st.setString(1, techPack);
        st.setString(2, type);
        st.setString(3, level);

        rs = st.executeQuery();

        if (rs.next()) {
          return rs.getLong(1);
        } else {
          log.log(Level.WARNING, "No defaultStorageTime found for " + techPack + " type " + type + " level " + level);
          return -1;
        }

      } catch (Exception e) {
        log.log(Level.WARNING, "Could not connect to database dwhrep.", e);
        return -1;
      } finally {
        try {
          rs.close();
        } catch (Throwable e) {/**/}
        try {
          st.close();
        } catch (Throwable e) {/**/}
        try {
          con.close();
        } catch (Throwable e) {/**/}

      }

    } else {
      return al.storageTime;
    }

  }

  public void revalidate() throws Exception {

    log.fine("Revalidating...");

   // tps = new HashMap<String, ATechPack>();
    
    Connection con = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {

      con = DriverManager.getConnection(dburl, dbusr, dbpwd);

      ps = con.prepareStatement(GET_ACTS);

      rs = ps.executeQuery();

      final HashMap<String, ATechPack> hm = new HashMap<String, ATechPack>();

      while (rs.next()) {

        final String tpname = rs.getString(1);
        final String tpact = rs.getString(2);
        final String typname = rs.getString(3);
        final String typlevel = rs.getString(4);
        final String typact = rs.getString(5);
        final int storage = rs.getInt(6);

        log.finest("TP " + tpname + ": " + tpact + " Type " + typname + ": " + typact + " (" + typlevel + ")");

        ATechPack atp = hm.get(tpname);

        if (atp == null) {
          atp = new ATechPack();
          atp.active = ACTIVE.equalsIgnoreCase(tpact);
          hm.put(tpname, atp);
        }

        if (typname == null) { // TP with no MTypes
          continue;
        }

        AType at = atp.types.get(typname);

        if (at == null) {
          at = new AType();
          atp.types.put(typname, at);
        }

        final boolean btypact = "ACTIVE".equalsIgnoreCase(typact);

        if (btypact) {
          at.active = true;
        }

        final ALevel al = new ALevel();
        al.active = btypact;
        al.storageTime = storage;

        at.levels.put(typlevel, al);

      }

      tps = hm;

      log.info("Revalidation succesfully performed. " + tps.size() + " techpacks found");

    } finally {

      if (rs != null) {
        try {
          rs.close();
        } catch (Exception e) {
          log.log(Level.WARNING, "Cleanup error", e);
        }
      }

      if (ps != null) {
        try {
          ps.close();
        } catch (Exception e) {
          log.log(Level.WARNING, "Cleanup error", e);
        }
      }

      if (con != null) {
        try {
          con.close();
        } catch (Exception e) {
          log.log(Level.WARNING, "Cleanup error", e);
        }
      }

    }
  }
  
  /**
   * Alternative new method to populate the activation cache for uncommitted transactions. Used by Generic SIU
   * Avoids creating a new connection
   * @param dwhrep the db connection to use to revalidate the cache
   * @throws Exception SQL Errors
   */
  public void revalidate(final RockFactory dwhrep) throws Exception {

	    log.info("Revalidating...");    

	    final Typeactivation whereObject = new Typeactivation(dwhrep);
	    final TypeactivationFactory taf = new TypeactivationFactory(dwhrep,whereObject);
	    final List<Typeactivation> typeActivationList = taf.get();
	    
	    final HashMap<String, ATechPack> hm = new HashMap<String, ATechPack>();
	    for(Typeactivation ta: typeActivationList){
	        final String tpname = ta.getTechpack_name();
	        final String tpact = ta.getStatus();
	        final String typname = ta.getTypename();
	        final String typlevel = ta.getTablelevel();
	        final String typact = ta.getStatus();
	        final int storage = getStorageId(ta.getPartitionplan(),dwhrep);
	        
	        log.info("TP " + tpname + ": " + tpact + " Type " + typname + ": " + typact + " (" + typlevel + ")");

	        ATechPack atp = hm.get(tpname);

	        if (atp == null) {
	          atp = new ATechPack();
	          atp.active = ACTIVE.equalsIgnoreCase(tpact);
	          hm.put(tpname, atp);
	        }

	        if (typname == null) { // TP with no MTypes
	          continue;
	        }

	        AType at = atp.types.get(typname);

	        if (at == null) {
	          at = new AType();
	          atp.types.put(typname, at);
	        }

	        final boolean btypact = ACTIVE.equalsIgnoreCase(typact);

	        if (btypact) {
	          at.active = true;
	        }

	        final ALevel al = new ALevel();
	        al.active = btypact;
	        al.storageTime = storage;

	        at.levels.put(typlevel, al);

	      }

	      tps = hm;

	      log.info("Revalidation succesfully performed. " + tps.size() + " techpacks found");

	    } 

	/**
	 * New method to populate the storage id at the time of initialisation of the cache.
	 * Populated during revalidate as well.
	 * @param partitionPlan The partition plan name
	 * @param dwhrep the db connection to use to update the cache/db
	 * @return Storage time for the partition plan
	 * @throws Exception SQL Errors
	 */
	private int getStorageId(final String partitionPlan, final RockFactory dwhrep) throws Exception{
		final Partitionplan whereObject = new Partitionplan(dwhrep);
		whereObject.setPartitionplan(partitionPlan);
		
		final PartitionplanFactory partitionFactory = new PartitionplanFactory(dwhrep, whereObject);
		final List<Partitionplan> partitionPlanList = partitionFactory.get();
		if(partitionPlanList.size() == 0){
			throw new Exception("No partitionplan found for:"+partitionPlan);
		}
		return partitionPlanList.get(0).getDefaultstoragetime().intValue();
  }

  public class ATechPack {

    public boolean active = false;

    public Map<String, AType> types = new HashMap<String, AType>();

  }

  public class AType {

    public boolean active = false;

    public Map<String, ALevel> levels = new HashMap<String, ALevel>();

  }

  public class ALevel {

    public boolean active = false;

    public int storageTime = -1;

  }

}
