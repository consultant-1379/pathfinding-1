package com.distocraft.dc5000.repository.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;

/**
 * Cache implementation for checking active statuses
 * 
 * @author lemminkainen
 * 
 */
public class AggregationRuleCache {

  private static Logger log = Logger.getLogger("etlengine.repository.AggregationRuleCache");

  private static final String GET_ACTS = "select y.aggregation, y.target_type, y.target_level,y.source_type, y.source_level, y.aggregationscope from LOG_AGGREGATIONRULES y ";
    
  private String dburl = null;

  private String dbusr = null;

  private String dbpwd = null;

  // key is source_type+source_level
  private Map<String, List<AggregationRule>> arc1 = null;
  
  // key is aggreagtion
  private Map<String, List<AggregationRule>> arc2 = null;

  private static AggregationRuleCache acc = null;

  private AggregationRuleCache() {
  }

  public static void initialize(final RockFactory rock) {

    acc = new AggregationRuleCache();

    try {

      log.fine("Initializing...");

      final Meta_databases selO = new Meta_databases(rock);
      selO.setConnection_name("dwh_coor");
      selO.setType_name("USER");

      final Meta_databasesFactory mdbf = new Meta_databasesFactory(rock, selO);

      final List<Meta_databases> dbs = mdbf.get();

      if (dbs == null || dbs.size() != 1) {
        log.severe("dwhrep database not correctly defined in etlrep.Meta_databases.");
      }

      final Meta_databases repdb = dbs.get(0);

      Class.forName(repdb.getDriver_name());

      acc.dburl = repdb.getConnection_string();
      acc.dbusr = repdb.getUsername();
      acc.dbpwd = repdb.getPassword();

      log.config("Repository: " + acc.dburl);

      acc.revalidate();

    } catch (Exception e) {
      log.log(Level.SEVERE, "Fatal initialization error", e);
    }

  }

  public static AggregationRuleCache getCache() {
    return acc;
  }

  public List<AggregationRule> getAggregationRules(final String source_type, final String source_level) {
	  return arc1.get(source_type+source_level);
  }

  public List<AggregationRule> getAggregationRules(final String aggregation) {
	  return arc2.get(aggregation);
  }

  
  public void revalidate() throws Exception {

    log.fine("Revalidating...");

  
    Connection con = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {

      con = DriverManager.getConnection(dburl, dbusr, dbpwd);

      ps = con.prepareStatement(GET_ACTS);

      rs = ps.executeQuery();

      final Map<String, List<AggregationRule>> hm1 = new HashMap<String, List<AggregationRule>>();
      final Map<String, List<AggregationRule>> hm2 = new HashMap<String, List<AggregationRule>>();

      while (rs.next()) {
    	  
        final String aggregation = rs.getString(1);
        final String target_type = rs.getString(2);
        final String target_level = rs.getString(3);
        final String source_type = rs.getString(4);
        final String source_level = rs.getString(5);
        final String aggregationscope = rs.getString(6);
        
        
        log.finest("Aggregation Rule: aggregation: "+aggregation+" target_type: "+target_type+" target_level: "+target_level+" source_type: "+source_type+" source_level: "+source_level+" aggregationscope: "+aggregationscope);
        final AggregationRule rco = new AggregationRule(aggregation, target_type,target_level, source_type, source_level,  aggregationscope);
        
        if (!hm1.containsKey(source_type + source_level)){
        	final List<AggregationRule> newList = new ArrayList<AggregationRule>();
        	hm1.put(source_type+source_level, newList);
        }

        if (!hm2.containsKey(aggregation)){
        	final List<AggregationRule> newList = new ArrayList<AggregationRule>();
        	hm2.put(aggregation, newList);
        }

        
        hm1.get(source_type+source_level).add(rco);
        hm2.get(aggregation).add(rco);
        
      }

      arc1 = hm1;
      arc2 = hm2;

      log.info("Revalidation succesfully performed. " + arc1.size() + " techpacks found");

    } catch (SQLException sqle) {
      
      if ((sqle.getMessage().indexOf("ASA Error -141") == 0) || (sqle.getMessage().indexOf("SQL Anywhere Error -141") == 0)) {// Table X not found
        log.log(Level.INFO, "Table LOG_AGGREGATIONRULES not found, Techpack DWH_MONITOR probably not installed.");  
      } else {
       log.log(Level.SEVERE, "Fatal initialization error", sqle);
      }
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
}
