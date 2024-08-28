package com.distocraft.dc5000.etl.engine.common;

import java.lang.ref.SoftReference;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;

import com.distocraft.dc5000.common.StaticProperties;

public class AggregationStatusCache {

  private static final SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");

  private static Connection con;

  private static String dburl;

  private static String dbuser;

  private static String passwd;

  private static Map<String,SoftReference<AggregationStatus>> cache = new Hashtable<String,SoftReference<AggregationStatus>>();

  private static long read = 0L;

  private static long write = 0L;

  private static long hits = 0L;

  private static long softmiss = 0L;

  private static long dbmiss = 0L;

  private static Logger log = Logger.getLogger("etlengine.AggregationStatusCache");

  private static Timer timer = null;

  private AggregationStatusCache() {
  	
  }
  
  public static void init(final String _dburl, final String _dbuser, final String _passwd, final String drvname) throws SQLException,ClassNotFoundException {

    Class.forName(drvname);

    dburl = _dburl;
    dbuser = _dbuser;
    passwd = _passwd;

    con = DriverManager.getConnection(dburl, dbuser, passwd);

    int speriod = -1;

    try {
      speriod = Integer.parseInt(StaticProperties.getProperty("AggregationStatusCache.StatisticsPeriod", "-1"));
    } catch (Exception e) {
    }

    if (speriod > 0) {
      timer = new Timer();
      timer.schedule(new StatisticsTask(), speriod * 60000, speriod * 60000);
    }

    log.fine("Initialized");

  }

  public static class StatisticsTask extends TimerTask {

    public void run() {
      AggregationStatusCache.logStatistics();
    }

  };

  public static AggregationStatus getStatus(final String aggregation, final long datadate) {

    read++;

    synchronized (cache) {

    	final String key = key(aggregation, datadate);

      final SoftReference<AggregationStatus> sr = cache.get(key);

      AggregationStatus ags = null;

      if (sr != null) { // Found cached entry for key

        ags = sr.get();

        if (ags != null) { // Softreference was effective
          hits++;
        } else { // Softreference was already broken
          softmiss++;

          ags = readDatabase(aggregation, datadate);

          if (ags != null) {
            final SoftReference<AggregationStatus> jr = new SoftReference<AggregationStatus>(ags);
            cache.put(key(ags.aggregation, ags.datadate), jr);
          }

        }

      } else { // Cache miss

        ags = readDatabase(aggregation, datadate);

        if (ags != null) {
          final SoftReference<AggregationStatus> jr = new SoftReference<AggregationStatus>(ags);
          cache.put(key(ags.aggregation, ags.datadate), jr);
        }

      }

      return ags;

    }

  }

  public static void setStatus(final AggregationStatus as) {
    write++;

    synchronized (cache) {

    	final SoftReference<AggregationStatus> sr = new SoftReference<AggregationStatus>(as);
      cache.put(key(as.aggregation, as.datadate), sr);
      writeDatabase(as);

    }

  }

  public static void update(final String sql) throws SQLException {
    synchronized (cache) {

      PreparedStatement stmt = null;

      try {

        try {

          stmt = con.prepareStatement(sql);
          final int rcount = stmt.executeUpdate();

          if (rcount > 0) {
            cache.clear();
          }

        } catch (SQLException e) {

          final String msg = e.getMessage();

          if ((e.getMessage().contains(RockException.CONN_CLOSED)) || 
        		  (e.getMessage().contains(RockException.CONN_TERMINATED))) { // KILL-IDLE is teasing us

            log.info("Connection was already closed. Trying again...");

            con = DriverManager.getConnection(dburl, dbuser, passwd);

            stmt = con.prepareStatement(sql);
            final int rcount = stmt.executeUpdate();
            
            if (rcount > 0) {
              cache.clear();
            }
          }
        }

      } finally {
        if (stmt != null) {
          try {
            stmt.close();
          } catch (Exception e) {
          }
        }
      }
    }
  }

  public static void logStatistics() {
    log.info("Cache statistics " + read + " reads " + write + " writes");
    final double hitrate = hits / read * 100;
    log.info("Cache hit rate " + hitrate + " %");
    final double smissrate = softmiss / read * 100;
    log.info("Soft reference miss " + smissrate);
    final double dbmissrate = dbmiss / read * 100;
    log.info("Database misses " + dbmissrate);
  }

  private static AggregationStatus readDatabase(final String aggregation, final long datadate) {

    log.finest("Reading database " + aggregation + "," + sdf.format(new Date(datadate)));

    PreparedStatement stmt = null;
    ResultSet rs = null;

    AggregationStatus ax = null;

    try {

      try {

        stmt = con.prepareStatement("SELECT * FROM LOG_AGGREGATIONSTATUS_ACT WHERE AGGREGATION = ? AND DATADATE = ?");

        stmt.setString(1, aggregation);
        stmt.setDate(2, new Date(datadate));

        rs = stmt.executeQuery();

      } catch (SQLException e) {

        final String msg = e.getMessage();

        // KILL-IDLE is teasing us
        if ((e.getMessage().contains(RockException.CONN_CLOSED)) || 
      		  (e.getMessage().contains(RockException.CONN_TERMINATED))) {

          log.info("Connection was already closed. Trying again...");

          con = DriverManager.getConnection(dburl, dbuser, passwd);

          stmt = con.prepareStatement("SELECT * FROM LOG_AGGREGATIONSTATUS_ACT WHERE AGGREGATION = ? AND DATADATE = ?");

          stmt.setString(1, aggregation);
          stmt.setDate(2, new Date(datadate));

          rs = stmt.executeQuery();

        }
      }

      while (rs.next()) {

        if (ax != null) {
          log.warning("AggregationStatus not unique for aggregation=" + aggregation + " datadate="
              + sdf.format(new Date(datadate)));
        }
          
        ax = new AggregationStatus();
        ax.aggregation = rs.getString("AGGREGATION");
        ax.typename = rs.getString("TYPENAME");
        ax.timelevel = rs.getString("TIMELEVEL");
        ax.datadate = rs.getDate("DATADATE").getTime();
        if (rs.getTimestamp("INITIAL_AGGREGATION") != null) {
          ax.initial_aggregation = rs.getTimestamp("INITIAL_AGGREGATION").getTime();
        }
        ax.status = rs.getString("STATUS");
        ax.description = rs.getString("DESCRIPTION");
        ax.rowcount = rs.getInt("ROWCOUNT");
        ax.aggregationscope = rs.getString("AGGREGATIONSCOPE");
        if (rs.getTimestamp("LAST_AGGREGATION") != null) {
          ax.last_aggregation = rs.getTimestamp("LAST_AGGREGATION").getTime();
        }
        //TR:HN57054 - EEIKBE (START).
        ax.loopcount = rs.getInt("LOOPCOUNT");
        //ax.ROWCOUNT = rs.getInt("LOOPCOUNT");
        //TR:HN57054 - EEIKBE (FINISH).
        if (rs.getTimestamp("THRESHOLD") != null) {
            ax.threshold = rs.getTimestamp("THRESHOLD").getTime();       
          }
      }

    } catch (Exception e) {
      log.log(Level.WARNING, "readDatabase failed", e);
    } finally {
      try {
        rs.close();
      } catch (Exception e) {
      }
      try {
        stmt.close();
      } catch (Exception e) {
      }
    }

    if (ax == null) {
      dbmiss++;
    }

    return ax;
  }

  private static void writeDatabase(final AggregationStatus as) {

    log.finest("Writing database: " + as.aggregation + "," + sdf.format(new Date(as.datadate)) + ",status=" + as.status
        + ",loopcount=" + as.loopcount);

    PreparedStatement stmt = null;

    try {

      try {

        stmt = con
            .prepareStatement("UPDATE LOG_AGGREGATIONSTATUS_IN SET AGGREGATION=?,TYPENAME=?,TIMELEVEL=?,DATADATE=?,INITIAL_AGGREGATION=?,STATUS=?,DESCRIPTION=?,ROWCOUNT=?,AGGREGATIONSCOPE=?,LAST_AGGREGATION=?, LOOPCOUNT=?, THRESHOLD=? WHERE AGGREGATION=? AND DATADATE=?");

        stmt.setString(1, as.aggregation);
        stmt.setString(2, as.typename);
        stmt.setString(3, as.timelevel);
        stmt.setDate(4, new Date(as.datadate));

        if (as.initial_aggregation == 0) {
          stmt.setTimestamp(5, null);
        } else {
          stmt.setTimestamp(5, new Timestamp(as.initial_aggregation));
        }
        stmt.setString(6, as.status);
        stmt.setString(7, as.description);
        stmt.setInt(8, as.rowcount);
        stmt.setString(9, as.aggregationscope);
        if (as.last_aggregation == 0) {
          stmt.setTimestamp(10, null);
        } else {
          stmt.setTimestamp(10, new Timestamp(as.last_aggregation));
        }
        stmt.setInt(11, as.loopcount);
        
        if (as.threshold == 0) {
            stmt.setTimestamp(12, null);
          } else {
            stmt.setTimestamp(12, new Timestamp(as.threshold));
          }
          
          stmt.setString(13, as.aggregation);
          stmt.setDate(14, new Date(as.datadate));

        stmt.executeUpdate();

      } catch (SQLException e) {

        final String msg = e.getMessage();

        // KILL-IDLE is teasing us
        if ((e.getMessage().contains(RockException.CONN_CLOSED)) || 
      		  (e.getMessage().contains(RockException.CONN_TERMINATED))) {
          log.info("Connection was already closed. Trying again...");

          con = DriverManager.getConnection(dburl, dbuser, passwd);

          stmt = con
              .prepareStatement("UPDATE LOG_AGGREGATIONSTATUS_IN SET AGGREGATION=?,TYPENAME=?,TIMELEVEL=?,DATADATE=?,INITIAL_AGGREGATION=?,STATUS=?,DESCRIPTION=?,ROWCOUNT=?,AGGREGATIONSCOPE=?,LAST_AGGREGATION=?, LOOPCOUNT=?, THRESHOLD=? WHERE AGGREGATION=? AND DATADATE=?");

          stmt.setString(1, as.aggregation);
          stmt.setString(2, as.typename);
          stmt.setString(3, as.timelevel);
          stmt.setDate(4, new Date(as.datadate));

          if (as.initial_aggregation == 0) {
            stmt.setTimestamp(5, null);
          } else {
            stmt.setTimestamp(5, new Timestamp(as.initial_aggregation));
          }
          stmt.setString(6, as.status);
          stmt.setString(7, as.description);
          stmt.setInt(8, as.rowcount);
          stmt.setString(9, as.aggregationscope);
          if (as.last_aggregation == 0) {
            stmt.setTimestamp(10, null);
          } else {
            stmt.setTimestamp(10, new Timestamp(as.last_aggregation));
          }
          stmt.setInt(11, as.loopcount);
          
          if (as.threshold == 0) {
              stmt.setTimestamp(12, null);
            } else {
              stmt.setTimestamp(12, new Timestamp(as.threshold));
            }
            
            stmt.setString(13, as.aggregation);
            stmt.setDate(14, new Date(as.datadate));


          stmt.executeUpdate();

        } else {
          log.log(Level.WARNING, "writeDatabase failed", e);
        }
      }

    } catch (Exception e) {
      log.log(Level.WARNING, "writeDatabase failed", e);
    } finally {
      try {
        stmt.close();
      } catch (Exception e) {
      }
      try {
        con.commit();
      } catch (Exception e) {
      }
    }

  }

  private static String key(final String aggregation, final long datadate) {
    return aggregation + "_" + datadate;
  }

}