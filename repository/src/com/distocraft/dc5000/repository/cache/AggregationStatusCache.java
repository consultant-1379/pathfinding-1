package com.distocraft.dc5000.repository.cache;

import java.lang.ref.SoftReference;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.ericsson.eniq.common.Constants;

public class AggregationStatusCache {

  private static final SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");

  private static String dburl;

  private static String dbuser;

  private static String passwd;
  
  private static String drvname;

  private static Hashtable cache = new Hashtable();

  private static long read = 0L;

  private static long write = 0L;

  private static long hits = 0L;

  private static long softmiss = 0L;

  private static long dbmiss = 0L;

  private static Logger log = Logger.getLogger("etlengine.AggregationStatusCache");

  private static Timer timer = null;

  private AggregationStatusCache(){}

  synchronized public static void init(final String _dburl, final String _dbuser, final String _passwd, final String _drvname) throws Exception {

    dburl = _dburl;
    dbuser = _dbuser;
    passwd = _passwd;
    drvname = _drvname;

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

  }

  public static AggregationStatus getStatus(final String aggregation, final long datadate) {

    read++;

    synchronized (cache) {

      final String key = key(aggregation, datadate);

      final SoftReference sr = (SoftReference) cache.get(key);

      AggregationStatus ags;

      if (sr == null) { // Cache miss

        ags = readDatabase(aggregation, datadate);

        if (ags != null) {
          final SoftReference jr = new SoftReference(ags);
          cache.put(key(ags.AGGREGATION, ags.DATADATE), jr);
        }
      } else { // Found cached entry for key

        ags = (AggregationStatus) sr.get();

        if (ags == null) { // Softreference was already broken
          softmiss++;

          ags = readDatabase(aggregation, datadate);

          if (ags != null) {
            final SoftReference jr = new SoftReference(ags);
            cache.put(key(ags.AGGREGATION, ags.DATADATE), jr);
          }
        } else { // Softreference was effective
          hits++;

        }


      }

      return ags;

    }

  }

  public static void setStatus(final AggregationStatus as) {
    write++;

    synchronized (cache) {
      if (checkThresholdReset(as.STATUS)) {
        as.THRESHOLD = 0;
      }
      final SoftReference sr = new SoftReference(as);
      cache.put(key(as.AGGREGATION, as.DATADATE), sr);
      writeDatabase(as);

    }

  }

  /**
   * Checks if the threshold value should be reset to 0 (null in database).
   *
   * @param currentStatus The current status as a string.
   * @return resetThreshold    True if the threshold value should be reset.
   */
  protected static boolean checkThresholdReset(final String currentStatus) {
    boolean resetThreshold = false;

    // MANUAL, IGNORED, ERROR, FAILEDDEPENDENCY, LATE DATA or AGGREGATED,
    // threshold should be reset:
    if (currentStatus.equalsIgnoreCase(Constants.AGG_MANUAL_STATUS) ||
      currentStatus.equalsIgnoreCase(Constants.AGG_IGNORED_STATUS) ||
      currentStatus.equalsIgnoreCase(Constants.AGG_FAILED_STATUS) ||
      currentStatus.equalsIgnoreCase(Constants.AGG_FAILED_DEPENDENCY_STATUS) ||
      currentStatus.equalsIgnoreCase(Constants.AGG_LATE_DATA_STATUS) ||
      currentStatus.equalsIgnoreCase(Constants.AGG_AGGREGATED_STATUS)) {
      resetThreshold = true;
    }
    return resetThreshold;
  }

  public static void update(final String sql) throws Exception {
	  synchronized (cache) {

		  RockFactory dwhRock1 = null;
		  RockFactory dwhRock2 = null;
		  PreparedStatement stmt1 = null;
		  PreparedStatement stmt2 = null;

		  try {
			  dwhRock1 = new RockFactory(dburl, dbuser, passwd, drvname, "AggregationStatusCache", true);

			  try {

				  stmt1 = dwhRock1.getConnection().prepareStatement(sql);
				  final int rcount = stmt1.executeUpdate();

				  if (rcount > 0){
					  cache.clear();
				  }
			  } catch (SQLException e) {

				  final String msg = e.getMessage();

				  if ((e.getMessage().contains(RockException.CONN_CLOSED)) || 
						  (e.getMessage().contains(RockException.CONN_TERMINATED))) { // KILL-IDLE is teasing us

					  log.info("Connection was already closed. Trying again...");

					  //con = DriverManager.getConnection(dburl, dbuser, passwd);
					  dwhRock2 = new RockFactory(dburl, dbuser, passwd, drvname, "AggregationStatusCache", true);

					  stmt2 = dwhRock2.getConnection().prepareStatement(sql);


					  final int rcount = stmt2.executeUpdate();

					  if (rcount > 0){
						  cache.clear();
					  }
				  }
			  }

		  } finally {
			  closeConnection(stmt1, stmt2, dwhRock1, dwhRock2);
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

	  RockFactory dwhRock1 = null;
	  RockFactory dwhRock2 = null;
	  PreparedStatement stmt1 = null;
	  PreparedStatement stmt2 = null;
	  ResultSet rs1 = null;
	  ResultSet rs2 = null;

	  AggregationStatus ax = null;

	  try {

		  try {
			  dwhRock1 = new RockFactory(dburl, dbuser, passwd, drvname, "AggregationStatusCache", true);     
			  stmt1 = dwhRock1.getConnection().prepareStatement("SELECT * FROM LOG_AGGREGATIONSTATUS WHERE AGGREGATION = ? AND DATADATE = ?");
			  stmt1.setString(1, aggregation);
			  stmt1.setDate(2, new Date(datadate));
			  rs1 = stmt1.executeQuery();
			  ax = populateAggregationStatus(rs1, aggregation, datadate);
		  } catch (SQLException e) {

			  final String msg = e.getMessage();

			  if ((e.getMessage().contains(RockException.CONN_CLOSED)) || 
					  (e.getMessage().contains(RockException.CONN_TERMINATED))) { // KILL-IDLE
				  // is
				  // teasing
				  // us

				  log.info("Connection was already closed. Trying again...");

				  dwhRock2 = new RockFactory(dburl, dbuser, passwd, drvname, "AggregationStatusCache", true);
				  stmt2 = dwhRock2.getConnection().prepareStatement("SELECT * FROM LOG_AGGREGATIONSTATUS WHERE AGGREGATION = ? AND DATADATE = ?");
				  stmt2.setString(1, aggregation);
				  stmt2.setDate(2, new Date(datadate));
				  rs2 = stmt2.executeQuery();
				  ax = populateAggregationStatus(rs2, aggregation, datadate);
			  }
		  }
	  } catch (Exception e) {
		  log.log(Level.WARNING, "readDatabase failed", e);
	  } finally {
		  closeConnection(rs1, rs2, stmt1, stmt2, dwhRock1, dwhRock2);
	  }
	  if (ax == null){
		  dbmiss++;
	  }
	  return ax;
  }
  
  private static AggregationStatus populateAggregationStatus(ResultSet rs, String aggregation, long datadate) throws SQLException {
	  AggregationStatus ax =null;
	  while (rs.next()) {

	        if (ax != null){
	          log.warning("AggregationStatus not unique for aggregation=" + aggregation + " datadate="
	              + sdf.format(new Date(datadate)));
	        }
	        ax = new AggregationStatus();
	        ax.AGGREGATION = rs.getString("AGGREGATION");
	        ax.TYPENAME = rs.getString("TYPENAME");
	        ax.TIMELEVEL = rs.getString("TIMELEVEL");
	        ax.DATADATE = rs.getDate("DATADATE").getTime();
	        if (rs.getTimestamp("INITIAL_AGGREGATION") != null) {
	          ax.INITIAL_AGGREGATION = rs.getTimestamp("INITIAL_AGGREGATION").getTime();
	        }
	        ax.STATUS = rs.getString("STATUS");
	        ax.DESCRIPTION = rs.getString("DESCRIPTION");
	        ax.ROWCOUNT = rs.getInt("ROWCOUNT");
	        ax.AGGREGATIONSCOPE = rs.getString("AGGREGATIONSCOPE");
	        if (rs.getTimestamp("LAST_AGGREGATION") != null){
	          ax.LAST_AGGREGATION = rs.getTimestamp("LAST_AGGREGATION").getTime();
	        }
	        //TR:HN57054 - EEIKBE (START).
	        ax.LOOPCOUNT = rs.getInt("LOOPCOUNT");
	        //ax.ROWCOUNT = rs.getInt("LOOPCOUNT");
	        //TR:HN57054 - EEIKBE (FINISH).
	        if (rs.getTimestamp("THRESHOLD") != null) {
	          ax.THRESHOLD = rs.getTimestamp("THRESHOLD").getTime();
	        }
	      }
	  return ax;
  }
  
  private static void closeConnection(ResultSet rs1, ResultSet rs2, 
		  PreparedStatement stmt1, PreparedStatement stmt2, 
		  RockFactory dwhRock1, RockFactory dwhRock2) {
	  //Close resultSet
	  if(rs1 != null){
		  try{
			  rs1.close();
		  } catch(Exception e) {

		  }
	  }
	  if(rs2 != null){
		  try{
			  rs2.close();
		  } catch(Exception e) {

		  }
	  }

	  //Close statement
	  if(stmt1 != null){
		  try{
			  stmt1.close();
		  } catch(Exception e) {

		  }
	  }
	  if(stmt2 != null){
		  try{
			  stmt2.close();
		  } catch(Exception e) {

		  }
	  }

	  //Close connection
	  if (dwhRock1 != null){
		  try {
			  dwhRock1.getConnection().close();  
		  } catch (Exception e) {

		  }
	  }
	  if (dwhRock2 != null){
		  try {
			  dwhRock2.getConnection().close();  
		  } catch (Exception e) {

		  }
	  }

  }
  
  private static void closeConnection(PreparedStatement stmt1, PreparedStatement stmt2, 
		  RockFactory dwhRock1, RockFactory dwhRock2) {
	  //Close statement
	  if(stmt1 != null){
		  try{
			  stmt1.close();
		  } catch(Exception e) {

		  }
	  }
	  if(stmt2 != null){
		  try{
			  stmt2.close();
		  } catch(Exception e) {

		  }
	  }

	  //Close connection
	  if (dwhRock1 != null){
		  try {
			  dwhRock1.getConnection().close();  
		  } catch (Exception e) {

		  }
	  }
	  if (dwhRock2 != null){
		  try {
			  dwhRock2.getConnection().close();  
		  } catch (Exception e) {

		  }
	  }
  }
  
  
  private static void writeDatabase(final AggregationStatus as) {

	  log.finest("Writing database: " + as.AGGREGATION + "," + sdf.format(new Date(as.DATADATE)) + ",status=" + as.STATUS
			  + ",loopcount=" + as.LOOPCOUNT);

	  RockFactory dwhRock1 = null;
	  RockFactory dwhRock2 = null;

	  PreparedStatement stmt1 = null;
	  PreparedStatement stmt2 = null;

	  try {
		  dwhRock1 = new RockFactory(dburl, dbuser, passwd, drvname, "AggregationStatusCache", true);

		  try {

			  final String tablename = PhysicalTableCache.getCache().getTableName("LOG_AggregationStatus:PLAIN",as.DATADATE);

			  if (tablename==null){
				  log.warning("LOG_AggregationStatus partition not found at "+sdf.format(new Date(as.DATADATE)));
			  } else {

				  stmt1 = dwhRock1.getConnection()
						  .prepareStatement("UPDATE "+tablename+" SET AGGREGATION=?,TYPENAME=?,TIMELEVEL=?,DATADATE=?,INITIAL_AGGREGATION=?,STATUS=?,DESCRIPTION=?,ROWCOUNT=?,AGGREGATIONSCOPE=?,LAST_AGGREGATION=?, LOOPCOUNT=?, THRESHOLD=? WHERE AGGREGATION=? AND DATADATE=?");

				  stmt1.setString(1, as.AGGREGATION);
				  stmt1.setString(2, as.TYPENAME);
				  stmt1.setString(3, as.TIMELEVEL);
				  stmt1.setDate(4, new Date(as.DATADATE));

				  if (as.INITIAL_AGGREGATION == 0) {
					  stmt1.setTimestamp(5, null);
				  } else {
					  stmt1.setTimestamp(5, new Timestamp(as.INITIAL_AGGREGATION));
				  }
				  stmt1.setString(6, as.STATUS);
				  stmt1.setString(7, as.DESCRIPTION);
				  stmt1.setInt(8, as.ROWCOUNT);
				  stmt1.setString(9, as.AGGREGATIONSCOPE);
				  if (as.LAST_AGGREGATION == 0) {
					  stmt1.setTimestamp(10, null);
				  } else {
					  stmt1.setTimestamp(10, new Timestamp(as.LAST_AGGREGATION));
				  }

				  stmt1.setInt(11, as.LOOPCOUNT);
				  if (as.THRESHOLD == 0) {
					  stmt1.setTimestamp(12, null);
				  } else {
					  stmt1.setTimestamp(12, new Timestamp(as.THRESHOLD));
				  }

				  stmt1.setString(13, as.AGGREGATION);
				  stmt1.setDate(14, new Date(as.DATADATE));


				  final int rowsUpdated = stmt1.executeUpdate();
				  log.finest("Wrote "+rowsUpdated+" database row(s) for : " + as.AGGREGATION + "," + sdf.format(new Date(as.DATADATE)) + ",status=" + as.STATUS
						  + ",loopcount=" + as.LOOPCOUNT);

			  }



		  } catch (SQLException e) {

			  final String msg = e.getMessage();

			  if ((e.getMessage().contains(RockException.CONN_CLOSED)) || 
					  (e.getMessage().contains(RockException.CONN_TERMINATED))) { // KILL-IDLE
				  // is
				  // teasing
				  // us

				  log.info("Connection was already closed. Trying again...");

				  //con = DriverManager.getConnection(dburl, dbuser, passwd);
				  dwhRock2 = new RockFactory(dburl, dbuser, passwd, drvname, "AggregationStatusCache", true);



				  final String tablename = PhysicalTableCache.getCache().getTableName("LOG_AggregationStatus:PLAIN",as.DATADATE);

				  if (tablename==null){
					  log.warning("LOG_AggregationStatus partition not found at "+sdf.format(new Date(as.DATADATE)));
				  } else {

					  stmt2 = dwhRock2.getConnection()
							  .prepareStatement("UPDATE "+tablename+" SET AGGREGATION=?,TYPENAME=?,TIMELEVEL=?,DATADATE=?,INITIAL_AGGREGATION=?,STATUS=?,DESCRIPTION=?,ROWCOUNT=?,AGGREGATIONSCOPE=?,LAST_AGGREGATION=?, LOOPCOUNT=?, THRESHOLD=? WHERE AGGREGATION=? AND DATADATE=?");

					  stmt2.setString(1, as.AGGREGATION);
					  stmt2.setString(2, as.TYPENAME);
					  stmt2.setString(3, as.TIMELEVEL);
					  stmt2.setDate(4, new Date(as.DATADATE));

					  if (as.INITIAL_AGGREGATION == 0) {
						  stmt2.setTimestamp(5, null);
					  } else {
						  stmt2.setTimestamp(5, new Timestamp(as.INITIAL_AGGREGATION));
					  }
					  stmt2.setString(6, as.STATUS);
					  stmt2.setString(7, as.DESCRIPTION);
					  stmt2.setInt(8, as.ROWCOUNT);
					  stmt2.setString(9, as.AGGREGATIONSCOPE);
					  if (as.LAST_AGGREGATION == 0) {
						  stmt2.setTimestamp(10, null);
					  } else {
						  stmt2.setTimestamp(10, new Timestamp(as.LAST_AGGREGATION));
					  }
					  stmt2.setInt(11, as.LOOPCOUNT);
					  if (as.THRESHOLD == 0) {
						  stmt2.setTimestamp(12, null);
					  } else {
						  stmt2.setTimestamp(12, new Timestamp(as.THRESHOLD));
					  }
					  stmt2.setString(13, as.AGGREGATION);
					  stmt2.setDate(14, new Date(as.DATADATE));


					  stmt2.executeUpdate();

				  }

			  } else {
				  log.log(Level.WARNING, "writeDatabase failed", e);
			  }
		  }

	  } catch (Exception e) {
		  log.log(Level.WARNING, "writeDatabase failed", e);
	  } finally {
		  closeConnection(stmt1, stmt2, dwhRock1, dwhRock2);
	  }

  }

  private static String key(final String aggregation, final long datadate) {
    return aggregation + "_" + datadate;
  }
 

}