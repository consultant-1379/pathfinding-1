package com.distocraft.dc5000.etl.engine.system;

import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.sql.SQLActionExecute;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.VelocityPool;

/**
 * Gatekeeper executes a sqlquery with count(*) as a result. if one or more of
 * the results are equal to 'closedValue' (default is zero) gatekeeper stops the
 * execution of the set (closes the gate).<br>
 * If 'closedAggregationStatus' (default is "BLOCKED") is not empty, aggregation
 * property is not empty and gate is closed aggregation status is set to
 * closedAggregationStatus.<br>
 * Every time gate is closed counter is increased by one. If counter exceeds
 * retries aggregation status is set to failedAggregationStatus (default:
 * "ERROR")<br>
 * Closed counter is located in log_aggregationStatus table in dwh (column:
 * loopCount) <br>
 * <br>
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="4"><font size="+2"><b>Parameter Summary</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Name</b></td>
 * <td><b>Key</b></td>
 * <td><b>Description</b></td>
 * <td><b>Default</b></td>
 * </tr>
 * <tr>
 * <td>Gate clause</td>
 * <td>Action_contents</td>
 * <td>Defines the sqlclause that returns numeric results (result). Result
 * values are compared to closeValue. If one result value equals to the
 * closeValue gate is closed ( execution of the set ends). See. closeValue</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>closedValue</td>
 * <td>Numeric value that closes the gate. See. Gate clause</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>closedAggregationStatus</td>
 * <td>If gate is closed aggregation status column is changed to
 * closedAggregationStatus.</td>
 * <td>BLOCKED</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>failedAggregationStatus</td>
 * <td>If gate is blocked (after number of retries resulting to a closed gate)
 * aggregation status column is changed to failedAggregationStatus.</td>
 * <td>ERROR</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>retries</td>
 * <td>Defines the number of retries that is allowed to a closed aggregation
 * before failedAggregationStatus is used. If aggregation is set to manual,
 * retries count is set to 0. See. failedAggregationStatus</td>
 * <td>4</td>
 * </tr>
 * </table>
 * <br>
 * <br>
 * 
 * @author savinen
 * 
 */
public class GateKeeperAction extends SQLActionExecute {

	private Meta_collections collection;

	private String action;

	protected Logger log;

	protected Logger sqlLog;

	private String set = "";

	private String timelevel = "";

	private String typename = "";

	private String stringDate = "";

	private long longDate = -1l;
	
	private String datadate="";

	private String aggregation = "";

	private String aggregationscope="";
	
	private String aggregationstatus="";
	
	private String where = "";

	private boolean isGateClosed = true;

	private Properties properties;
	
	/**
	 * Empty protected constructor
	 */
	protected GateKeeperAction(final Logger logger) {
		this.log = logger;
		this.properties = new Properties();
	}

	/**
	 * Constructor
	 * 
	 * @param versionNumberga
	 *          metadata version
	 * @param collectionSetId
	 *          primary key for collection set
	 * @param collectionId
	 *          primary key for collection
	 * @param transferActionId
	 *          primary key for transfer action
	 * @param transferBatchId
	 *          primary key for transfer batch
	 * @param connectId
	 *          primary key for database connections
	 * @param rockFact
	 *          metadata repository connection object
	 * @param connectionPool
	 *          a pool for database connections in this collection
	 * @param trActions
	 *          object that holds transfer action information (db contents)
	 */
	public GateKeeperAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final Logger clog)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.collection = collection;
		this.action = trActions.getAction_contents();
		this.where = trActions.getWhere_clause();
		this.set = collection.getCollection_name();
		this.properties = new Properties();
		
		log = Logger.getLogger(clog.getName() + ".GateKeeper");
		sqlLog = Logger.getLogger("sql" + log.getName().substring(4));
	}

	public void execute() throws EngineException {

		String closedValue = "0";
		String closedStatus = Constants.AGG_BLOCKED_STATUS;
		String failedStatus = Constants.AGG_FAILED_STATUS;
		String failedDependency = Constants.AGG_FAILED_DEPENDENCY_STATUS;

		// Get the threshold limit in minutes from the static.properties file:
		int threshold = Integer.parseInt(StaticProperties.getProperty("GateKeeper.thresholdLimit",
				StaticProperties.getProperty("GateKeeper.thresholdLimit", "180")));

		try {
			TransferActionBase.stringToProperties(where, properties);

			closedValue = properties.getProperty("closedValue", "0");
			closedStatus = properties.getProperty("closedAggregationStatus", Constants.AGG_BLOCKED_STATUS);
			failedStatus = properties.getProperty("failedAggregationStatus", Constants.AGG_FAILED_STATUS);
			failedDependency = properties.getProperty("failedDependency", Constants.AGG_FAILED_DEPENDENCY_STATUS);

			threshold = Integer.parseInt(properties.getProperty("thresholdLimit", Integer.toString(threshold)));
		} catch (Exception e) {
			throw new EngineException("Failed to parse configuration from WHERE", e, this, "execute", "");
		}

		Statement stmt = null;
		ResultSet rSet = null;

		String convertedQuery = "#No GateKeeper Query Found";
		try {

			init();

			log.finest("closedValue is " + closedValue);

			if (this.action != null && this.action.length() > 0) {
				convertedQuery = convert(this.stringDate, this.action);
				//Changed the logging from SqlLoging to engine loging.	
				log.finest("Sql:  " + convertedQuery);
			

				stmt = this.getConnection().getConnection().createStatement();
				stmt.getConnection().commit();
				rSet = stmt.executeQuery(convertedQuery);
				stmt.getConnection().commit();

				final List<String> resultList = new ArrayList<String>();

				// collect resultSets
				if (rSet != null) {
					while (rSet.next()) {
						resultList.add(rSet.getString("result"));
						log.fine("subResult is " + rSet.getString("result"));
					}
				}

				// check resultSets
				if (resultList.contains(closedValue)) {
					log.fine("Gate is closed");
					isGateClosed = true;
                   
					if (!closedStatus.equalsIgnoreCase("") && !aggregation.equalsIgnoreCase("")) {
						// set status to given value...
						AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, longDate);
			
						// is this aggregation blocked too many times
						aggSta = setAggToClosedStatus(aggSta, threshold, closedStatus, failedStatus, failedDependency);

						log.fine("Aggregation status: " + aggSta.STATUS);
						AggregationStatusCache.setStatus(aggSta);
					}

				} else {

					log.fine("Gate is open");
					isGateClosed = false;
				}

			}

		} catch (Exception e) {

			final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, longDate);
			if(aggSta != null){
			aggSta.STATUS = failedStatus;
			aggSta.DESCRIPTION = "Error in GateKeepper";
			AggregationStatusCache.setStatus(aggSta);
			}else{
				log.log(Level.WARNING, "Unable to update the log status. GateKeeper query is "+convertedQuery);

			}

			log.log(Level.WARNING, "Gate is closed (Error in GateKeeper)", e);

			throw new EngineException("Error in GateKeeper, gate is closed, aggregations status changed to " + failedStatus,
					e, this, "execute", "");
		} finally {
			try {
				rSet.close();
			} catch (Exception e) {
			}

			try {
				stmt.close();
			} catch (Exception e) {
			}
		}
	}

	/**
	 * Sets an AggregationStatus to closed. If the threshold time has not been
	 * passed, status will be set to BLOCKED. If the time has been passed, status
	 * will be set to FAILEDDEPENDENCY.
	 * 
	 * @param aggStatus
	 *          The aggregation status object.
	 * @param threshold
	 *          The number of minutes in the future to set the threshold, if it
	 *          has not been set yet.
	 * @return aggStatus Updated aggregation status object.
	 * 
	 *         If the threshold time limit has not passed, status will be BLOCKED.
	 *         Otherwise status will be FAILEDDEPENDENCY.
	 */
	public AggregationStatus setAggToClosedStatus(final AggregationStatus aggStatus, final int threshold,
			final String closedStatus, final String failedStatus, final String failedDependency) {

		
		if (aggStatus.STATUS == null) {
			log.log(Level.WARNING, "Aggregation status was not set, resetting value");
			aggStatus.STATUS = "";
		}

		final long currentTime = getCurrentTime();
		if (aggStatus.THRESHOLD == 0) {
			// No time limit is set:
			aggStatus.THRESHOLD = getThresholdTime(threshold);
			final String timeAsString = convertLongToDate(aggStatus.THRESHOLD);
			log.fine("Aggregation blocked, set threshold time to " + timeAsString);
			aggStatus.STATUS = closedStatus;
			aggStatus.LOOPCOUNT++;
		} else {
			// Time limit is set, check if it has been passed:
			if (currentTime >= aggStatus.THRESHOLD) {
				log.fine("Threshold time passed, changing to status" + failedDependency + " for: " + aggregation);
				aggStatus.STATUS = failedDependency;
			} else {
				aggStatus.STATUS = closedStatus;
				aggStatus.LOOPCOUNT++;
			}
		}

		return aggStatus;
	}

	/**
	 * Gets the threshold time when an aggregation first goes to "BLOCKED".
	 * 
	 * @return Time in milliseconds.
	 */
	protected long getThresholdTime(final int threshold) {
		final Calendar timeLimit = Calendar.getInstance();
		timeLimit.add(Calendar.MINUTE, threshold);
		return timeLimit.getTimeInMillis();
	}

	protected long getCurrentTime() {
		return System.currentTimeMillis();
	}

	protected String convertLongToDate(final long timeInMilliseconds) {
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final Date mydate = new Date(timeInMilliseconds);
		final String returnValue = sdf.format(mydate);
		return returnValue;
	}

	protected void init() throws EngineMetaDataException {

		// '2005-03-16 00:00:00'
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final SimpleDateFormat sdfShort = new SimpleDateFormat("yyyy-MM-dd");
		String dateString = "";
		String sqlClause = "";

		Statement stmt = null;
		ResultSet rSet = null;
		log.info("Set is "+this.set);
		String aggregationFromSet=this.set.substring(this.set.indexOf("_")+1,this.set.length());
		try {
			
			sqlClause = "select agg.aggregation,agg.target_type, agg.target_level,agg.aggregationscope " + "from LOG_AggregationRules agg"
					+ " where agg.aggregation = '" + aggregationFromSet + "' ";

			log.fine("Retrieve " + aggregationFromSet + " target.type and target.level");
			sqlLog.finest("Unparsed sql:" + sqlClause);

			stmt = this.getConnection().getConnection().createStatement();
			stmt.getConnection().commit();
			rSet = stmt.executeQuery(sqlClause);
			stmt.getConnection().commit();

			if (rSet != null) {
				while (rSet.next()) {
					this.timelevel = (String) rSet.getString("target_level");
					this.typename = (String) rSet.getString("target_type");
					this.aggregation = (String) rSet.getString("aggregation");
					this.aggregationscope=(String) rSet.getString("aggregationscope");
				}
			}
			
			log.info("Time level "+this.timelevel);
			log.info("Aggregation: "+this.aggregation);
			log.info("Aggregation Scope :"+this.aggregationscope);

		} catch (Exception e) {
			log.log(Level.FINE, "Error while accesing LOG_AggregationRules table", e);
		} finally {
			try {
				rSet.close();
			} catch (Exception e) {
			}
			try {
				stmt.close();
			} catch (Exception e) {
			}
		}
		
		log.info("Props "+collection.getScheduling_info());

		TransferActionBase.stringToProperties(collection.getScheduling_info(), properties);

		this.aggregation = properties.getProperty("aggregation", this.aggregation);
		this.typename = properties.getProperty("typename", this.typename);
		this.timelevel = properties.getProperty("timelevel", this.timelevel);
		// this.aggStatus = properties.getProperty("status",this.aggStatus);
		dateString = properties.getProperty("aggDate", "");

		log.info("Date from property is "+dateString);
	try {
			// try to parse from yyyy-MM-dd HH:mm:ss format
			this.longDate = sdf.parse(dateString).getTime();
			this.stringDate = dateString;
		} catch (Exception e) {
			// not in yyyy-MM-dd HH:mm:ss format
			this.stringDate = "";
		}
		
		try {
			// try to parse from yyyy-MM-dd format
			final Date date = sdfShort.parse(dateString);
			this.longDate = date.getTime();
			this.stringDate = sdf.format(date);
			log.info("Date string is "+stringDate);
		} catch (Exception e) {
			// not in yyyy-MM-dd HH:mm:ss format
			this.stringDate = "";
		}

		/*if (this.stringDate.equalsIgnoreCase("")) {

			try {
				// try to parse from long format
				final GregorianCalendar curCal = new GregorianCalendar();
				curCal.setTimeInMillis(Long.parseLong(dateString));
				this.stringDate = sdf.format(curCal.getTime());
				this.longDate = Long.parseLong(dateString);
			} catch (Exception e) {
				// not in long format
				this.stringDate = "";
			}
		}*/
		stmt = null;
		rSet = null;
		try {

			sqlClause = "select aggs.datadate,aggs.status " + "from LOG_AggregationStatus aggs"
					+ " where aggs.aggregation = '" + aggregationFromSet + "' and aggs.status ='MANUAL'"; 

			log.fine("Retrieve " + this.set + " aggregation status and aggregation scope");
			sqlLog.finest("Unparsed sql:" + sqlClause);

			stmt = this.getConnection().getConnection().createStatement();
			stmt.getConnection().commit();
			rSet = stmt.executeQuery(sqlClause);
			stmt.getConnection().commit();

			if (rSet != null) {
				while (rSet.next()) {
					this.datadate = (String) rSet.getString("datadate");
					this.aggregationstatus = (String) rSet.getString("status");
				}
			}

		} catch (Exception e) {
			log.log(Level.FINE, "Error while accesing LOG_AggregationStatus table", e);
		} finally {
			try {
				rSet.close();
			} catch (Exception e) {
			}
			try {
				stmt.close();
			} catch (Exception e) {
			}
		}
		
		log.info("Aggregation Status "+aggregationstatus);
		// if stringDate is empty, use previous day
		if (stringDate.equalsIgnoreCase("")) {
			
			if(aggregationstatus.equals("MANUAL"))
			{
				try {
					final Date date = sdfShort.parse(datadate);
					this.stringDate = sdf.format(date);
					this.longDate = date.getTime();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				log.fine("Date of aggregation is " + stringDate + " (" + longDate + ")");
				
			}
			else
			{
				log.info("Aggregation Scope "+aggregationscope);
				final Calendar cal = Calendar.getInstance();
				if(aggregationscope.equals("WEEK"))
				{
					cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
					cal.add(Calendar.DAY_OF_MONTH, -7);
			    }
				else if(aggregationscope.equals("DAY"))
				{
					cal.add(Calendar.DAY_OF_MONTH, -1);
				}
				else if(aggregationscope.equals("MONTH"))
				{
					cal.add(Calendar.MONTH,-1);
					cal.set(Calendar.DATE, 1);
				}

				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);

				this.stringDate = sdf.format(cal.getTime());
				this.longDate = cal.getTimeInMillis();
			}
			log.fine("Unable to parse date of aggregation. Using calculated date.");

		}

		log.fine("Date of aggregation is " + stringDate + " (" + longDate + ")");

	}

	public String convert(final String dateString, final String s) {
		final StringWriter writer = new StringWriter();
		VelocityEngine ve = null;
		try {

			ve = VelocityPool.reserveEngine();
			final VelocityContext context = new VelocityContext();
			context.put("date", "'" + dateString + "'");
			ve.evaluate(context, writer, "", s);

		} catch (Exception e) {
			log.log(Level.FINE, "Velocity covert failed", e);
		} finally {
			VelocityPool.releaseEngine(ve);
		}

		return writer.toString();
	}

	public boolean isGateClosed() {
		return isGateClosed;
	}

	protected Properties getProperties() {
		return properties;
	}

	protected void setProperties(final Properties properties) {
		this.properties = properties;
	}

}
