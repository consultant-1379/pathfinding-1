package com.ericsson.eniq.common.setWizards;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.VelocityException;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.Constants;
import com.distocraft.dc5000.repository.dwhrep.Aggregation;
import com.distocraft.dc5000.repository.dwhrep.AggregationFactory;
import com.distocraft.dc5000.repository.dwhrep.Aggregationrule;
import com.distocraft.dc5000.repository.dwhrep.AggregationruleFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhour;
import com.distocraft.dc5000.repository.dwhrep.BusyhourFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementcounter;
import com.distocraft.dc5000.repository.dwhrep.MeasurementcounterFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementkey;
import com.distocraft.dc5000.repository.dwhrep.MeasurementkeyFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtable;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtableFactory;
import com.ericsson.eniq.common.Utils;

/**
 * 
 * @author savinen Copyright Distocraft 2005
 * 
 *         $id$
 */
public class CreateAggregatorSet implements SetCreator {

	private static final Logger log = Logger.getLogger(CreateAggregatorSet.class.getName());

	protected String loaderTemplateName;

	protected String setTemplateName;

	protected RockFactory dwhrepRock;

	protected RockFactory rock;

	protected int techPackID;

	protected long maxSetID = 0;

	protected long maxActionID = 0;

	protected long maxSchedulingID = 0;

	protected boolean doSchedulings = false;

	protected String templateDir = "";

	protected String name;

	protected String version;

  private final  String versionid;

	/**
	 * The default template used to generate the BH placeholder aggregators
	 */
	private static final String DEFAULT_RANKBH_TEMPLATE = "RANKBH_TIMELIMITED";

	/**
	 * constructor
	 * 
	 * @param templateDir
	 *          Directory containing all the required templates to create the
	 *          aggregation sets
	 * @param name
	 *          The aggregator name
	 * @param version
	 *          The TP Version
	 * @param versionid
	 *          The version ID
	 * @param dwhrepRock
	 *          DWHREP connection
	 * @param etlrep
	 *          ETLREP connection
	 * @param techPackID
	 *          TP ID
	 * @param schedulings
	 *          Regenerate the schedulings
	 * @throws Exception
	 *           Any errors
	 */
	public CreateAggregatorSet(final String templateDir, final String name, final String version, final String versionid, final RockFactory dwhrepRock,
			final RockFactory etlrep, final int techPackID, final boolean schedulings) throws SQLException {

		this.templateDir = templateDir;
		this.dwhrepRock = dwhrepRock;
		this.rock = etlrep;
		this.techPackID = techPackID;

		this.maxSetID = Utils.getSetMaxID(rock) + 1;
		this.maxActionID = Utils.getActionMaxID(rock) + 1;
		this.maxSchedulingID = Utils.getScheduleMaxID(rock) + 1;
		this.doSchedulings = schedulings;

		this.name = name;
		this.version = version;
		this.versionid = versionid;

	}

	/**
	 * 
	 * Merges template and context
	 * 
	 * @param templateName
	 *          The template to use for the merge
	 * @param context
	 *          A context object holiing required data for the merge
	 * @return string contains the output of the merge
	 * @throws Exception
	 *           Any errors
	 */
	public String merge(final String templateName, final VelocityContext context) throws VelocityException {

		try {
			final StringWriter strWriter = new StringWriter();
			final boolean isMergeOk = Velocity.mergeTemplate(templateDir + "/" + templateName, Velocity.ENCODING_DEFAULT, context,
					strWriter);
			log.fine("   Velocity Merge OK: " + isMergeOk);
			return strWriter.toString();
		} catch(Exception e) {
			throw new VelocityException(e.getMessage(),e);
		}
			
			
	}
	
	/**
	 * 20110627 eanguan :: Re-factoring this function so that one can set their own Velocity Context if they want.
	 * In IDE test cases this is required.
	 * @param aggStr
	 * @return
	 * @throws RockException
	 * @throws SQLException
	 * @throws IOException
	 */
	public String CreateAggregationSql(final String aggStr) throws RockException, SQLException, IOException {
		final VelocityContext context = new VelocityContext();
		return CreateAggregationSql(aggStr, context);
	}

	/**
	 * Creates SQL command(s)
	 * 
	 * @param aggStr
	 *          The aggregation name
	 * @return The aggregation clause to store
	 * @throws Exception
	 *           Any errors
	 */
	public String CreateAggregationSql(final String aggStr, final VelocityContext vContext) throws RockException, SQLException, IOException {

		final VelocityContext context = vContext ;

		final Aggregation agga = new Aggregation(dwhrepRock);
		agga.setAggregation(aggStr); // e.g. DC_E_BSS_CELLBH_RANKBH_CELL_PP4
		agga.setVersionid(versionid); // e.g. DC_E_BSS:((12))
		final AggregationFactory aggf = new AggregationFactory(dwhrepRock, agga);
		final Aggregation agg = aggf.getElementAt(0);
		if (agg == null) {
			throw new SQLException("Aggregation " + aggStr + " not found in dwhrep");
		}

		log.finest("   Aggregation clause: " + aggStr);

		final Map<String, Vector<Measurementkey>> sourcemeaskeyMap = new HashMap<String, Vector<Measurementkey>>();
		final Map<String, Vector<Measurementkey>> targetmeaskeyMap = new HashMap<String, Vector<Measurementkey>>();
		final Map<String, Vector<Measurementcounter>> sourcemeascountMap = new HashMap<String, Vector<Measurementcounter>>();
		final Map<String, Vector<Measurementcounter>> targetmeascountMap = new HashMap<String, Vector<Measurementcounter>>();
		final Map<String, String> targetTableMap = new HashMap<String, String>();
		final Map<String, String> sourceTableMap = new HashMap<String, String>();
		final Map<String, String> bhtypeMap = new HashMap<String, String>();

		// Iterator iter = agg.getAggregationRules().iterator();
		final Aggregationrule aggrule = new Aggregationrule(dwhrepRock);
		aggrule.setAggregation(aggStr);
		aggrule.setVersionid(versionid);
		final AggregationruleFactory aggrulef = new AggregationruleFactory(dwhrepRock, aggrule);
		final List<Aggregationrule> aggRules = aggrulef.get();
		if (aggRules.isEmpty()) {
			log.info("No aggregation rules found for " + aggStr);
		}
		final Iterator<Aggregationrule> iter = aggRules.iterator();
		String bhAggType = null;

		while (iter.hasNext()) {

			final Aggregationrule aggRule = iter.next();

			// source table

			sourceTableMap.put(aggRule.getRuletype(), aggRule.getSource_table());

			final Measurementtable mts = new Measurementtable(dwhrepRock);
			mts.setMtableid(aggRule.getSource_mtableid());
			final MeasurementtableFactory mtsf = new MeasurementtableFactory(dwhrepRock, mts);
			final List<Measurementtable> qLists = mtsf.get();

			if (qLists != null && qLists.size() > 0) {
				for (Measurementtable mTbl : qLists) {
					final Measurementkey mkey = new Measurementkey(dwhrepRock);
					mkey.setTypeid(mTbl.getTypeid());
					final MeasurementkeyFactory mtypef = new MeasurementkeyFactory(dwhrepRock, mkey);
					sourcemeaskeyMap.put(aggRule.getRuletype(), mtypef.get());
				}
			}

			if (qLists != null && qLists.size() > 0) {
				for (Measurementtable mTbl : qLists) {
					final Measurementcounter mcount = new Measurementcounter(dwhrepRock);
					mcount.setTypeid(mTbl.getTypeid());
					final MeasurementcounterFactory mcountf = new MeasurementcounterFactory(dwhrepRock, mcount);
					sourcemeascountMap.put(aggRule.getRuletype(), mcountf.get());
				}

			}

			// target table

			targetTableMap.put(aggRule.getRuletype(), aggRule.getTarget_table());

			final Measurementtable mtt = new Measurementtable(dwhrepRock);
			mtt.setMtableid(aggRule.getTarget_mtableid());
			final MeasurementtableFactory mttf = new MeasurementtableFactory(dwhrepRock, mtt);
			final List<Measurementtable> qListt = mttf.get();

			if (qListt != null && qListt.size() > 0) {
				for (Measurementtable mTbl : qListt) {
					final Measurementkey mkey = new Measurementkey(dwhrepRock);
					mkey.setTypeid(mTbl.getTypeid());
					final MeasurementkeyFactory mtypef = new MeasurementkeyFactory(dwhrepRock, mkey);
					targetmeaskeyMap.put(aggRule.getRuletype(), mtypef.get());
				}

			}

			if (qListt != null && qListt.size() > 0) {
				for (Measurementtable mTbl : qListt) {
					final Measurementcounter mcount = new Measurementcounter(dwhrepRock);
					mcount.setTypeid(mTbl.getTypeid());
					final MeasurementcounterFactory mcountf = new MeasurementcounterFactory(dwhrepRock, mcount);
					targetmeascountMap.put(aggRule.getRuletype(), mcountf.get());
				}

			}

			final String bhtype;
			if (aggRule.getBhtype() == null) {
				bhtype = "";
			} else {
				bhtype = aggRule.getBhtype();
				if (aggRule.getAggregationscope().equals(Constants.DAYLEVEL)) {
					// only do this for the RANKBH_DAY
					// the RANKBH WEEK/MONTH still use the RANKBH_WEEK|MONTH.vm templates
					final Busyhour phWhere = new Busyhour(dwhrepRock);
					phWhere.setBhlevel(aggRule.getTarget_type());
					phWhere.setVersionid(aggRule.getVersionid());
					// bit of a hack this!
										
					final String bhType = bhtype.substring(bhtype.lastIndexOf('_') + 1);
					phWhere.setBhtype(bhType);
					
					final BusyhourFactory fac = new BusyhourFactory(dwhrepRock, phWhere);
					final List<Busyhour> bh = fac.get();
					if (bh.isEmpty()) {
						// default to this if nothing found??
						bhAggType = DEFAULT_RANKBH_TEMPLATE;
					} else {
						bhAggType = bh.get(0).getAggregationtype();
					}
				}
			}

			bhtypeMap.put(aggRule.getRuletype(), bhtype);

		}

		context.put("Bhtype", bhtypeMap);
		context.put("TargetTable", targetTableMap);
		context.put("SourceTable", sourceTableMap);
		context.put("TargetMeasurementKeyMap", targetmeaskeyMap);
		context.put("SourceMeasurementKeyMap", sourcemeaskeyMap);
		context.put("TargetMeasurementCounterMap", targetmeascountMap);
		context.put("SourceMeasurementCounterMap", sourcemeascountMap);

		final String templateToUse;
		if (bhAggType == null) {
			templateToUse = getTemplate(agg.getAggregationtype(), agg.getAggregationscope());
		} else {
			templateToUse = getTemplate(bhAggType, agg.getAggregationscope());
		}
		final String result = merge(templateToUse, context);
		log.finer("   SQL: " + result);

		return result;

	} // end CreateAggregationSql

	/**
	 * Get the GateKeeper SQL clause
	 * 
	 * @param aggStr
	 *          The aggregation name
	 * @return The gate keeper clause
	 * @throws Exception
	 *           Any errors
	 */
	private String getGateKeeperSQL(final String aggStr) throws RockException,SQLException {

		// list of all non rankbh aggregation sql clauses
		final List<String> sqlClause = new ArrayList<String>();

		// list of all rankbh tables. Actual sql clause is done elsewhere.
		final List<String> sqlClause_rankbh = new ArrayList<String>();

		final Aggregationrule aggr = new Aggregationrule(dwhrepRock);
		aggr.setAggregation(aggStr);
		aggr.setVersionid(versionid);
		final AggregationruleFactory aggrf = new AggregationruleFactory(dwhrepRock, aggr);
		final Iterator<Aggregationrule> iter = aggrf.get().iterator();

		boolean first = true;

		while (iter.hasNext()) {

			final Aggregationrule aggRule = (Aggregationrule) iter.next();

			// checks that aggregation itself is not aggregated
			if (first) {

				final String type = aggRule.getTarget_type();
				final String level = aggRule.getTarget_level();
				final String scope = aggRule.getAggregationscope();

				// check if this aggregation is already done
				sqlClause.add("SELECT count(*) result FROM LOG_AGGREGATIONSTATUS WHERE TYPENAME = '" + type
						+ "' AND TIMELEVEL = '" + level + "' AND DATADATE = $date AND AGGREGATIONSCOPE = '" + scope
						+ "' AND STATUS NOT IN ('AGGREGATED')");

				first = false;

			}

			// if the aggregation type is rankbh
			if ("RANKBH".equalsIgnoreCase(aggRule.getRuletype())) {

				// collect the sourcetables so we can create the check sql later
				sqlClause_rankbh.add(aggRule.getSource_table());

			} else
			// filters out week and month aggregations because we cannot check every
			// aggregation in week/month.
				//Now this block will work for DAY/WEEK/MONTH with respect to TR HU78433.
				//Reverting back the changes of TR: HU78433 for : EQEV-35405
				if (!aggRule.getSource_level().equalsIgnoreCase("RAW")  && !aggRule.getAggregationscope().equalsIgnoreCase("WEEK")
						&& !aggRule.getAggregationscope().equalsIgnoreCase("MONTH")) {

				// check dependencies on other aggregations (one step backwards)
				final String type = aggRule.getSource_type();
				final String level = aggRule.getSource_level();
				final String scope = aggRule.getAggregationscope();

				// create clause that checks that aggregation_rules and
				// aggregation_status have the same amount of aggregations
				// zero count in LOG_AggregationRules part of the sql closes the gate
				// also.
				// Modified the query for TR HU78433.
				final String tmpSqlClause = "select count(*) from (SELECT count(distinct aggregation) c  FROM LOG_AGGREGATIONSTATUS   WHERE typename = '"
						+ type
						+ "' AND timelevel = '"
						+ level
						+ "' AND DATADATE = $date and AGGREGATIONSCOPE = '"
						+ scope
						+ "' AND STATUS IN ('AGGREGATED') ) as a ,(select  count(distinct aggregation) c  from LOG_AggregationRules where target_type = '"
						+ type
						+ "' AND target_level = '"
						+ level
						+ "'  and aggregationscope = '"
						+ scope
						+ "') as b where a.c>=b.c and b.c <> 0 ";

				sqlClause.add(tmpSqlClause);

			} else if (aggRule.getSource_level().equalsIgnoreCase("RAW")) {

				// check is there raw data in active partition for aggregation day

				final String tmpSqlClause = "SELECT count(*) result FROM " + aggRule.getSource_table() + " WHERE DATE_ID = $date ";

				sqlClause.add(tmpSqlClause);

			} else if ((aggRule.getSource_level().equalsIgnoreCase("RANKBH") && aggRule.getAggregationscope().equalsIgnoreCase("WEEK") ) || 
				     (aggRule.getSource_level().equalsIgnoreCase("RANKBH") && aggRule.getAggregationscope().equalsIgnoreCase("MONTH"))){
                
			//For TR HO35036, HS29639 - Changing the gatekeeper action for week & Month so that they will run only after DAYBH
           String tmpSqlClause = "SELECT count(*) result FROM " + aggRule.getSource_table() + " WHERE DATE_ID = dateadd(dd, 6, $date) ";
           sqlClause.add(tmpSqlClause);
           
		} else if ((aggRule.getSource_level().equalsIgnoreCase("DAYBH") && aggRule.getAggregationscope().equalsIgnoreCase("WEEK")) ||
				    (aggRule.getSource_level().equalsIgnoreCase("DAYBH") && aggRule.getAggregationscope().equalsIgnoreCase("MONTH"))) {
			
			final String type = aggRule.getTarget_type();
			final String level = aggRule.getTarget_level();
			final String tmpSqlClause = "SELECT count(*) result FROM LOG_AGGREGATIONSTATUS WHERE TYPENAME = '" + type
					+ "' AND TIMELEVEL = '" + level + "' AND DATADATE = dateadd(dd ,6, $date) AND AGGREGATIONSCOPE = '"+"DAY"+"' AND STATUS IN ('AGGREGATED')";
			sqlClause.add(tmpSqlClause);
		}

		}

		String result = "";

		if (!sqlClause_rankbh.isEmpty()) {

			// we need a if statement to check the actual view before checking
			// the row count of a view

			for (String rsql : sqlClause_rankbh) {
				if (result.isEmpty()) {

					// first row
					// start of the if statement and first count check

					// loop all rank bh tables to create the if statement
					boolean firsttable = true;
					for (String table : sqlClause_rankbh) {

						if (firsttable) {
							// start of the if statement and first view check
							result += "IF (\nselect (SELECT  count(*) FROM SYSVIEWS WHERE viewname = '" + table
									+ "' AND vcreator='dc')\n";
							firsttable = false;
						} else {
							// Just add a view check
							result += " + (SELECT  count(*) FROM SYSVIEWS WHERE viewname = '" + table + "' AND vcreator='dc')\n";
						}
					}

					// end of the if statement check is done against the total
					// number of rankbh tables
					result += ") = " + sqlClause_rankbh.size() + " then\n";

					// if count matches we add the actual count checks for RANKBH
					// first actual count check
					result += "SELECT count(*) result FROM " + rsql + " WHERE DATE_ID = $date\n";
				} else {

					// add a rankbh count check with union all
					result += "\nUNION ALL\nSELECT count(*) result FROM " + rsql + " WHERE DATE_ID = $date\n";
				}
			}

			// add the non rankbh sql clauses
			for (String sql : sqlClause) {
				if (result.isEmpty()) {
					// first row
					result += sql + "\n";
				} else {
					result += "UNION ALL\n" + sql + "\n";
				}
			}

			// last row
			result += "else\n";
			// if we dont find all views the gate is closed so just return 0
			result += "select 0 result\n";
			result += "end if\n";

		} else {

			// no rankbh aggregation no need for if statement

			for (String sql : sqlClause) {
				if (result.isEmpty()) {
					// first row
					// add first sql
					result += sql + "\n";
				} else {
					// add a new sql with union all
					result += "UNION ALL\n" + sql + "\n";
				}
			}
		}

		return result;
	} // end getGateKeeperSQL			
	
	private String getGateKeeperSQLForSON(final String aggStr) throws RockException,SQLException {		
		// list of all non rankbh aggregation sql clauses		
		final List<String> sqlClause = new ArrayList<String>();				
		final Aggregationrule aggr = new Aggregationrule(dwhrepRock);		
		aggr.setAggregation(aggStr);		
		aggr.setVersionid(versionid);		
		final AggregationruleFactory aggrf = new AggregationruleFactory(dwhrepRock, aggr);		
		final Iterator<Aggregationrule> iter = aggrf.get().iterator();		
		boolean first = true;		
		while (iter.hasNext()) {			
			final Aggregationrule aggRule = (Aggregationrule) iter.next();			
			// checks that aggregation itself is not aggregated			
			if (first) {				
				final String type = aggRule.getTarget_type();				
				final String level = aggRule.getTarget_level();				
				final String scope = aggRule.getAggregationscope();				
				// check if this aggregation is already done				
				sqlClause.add("SELECT count(*) result FROM LOG_AGGREGATIONSTATUS WHERE TYPENAME = '" + type						
						+ "' AND TIMELEVEL = '" + level + "' AND AGGREGATIONSCOPE = '" + scope						
						+ "' AND STATUS NOT IN ('AGGREGATED')");				
				first = false;			
				}		
			}		
		String result = "";		for (String sql : sqlClause) {			
			if (result.isEmpty()) {				
				result += sql + "\n";			
			}else{				
				result += "UNION ALL\n" + sql + "\n";			
			}					
		}		
		return result;	
	} // end getGateKeeperSQL
			
	/**
	 * Parses template name
	 * 
	 * @param aggregationtype
	 *          The agg type
	 * @param aggregationscope
	 *          The agg scope
	 * @return template name
	 */
	protected String getTemplate(final String aggregationtype, final String aggregationscope) throws RockException, SQLException, IOException {
		return aggregationtype + "." + aggregationscope + ".vm";
	}

	public void create(final boolean skip) throws RockException, SQLException, IOException {
		create(skip, null);
	}

	public void create(final boolean skip, final List<String> skipMeasType) throws RockException, SQLException, IOException {
		create(skip, skipMeasType, null);
	}

	public void create(final boolean skip, final List<String> skipMeasType, final List<String> skipAggType) throws RockException,SQLException,IOException {

		final Aggregation aagg = new Aggregation(dwhrepRock);
		aagg.setVersionid(versionid);
		final AggregationFactory aggf = new AggregationFactory(dwhrepRock, aagg);
		final Iterator<Aggregation> iAgg = aggf.get().iterator();

		long iSet = 0;
		long iAction = 0;
		long order;
		long iScheduling = 0;
		String aggstr = "";

		try {
			final VelocityContext context = new VelocityContext();
			final StringWriter strWriter = new StringWriter();
			Velocity.mergeTemplate(templateDir + "/aggregator.vm", Velocity.ENCODING_DEFAULT, context, strWriter);
			aggstr = strWriter.toString();

		} catch (Exception e) {
			log.log(Level.WARNING, "Velocity error", e);
		}

		while (iAgg.hasNext()) {

			order = 0;
			final Aggregation agg = iAgg.next();

			String tablename = "";
			String tabletype = "";
			String aggType = "";

			final Properties aggregationProp = new Properties();

			// Iterator iter = agg.getAggregationRules().iterator();
			final Aggregationrule aggr = new Aggregationrule(dwhrepRock);
			aggr.setAggregation(agg.getAggregation());
			final AggregationruleFactory aggrf = new AggregationruleFactory(dwhrepRock, aggr);
			final Iterator<Aggregationrule> iter = aggrf.get().iterator();

			if (iter != null && iter.hasNext()) {
				final Aggregationrule aggRule = (Aggregationrule) iter.next();
				if (aggRule != null) {
					tablename = aggRule.getTarget_table();
					tabletype = aggRule.getTarget_type();
					aggType = aggRule.getTarget_level();
				}
			}

			aggregationProp.setProperty("tablename", tablename);
			final String name = aggstr + agg.getAggregation();

			if (skipMeasType != null && skipMeasType.contains(tabletype.toUpperCase())) {
				log.info("Skipped set " + name + " for " + tabletype);
				continue;
			}

			if (skipAggType != null && skipAggType.contains(aggType.toUpperCase())) {
				log.info("Skipped set " + name + " for aggregation type " + aggType);
				continue;
			}

			if (skip && Utils.isSet(name, version, techPackID, rock)) {
				log.info("Skipped set " + name);
				continue;
			}

			log.info("Creating set " + name);									
			createSet(name, "Aggregator", new Long(this.maxSetID + iSet).intValue()).insertToDB(rock);

			final Properties gk_where = new Properties();
			gk_where.setProperty("aggregation", agg.getAggregation());
			log.info("  Creating GateKeeper action for: " + name);			
			if(name.contains(Constants.SON15AGG)){				
				createAction("GateKeeper", order, "GateKeeper", new Long(this.maxSetID + iSet).intValue(),						
					new Long(this.maxActionID + iAction).intValue(), getGateKeeperSQLForSON(agg.getAggregation()),						
					Utils.propertyToString(gk_where)).insertToDB(rock);			
			}else{				
				createAction("GateKeeper", order, "GateKeeper", new Long(this.maxSetID + iSet).intValue(),						
					new Long(this.maxActionID + iAction).intValue(), getGateKeeperSQL(agg.getAggregation()),						
					Utils.propertyToString(gk_where)).insertToDB(rock);			
			}			
			iAction++;
			order++;

			log.info("  Creating Aggregation action: " + name);
			createAction(name, order, "Aggregation", new Long(this.maxSetID + iSet).intValue(),
					new Long(this.maxActionID + iAction).intValue(), CreateAggregationSql(agg.getAggregation()),
					Utils.propertyToString(aggregationProp)).insertToDB(rock);
			iAction++;				
			if (doSchedulings) {

				// create scheduling				
				final String holdFlag = "N";
				iScheduling++;

				log.info("  Creating schedule " + name);

				createWaitSchedule(new Long(this.maxSchedulingID + iScheduling).intValue(), this.techPackID,
						new Long(this.maxSetID + iSet).intValue(), name, holdFlag).insertToDB(this.rock);
			}

			iSet++;

		}

	} // create

	/**
	 * Removes the created sets.
	 * 
	 * @throws Exception
	 */
	public void removeSets() throws RockException,SQLException {
		removeSets(null);
	}

	/**
	 * Removes the created sets.
	 * 
	 * @throws Exception
	 */
	public void removeSets(final List<String> skipMeasTypes) throws RockException,SQLException {
		removeSets(skipMeasTypes, null);
	}

	/**
	 * Removes the created sets.
	 * 
	 * @param skiplist
	 *          The list of measurement and reference types to be skipped, i.e.
	 *          not removed.
	 * @throws Exception
	 *           Any errors
	 */
	public void removeSets(final List<String> skipMeasTypes, final List<String> skipAggTypes) throws RockException,SQLException {

		final Aggregation aagg = new Aggregation(dwhrepRock);
		aagg.setVersionid(versionid);
		final AggregationFactory aggf = new AggregationFactory(dwhrepRock, aagg);
		final Iterator<Aggregation> iAgg = aggf.get().iterator();

		String aggstr = "";

		try {
			final VelocityContext context = new VelocityContext();
			final StringWriter strWriter = new StringWriter();
			Velocity.mergeTemplate(templateDir + "/aggregator.vm", Velocity.ENCODING_DEFAULT, context, strWriter);
			aggstr = strWriter.toString();

		} catch (Exception e) {
			log.log(Level.WARNING, "Velocity error", e);
		}

		while (iAgg.hasNext()) {

			final Aggregation agg = iAgg.next();

			// Skip the already existing aggregations
			String tabletype = "";
			String aggtype = "";
			final Aggregationrule aggr = new Aggregationrule(dwhrepRock);
			aggr.setAggregation(agg.getAggregation());
			final AggregationruleFactory aggrf = new AggregationruleFactory(dwhrepRock, aggr);
			final Iterator<Aggregationrule> iter = aggrf.get().iterator();
			if (iter != null && iter.hasNext()) {

				final Aggregationrule aggRule = iter.next();
				if (aggRule != null) {
					tabletype = aggRule.getTarget_type();
					aggtype = aggRule.getTarget_level();
				}
			}
			if (skipMeasTypes != null && skipMeasTypes.contains(tabletype.toUpperCase())) {
				log.info("Skipped set " + name + " for " + tabletype);
				continue;
			}

			if (skipAggTypes != null && skipAggTypes.contains(aggtype.toUpperCase())) {
				log.info("Skippet set " + name + " for type " + aggtype);
				continue;
			}

			final String name = aggstr + agg.getAggregation();

			log.info("Removing set " + name);
			final long setid = Utils.getSetId(name, version, techPackID, rock);
			if (setid == -1) {
				log.info("No sets found, no need to remove");
				continue;
			}
			log.info("  Removing action: " + name);
			Utils.removeAction("GateKeeper", version, techPackID, setid, rock);

			log.info("  Removing action: " + name);
			Utils.removeAction(name, version, techPackID, setid, rock);

			// if (doSchedulings) {

			// create scheduling
			log.info("  Removing schedule " + name);
			Utils.removeScheduling(name, version, techPackID, setid, rock);
			// }

			Utils.removeSet(name, version, techPackID, rock);

		}
	}

	private SetGenerator createSet(final String name, final String type, final long iSet) {

		final SetGenerator set = new SetGenerator();
		set.setCOLLECTION_NAME(name);
		set.setCOLLECTION_ID(Long.toString(iSet));
		set.setMAX_ERRORS("0");
		set.setMAX_FK_ERRORS("0");
		set.setMAX_COL_LIMIT_ERRORS("0");
		set.setCHECK_FK_ERROR_FLAG("N");
		set.setCHECK_COL_LIMITS_FLAG("N");
		set.setVERSION_NUMBER(version);
		set.setCOLLECTION_SET_ID(Integer.toString(techPackID));
		set.setPRIORITY("0");
		set.setQUEUE_TIME_LIMIT("30");
		set.setENABLED_FLAG("Y");
		set.setSETTYPE(type);
		set.setFOLDABLE_FLAG("Y");
		set.setHOLD_FLAG("N");

		return set;

	}

	private ActionGenerator createAction(final String name, final long order, final String type, final long iSet,
			final int iAct, final String actionContents, final String where) {

		final ActionGenerator loadAction = new ActionGenerator();
		loadAction.setVERSION_NUMBER(version);
		loadAction.setTRANSFER_ACTION_ID(Long.toString(iAct));
		loadAction.setCOLLECTION_ID(Long.toString(iSet));
		loadAction.setCOLLECTION_SET_ID(Integer.toString(techPackID));
		loadAction.setACTION_TYPE(type);
		// loadAction.setTRANSFER_ACTION_NAME(type+"_"+agg.getComp_id().getAggregation());
		loadAction.setTRANSFER_ACTION_NAME(name);
		loadAction.setORDER_BY_NO(Long.toString(order));
		loadAction.setWHERE_CLAUSE(where);
		loadAction.setACTION_CONTENTS(actionContents);
		loadAction.setENABLED_FLAG("Y");
		loadAction.setCONNECTION_ID("2");
		return loadAction;

	}

	protected ScheduleGenerator createWaitSchedule(final long scheduleID, final long techpackID, final long setID,
			final String name, final String holdFlag) {

		final ScheduleGenerator schedule = new ScheduleGenerator();

		schedule.setVERSION_NUMBER(this.version);
		schedule.setID(scheduleID);
		schedule.setEXECUTION_TYPE("wait");
		schedule.setCOLLECTION_SET_ID(techpackID);
		schedule.setCOLLECTION_ID(setID);
		schedule.setNAME(name);
		schedule.setHOLD_FLAG(holdFlag);

		return schedule;
	}

}