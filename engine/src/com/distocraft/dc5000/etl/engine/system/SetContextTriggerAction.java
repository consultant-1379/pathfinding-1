package com.distocraft.dc5000.etl.engine.system;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.etl.scheduler.ISchedulerRMI;
import com.distocraft.dc5000.etl.scheduler.SchedulerConnect;

/**
 * 
 * Method triggers sets according rules and data retrieved from set context. <br>
 * Usually parsedMeastypes (HashSet) <br>
 * <br>
 * There can be more than one rule sets in actions. <br>
 * Rules set number zero is not added to the rule name (rule.key). <br>
 * Rule sets numbers from 1 are shown in rule name (ex. rule.1.key) <br>
 * <br>
 * ex1: <br>
 * <br>
 * rule.key = parsedMeastypes <br>
 * rule.contain = s1 <br>
 * rule.triggers = t1 <br>
 * <br>
 * rule.1.contain = s2 <br>
 * rule.1.triggers = t2 <br>
 * - triggers t1 if parsedMeasTypes contain s1 <br>
 * - triggers t2 if parsedMeasTypes contain s2 <br>
 * <br>
 * ex2. <br>
 * <br>
 * rule.key = parsedMeastypes <br>
 * rule.triggers = ALL <br>
 * rule.prefix = Loader_ <br>
 * <br>
 * - triggers all measurements listed in parsedMeastypes. Adds String 'Loader_' <br>
 * to the names of measurementtypes <br>
 * <br>
 * <br>
 * ex3. <br>
 * <br>
 * rule.key = anyObjectInSetContext <br>
 * rule.exists = true <br>
 * rule.triggers = t1 <br>
 * <br>
 * - triggers t1 if anyObjectInSetContext object is in set context <br>
 * <br>
 * <br>
 * <br>
 * -------------------------------------------------------------------- <br>
 * <br>
 * rule[.num].key = objectName: <br>
 * <br>
 * - is the mapKey that retrieves the required object from set context
 * (parsebleFiles,parsedMeasTypes) <br>
 * - parsebleFiles (Integer) contain number of parseable file in INdirectory. <br>
 * - parsedMeasTypes (HashSet) contains list of measurements that adapter has
 * handled <br>
 * rule[.num].exists (Boolean) <br>
 * <br>
 * - object retrieved from content is treated as object <br>
 * - checks if key defined in rule[.num].key exists (true) or does not exists
 * (false) in set context <br>
 * rule[.num].contain = str1,str2,str3 (HashSet) <br>
 * <br>
 * - object retrieved from content is treated as HashSet <br>
 * - list (1 or more) comma delimited strings that are searched from HashTable. <br>
 * if all strings are found from HashTable, scheduled sets (listed in property
 * rule.triggers) are triggered from scheduler <br>
 * rule[.num].more (Integer) <br>
 * <br>
 * - object retrieved from content is treated as Integer <br>
 * - checks if Integer from setContext is larger than given value <br>
 * rule[.num].equal (Integer) <br>
 * <br>
 * - object retrieved from content is treated as Integer <br>
 * - checks if Integer from setContext is equal than given value <br>
 * rule[.num].less (Integer) <br>
 * <br>
 * - object retrieved from content is treated as Integer <br>
 * - checks if Integer from setContext is smaller than given value <br>
 * rule[.num].triggers <br>
 * <br>
 * - List of set names that are triggered if one of the conditions
 * (contains,more,equal,less) is true <br>
 * - if triggers value is 'ALL' all of the elements from HashSet defined in key
 * are listed in triggered list (and triggered if condition matches) <br>
 * rule[.num].prefix <br>
 * <br>
 * - prefix that is added to the trigered sets name <br>
 * <br>
 * <br>
 */
public class SetContextTriggerAction extends TransferActionBase {

	private final Logger log;

	private SetContext sctx;

	private String where;
	
	/**
	 * Empty public constructor
	 * 
	 */
	public SetContextTriggerAction() {
		this.log = Logger.getLogger("etlengine.SetContextTriggerAction");
	}

	/**
	 * Constructor
	 * 
	 * @param versionNumber
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
	 * 
	 */
	public SetContextTriggerAction(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory rockFact, final Meta_transfer_actions trActions, final SetContext sctx)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		this.sctx = sctx;
		this.where = trActions.getWhere_clause();

		try {
			final Meta_collection_sets whereCollSet = new Meta_collection_sets(rockFact);
			whereCollSet.setEnabled_flag("Y");
			whereCollSet.setCollection_set_id(collectionSetId);
			final Meta_collection_sets collSet = new Meta_collection_sets(rockFact, whereCollSet);

			final String tech_pack = collSet.getCollection_set_name();
			final String set_type = collection.getSettype();
			final String set_name = collection.getCollection_name();

			this.log = Logger.getLogger("etl." + tech_pack + "." + set_type + "." + set_name
					+ ".action.SetContextTriggerAction");

		} catch (Exception e) {
			throw new EngineMetaDataException("ExecuteSetAction unable to initialize loggers", e, "init");
		}

	}

	public boolean isEqual(final Object str1, final String str2) {

		if (str1 != null) {

			try {

				// is it Boolean

				final boolean tmp = ((Boolean) str1).booleanValue();

				if (str2.equalsIgnoreCase("true")) {

					return tmp == "true".equalsIgnoreCase(str2);
				} else if (str2.equalsIgnoreCase("false")) {

					return tmp == "true".equalsIgnoreCase(str2);
				} else {

					throw new Exception("not boolean");
				}

			} catch (Exception e1) {
				// no Boolean
				try {

					final Integer i = (Integer) str1;

					// is it Integer
					return i.intValue() == Integer.parseInt(str2);

				} catch (Exception e2) {

					// no Integer
					try {

						// is it string
						return ((String) str1).equalsIgnoreCase(str2);

					} catch (Exception e3) {

						return false;
					}
				}
			}

		} else {
			return false;
		}

	}

	public boolean isMore(final Integer i, final String str2) {

		if (i != null && str2 != null && !str2.equalsIgnoreCase("")) {
			return (i.intValue()) < Integer.parseInt(str2);
		} else {
			return false;
		}

	}

	public boolean isLess(final Integer i, final String str2) {

		if (i != null && str2 != null && !str2.equalsIgnoreCase("")) {
			return (i.intValue()) > Integer.parseInt(str2);
		} else {
			return false;
		}

	}

	public void trigger(final List<String> list, final String prefix) {

		if (list != null) {

			String triggerName = null;
			try {

				final ISchedulerRMI scheduler = connect();
								
				if (!list.isEmpty()) {

					final Iterator<String> iter = list.iterator();
					while (iter.hasNext()) {
						triggerName = iter.next();
						if (prefix != null) {
							triggerName = prefix + triggerName;
						}

						scheduler.trigger(triggerName);
					}
				}

			} catch (Exception e) {

				// could not start trigger in scheduler
				log.warning("Could not start trigger " + triggerName + " in scheduler");
			}
		}

	}

	public boolean contains(final Set<String> set, final String content) {

		if (set != null) {

			List<String> list = null;

			if (content != null) {

				// list content
				list = new ArrayList<String>();
				final StringTokenizer contentTokens = new StringTokenizer(content, ",");
				while (contentTokens.hasMoreTokens()) {
					list.add(contentTokens.nextToken());
				}
			}

			if (list != null && !list.isEmpty()) {
				return set.containsAll(list);
			} else {
				return false;
			}

		} else {
			return false;
		}

	}

	public List<String> strToList(final String str) {

		final List<String> list = new ArrayList<String>();

		if (str != null) {

			// list all triggers
			final StringTokenizer triggerTokens = new StringTokenizer(((String) str), ",");
			while (triggerTokens.hasMoreTokens()) {
				list.add(triggerTokens.nextToken());
			}
		}

		return list;
	}

	public List<String> setToList(final Set<String> set) {

		final List<String> list = new ArrayList<String>();
		list.addAll(set);

		return list;
	}

	public void execute() throws EngineException {

		log.finest("Starting SetContextTriggerAction");

		try {

			final Properties properties = TransferActionBase.stringToProperties(where);

			int ruleNum = 0;
			String key = null;

			do {

				final String oldKey = key;
				key = null;
				String content = null;
				String triggers = null;
				List<String> triggerList = new ArrayList<String>();
				String less = null;
				String more = null;
				String equals = null;
				String prefix = "";
				String exists = null;

				try {
					String cmd = "";
					if (ruleNum == 0) {
						cmd = "rule";
					} else {
						cmd = "rule." + ruleNum;
					}

					key = properties.getProperty(cmd + ".key");
					content = properties.getProperty(cmd + ".contain");
					more = properties.getProperty(cmd + ".more");
					equals = properties.getProperty(cmd + ".equal");
					less = properties.getProperty(cmd + ".less");
					triggers = properties.getProperty(cmd + ".triggers");
					prefix = properties.getProperty(cmd + ".prefix");
					exists = properties.getProperty(cmd + ".exists");

					ruleNum++;

					if (triggers == null) {
						break;
					}

					if (key == null) {
						if (oldKey != null) {
							key = oldKey;
						} else {
							break;
						}
					}

					boolean trigger = true;
					if (triggers.equalsIgnoreCase("ALL")) {

						triggerList = setToList((Set<String>) sctx.get(key));

					} else {

						triggerList = strToList(triggers);

					}

					if ("true".equalsIgnoreCase(exists)) {
						if (sctx.containsKey(key)) {
							trigger = true;
						}

					} else if ("false".equalsIgnoreCase(exists)) {
						if (!sctx.containsKey(key)) {
							trigger = true;
						}

					} else if (content != null) {

						trigger = contains((Set<String>) sctx.get(key), content);
					} else if (equals != null) {

						trigger = isEqual(sctx.get(key), equals);
					} else if (less != null) {

						trigger = isLess((Integer) sctx.get(key), less);
					} else if (more != null) {

						trigger = isMore((Integer) sctx.get(key), more);
					}

					if (trigger) {
						trigger(triggerList, prefix);
					}

				} catch (Exception e) {
					log.warning(" Error while reading properties " + where);
				}

			} while (key != null);

		} catch (Exception e) {
			log.log(Level.WARNING, " Error In SetContextTriggerAction ", e);

			throw new EngineException("Exception in SetContextTriggerAction", new String[] { "" }, e, this, this.getClass()
					.getName(), EngineConstants.ERR_TYPE_SYSTEM);

		}

	}

	/**
	 * Return scheduler RMI object. This method is implemented for to be
	 * overwritten in unit tests.
	 */
	protected ISchedulerRMI connect() throws IOException, NotBoundException {
		return SchedulerConnect.connectScheduler();
	}

}
