package com.distocraft.dc5000.etl.engine.system;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

public class ChangeProfileAction extends TransferActionBase {

	private final Logger log;
	private final Properties props;
	
	/**
	 * Empty protected constructor
	 * 
	 */
	protected ChangeProfileAction() {
		this.log = Logger.getLogger("");
		this.props = new Properties();
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
	public ChangeProfileAction(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory rockFact, final Meta_transfer_actions trActions) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		try {
			final Meta_collection_sets whereCollSet = new Meta_collection_sets(rockFact);
			whereCollSet.setEnabled_flag("Y");
			whereCollSet.setCollection_set_id(collectionSetId);
			final Meta_collection_sets collSet = new Meta_collection_sets(rockFact, whereCollSet);

			final String tech_pack = collSet.getCollection_set_name();
			final String set_type = collection.getSettype();
			final String set_name = collection.getCollection_name();

			this.log = Logger.getLogger("etl." + tech_pack + "." + set_type + "." + set_name + ".action.ChangeProfileAction");

			this.props = TransferActionBase.stringToProperties(trActions.getAction_contents());
			
		} catch (Exception e) {
			throw new EngineMetaDataException("ExecuteSetAction unable to initialize loggers", e, "init");
		}

	}

	public void execute() throws EngineException {

		try {

			final String profileName = props.getProperty("profileName", "");
			final String forbidenTypes = props.getProperty("forbidenTypes", "");

			final List<String> forbidenTypesList = new ArrayList<String>();
			final StringTokenizer token = new StringTokenizer(forbidenTypes, ",");

			while (token.hasMoreTokens()) {
				forbidenTypesList.add(token.nextToken());
			}

			// not empty
			if (!profileName.equalsIgnoreCase("")) {

				final EngineAdmin admin = new EngineAdmin();

				log.info("changing to profile" + profileName);

				final boolean result = admin.changeProfile(profileName);

				// if profile change is succesful
				if (result) {

					// if no forbiden types defined no need to wait anything.
					if (!forbidenTypesList.isEmpty()) {

						boolean waiting = true;

						log.info("Waiting for forbiden set types to clear from execution slots.. ");
						
						while (waiting) {

							// no forbiden setTypes found so free to end..
							waiting = false;

							// sleep half second...
							Thread.sleep(500);

							// Get execution slots
							final Set<String> tmpVec = admin.getAllActiveSetTypesInExecutionProfiles();

							// if no sets in execution slots we can exit...
							if (tmpVec.isEmpty()) {
								break;
							}

							final Iterator<String> slotIter = tmpVec.iterator();

							// loop all execution slots or untill forbiden type is found
							while (slotIter.hasNext() && !waiting) {

								final String setType = (String) slotIter.next();

								// get forbiden types
								final Iterator<String> forbidenTypesIter = forbidenTypesList.iterator();

								// check for forbiden types in slot
								while (forbidenTypesIter.hasNext()) {

									final String forbidenType = forbidenTypesIter.next();

									if (forbidenType.equalsIgnoreCase(setType)) {

										// we found one forbiden setType so we wait somem more
										waiting = true;
										break;
									}

								}
							}
						}

					}
					log.info("No forbiden set types in execution slots..");

				} else {
					log.warning("Could not change to profile: " + profileName);
				}
			}
		} catch (Exception e) {
			throw new EngineException("Exception in ChangeProfileAction", new String[] { "" }, e, this, this.getClass()
					.getName(), EngineConstants.ERR_TYPE_SYSTEM);

		}

	}

}
