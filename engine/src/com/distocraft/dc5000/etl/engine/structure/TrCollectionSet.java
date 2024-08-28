package com.distocraft.dc5000.etl.engine.structure;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineCom;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.RemoveDataException;
import com.distocraft.dc5000.etl.engine.plugin.PluginLoader;
import com.distocraft.dc5000.etl.engine.priorityqueue.PriorityQueue;
import com.distocraft.dc5000.etl.engine.system.SetListener;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.etl.rock.Meta_versionsFactory;

/**
 * A class for transfer collection set . A starting point for a transfer.
 * 
 * @author Jukka Jaaheimo
 */
public class TrCollectionSet {

	private final Meta_versions version;
	private final String versionNumber;

	private final Long collectionSetId;
	private final String collectionSetName;

	private final Long collectionId;

	private final RockFactory rockFact;

	private final List<TrCollection> trCollections;

	private final PluginLoader pLoader;

	// is this techpack enabled
	private final String isEnabled;

	private final EngineCom eCom;

	private TrCollection currentCollection = null;

	private final String slotType;
	
	/**
	 * Constructor for starting the transfer
	 * 
	 * @param rockFact
	 *          the database connection for the metadata
	 * @param collectionSetName
	 *          the name of the transfer collection set
	 * @param collectionName
	 *          the name of the transfer collection
	 */
	public TrCollectionSet(final RockFactory rockFact, final String collectionSetName, final String collectionName,
			final PluginLoader pLoader, final EngineCom eCom, final PriorityQueue pq, final Integer slotId,
			final String schedulingInfo, final String slotType) throws EngineMetaDataException {

		this.eCom = eCom;
		this.collectionSetName = collectionSetName;
		this.rockFact = rockFact;
		this.pLoader = pLoader;
    this.slotType = slotType;

		try {

			final Meta_versions whereVersion = new Meta_versions(rockFact);
			whereVersion.setCurrent_flag("Y");
			final Meta_versionsFactory mF = new Meta_versionsFactory(rockFact, whereVersion);
			this.version = mF.getElementAt(0);

			final Meta_collection_sets whereCollSet = new Meta_collection_sets(rockFact);
			whereCollSet.setEnabled_flag("Y");
			whereCollSet.setCollection_set_name(collectionSetName);
			final Meta_collection_sets collSet = new Meta_collection_sets(rockFact, whereCollSet);
			this.collectionSetId = collSet.getCollection_set_id();

			this.versionNumber = collSet.getVersion_number();
			this.isEnabled = collSet.getEnabled_flag();

			if (collectionName != null) {
				final Meta_collections whereColl = new Meta_collections(rockFact);
				whereColl.setVersion_number(versionNumber);
				whereColl.setCollection_set_id(this.collectionSetId);
				whereColl.setCollection_name(collectionName);
				final Meta_collections coll = new Meta_collections(rockFact, whereColl);
				this.collectionId = coll.getCollection_id();
			} else {
				this.collectionId = null;
			}

			this.trCollections = getTrCollections(pq, slotId, schedulingInfo);

		} catch (SQLException se) {
			throw new EngineMetaDataException("Unable to init set, metadata query failed", se, "constructor");
		} catch (RockException re) {
			throw new EngineMetaDataException("Unable to init set, metadata query failed", re, "constructor");
		} catch (EngineMetaDataException em) {
			throw em;
		}

	}

	/**
	 * Creates executed transfer collection objects
	 */
	private List<TrCollection> getTrCollections(final PriorityQueue pq, final Integer slotId, final String schedulingInfo)
			throws RockException, SQLException, EngineMetaDataException {

		final List<TrCollection> ret = new ArrayList<TrCollection>();

		final Meta_collections whereColl = new Meta_collections(this.rockFact);
		whereColl.setVersion_number(this.versionNumber);
		whereColl.setCollection_set_id(this.collectionSetId);
		whereColl.setCollection_id(this.collectionId);
		final Meta_collectionsFactory dbCollections = new Meta_collectionsFactory(this.rockFact, whereColl);

		final List<Meta_collections> dbVec = dbCollections.get();

		for (Meta_collections dbTrCollection : dbVec) {
			dbTrCollection.setScheduling_info(schedulingInfo);
			final TrCollection trCollection = new TrCollection(this.rockFact, this.version, this.collectionSetId,
					dbTrCollection, this.pLoader, eCom, pq, slotId, slotType);
			ret.add(trCollection);
		}

		return ret;
	}

	/**
	 * Returns the collection of a given name
	 * 
	 * @param name
	 * @return
	 */
	public TrCollection getCollection(final String name) {
		TrCollection ret = null;

		for (TrCollection trCollection : trCollections) {
			if (trCollection.getName().endsWith(name)) {
				ret = trCollection;
				break;
			}
		}

		return ret;
	}

	/**
	 * Executes all transfer collections of this collection set
	 * TODO: Proper exception should be thrown
	 * @param SetListener
	 */
	public void executeSet(final SetListener setListener) throws Exception {
		for (TrCollection trCollection : trCollections) {
			if (trCollection.getEnabledFlag().equalsIgnoreCase("y") && trCollection.getHoldFlag().equalsIgnoreCase("n")) {
				currentCollection = trCollection;
				trCollection.execute(setListener); // EXECUTE SET
				currentCollection = null;
			} else {
				final Logger slog = Logger.getLogger("etl." + collectionSetName + "." + trCollection.getSettype() + "."
						+ trCollection.getName());

				if (!trCollection.getEnabledFlag().equalsIgnoreCase("y")) {
					slog.info("Execution cancelled: Set " + trCollection.getName() + " is disabled");
				}

				if (!trCollection.getHoldFlag().equalsIgnoreCase("n")) {
					slog.info("Execution cancelled: Set " + trCollection.getName() + " is on hold");
				}

			}

		}

	}

	public int cleanSet() throws EngineMetaDataException, RemoveDataException {
		int count = 0;

		for (TrCollection trCollection : trCollections) {
			if (trCollection.getEnabledFlag().equalsIgnoreCase("y") && trCollection.getHoldFlag().equalsIgnoreCase("n")) {
				count += trCollection.cleanCollection();
			}
		}

		return count;
	}

	/**
	 * return true is collection set is enabled.
	 * 
	 * @return
	 */
	public boolean isEnabled() {
		return this.isEnabled.equalsIgnoreCase("y");
	}

	public TrCollection getCurrentCollection() {
		return currentCollection;
	}

}
