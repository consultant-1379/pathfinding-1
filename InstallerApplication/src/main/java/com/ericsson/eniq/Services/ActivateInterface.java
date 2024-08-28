package com.ericsson.eniq.Services;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import ssc.rockfactory.RockFactory;


import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_schedulings;
import com.distocraft.dc5000.etl.rock.Meta_schedulingsFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.distocraft.dc5000.repository.dwhrep.Dataformat;
import com.distocraft.dc5000.repository.dwhrep.DataformatFactory;
import com.distocraft.dc5000.repository.dwhrep.Datainterface;
import com.distocraft.dc5000.repository.dwhrep.DatainterfaceFactory;
import com.distocraft.dc5000.repository.dwhrep.Defaulttags;
import com.distocraft.dc5000.repository.dwhrep.DefaulttagsFactory;
import com.distocraft.dc5000.repository.dwhrep.Interfacemeasurement;
import com.distocraft.dc5000.repository.dwhrep.InterfacemeasurementFactory;
import com.distocraft.dc5000.repository.dwhrep.Interfacetechpacks;
import com.distocraft.dc5000.repository.dwhrep.InterfacetechpacksFactory;
import com.distocraft.dc5000.repository.dwhrep.Tpactivation;
import com.distocraft.dc5000.repository.dwhrep.TpactivationFactory;
import com.distocraft.dc5000.repository.dwhrep.Transformer;
import com.distocraft.dc5000.repository.dwhrep.TransformerFactory;
import com.distocraft.dc5000.repository.dwhrep.Typeactivation;
import com.distocraft.dc5000.repository.dwhrep.TypeactivationFactory;




/**
 * This is custom made ANT task that activates an interface and copies interface's set to the activated interface.
 * 
 * @author Berggren
 * 
 */
@Component
public class ActivateInterface{

	private static final String START_OF_INTERFACE_NAME = "INTF_";

	private static final String START_OF_MZ_INTERFACE_NAME = "M_";

	private String activatedInterfaceName = "";

	private String ossName = "";

	private RockFactory dwhrepRockFactory = null;

	private RockFactory etlrepRockFactory = null;


	private String onlyActivateInterface = "";

	private final int maxRetries = 3;

	private final int retryPeriod_in_seconds = 60;

	private final int retryRandom_in_seconds = 120;
	
	private String tpName="";
	private String tpDirectory="";

	int error = 0;

	int retry = 1;
	
	private static final Logger logger = LogManager.getLogger(ActivateInterface.class);

	/**
	 * This function starts the interface activation.
	 */
	
	public ActivateInterface(final String tpName,String ossName)
	{
		this.activatedInterfaceName=tpName;
		this.ossName=ossName;
	}
	
	public ActivateInterface()
	{
		
	}

	public void execute() throws Exception {
			
		try {

		//	final ITransferEngineRMI engineRMI = connectEngine();
			if (this.activatedInterfaceName.startsWith(START_OF_MZ_INTERFACE_NAME)) {
				logger.info("Skipping activation of interface " + this.activatedInterfaceName
						+ " as it is a Mediation Zone interface.");
				return;
			}

			if (!this.activatedInterfaceName.startsWith(START_OF_INTERFACE_NAME)) {
				logger.info("Skipping activation of " + this.activatedInterfaceName
						+ " as it is not an interface.");
				return;
			}

			logger.info("Checking connection to database");
			GetDatabaseDetails getdb=new GetDatabaseDetails();
			
			final Map<String, String> databaseConnectionDetails =getdb.getDatabaseConnectionDetails();
			
			
			this.etlrepRockFactory = getdb.createEtlrepRockFactory(databaseConnectionDetails);

			
			this.dwhrepRockFactory=getdb.createDwhrepRockFactory(this.etlrepRockFactory);

			logger.info("Connections to database created.");
			
			logger.info("Starting activation of interface " + this.activatedInterfaceName);
			// Activate the interface.
			
			
			this.activateInterface();

			logger.info("Interface " + this.activatedInterfaceName + " activated");

			if (this.onlyActivateInterface.equalsIgnoreCase("true")) {
				logger.info("Only activation of interface selected. No sets will be copied.");
			} /*else {
				// Copy the tech pack set.
				logger.info("Starting copying of interface set " + this.activatedInterfaceName);
				final boolean copyIntfSetResult = this.copyInterfaceSet(engineRMI);

				if (copyIntfSetResult) {
					logger.info("Interface set " + this.activatedInterfaceName + " copied");
					this.checkNetAnFeature();
					createAlarmDir();
					createBulkcmMappingDir();
					
				} else {
					logger.info("Interface set " + this.activatedInterfaceName + " was not copied");
				}

				
			}*/
		} catch (final Exception e) {
			throw new Exception("InterfaceActivation failed.", e);
		}finally{
			
				try {
					if (this.dwhrepRockFactory != null) {
						this.dwhrepRockFactory.getConnection().close();
					}
					
				} catch (SQLException e) {
				}
			
			
				try {
					if (this.etlrepRockFactory != null) {
						this.etlrepRockFactory.getConnection().close();
					}
					
				} catch (SQLException e) {
				
			}
		}

	}
	
	
		



	public String getActivatedInterfaceName() {
		return activatedInterfaceName;
	}

	public void setActivatedInterfaceName(final String activatedInterfaceName) {
		this.activatedInterfaceName = activatedInterfaceName;
	}

	public String getOssName() {
		return ossName;
	}

	public void setOssName(final String ossName) {
		this.ossName = ossName;
	}

	/**
	 * This function activates interface.
	 * 
	 * @throws Exception
	 *             On Errors
	 * @return TRUE is the interface was activated, FALSE otherwise
	 */
	private boolean activateInterface() throws Exception {

		try {

			// Get the dataformattype of this interface.
			final Datainterface whereDataInterface = new Datainterface(this.dwhrepRockFactory);
			whereDataInterface.setInterfacename(this.activatedInterfaceName);
			final DatainterfaceFactory dataInterfaceFactory = new DatainterfaceFactory(this.dwhrepRockFactory,
					whereDataInterface);
			final Vector<Datainterface> dataInterfaceVector = dataInterfaceFactory.get();

			final String dataFormatType;

			final String activatedInterfaceVersion;
			if ((dataInterfaceVector != null) && (dataInterfaceVector.size() > 0)) {
				final Datainterface targetDataInterface = dataInterfaceVector.get(0);
				dataFormatType = targetDataInterface.getDataformattype();
				activatedInterfaceVersion = targetDataInterface.getInterfaceversion();
				if (dataFormatType == null) {
					throw new Exception("DataFormatType was null. Interface activation cannot continue.");
				}

			} else {
				throw new Exception("Dataformat type not found for interface " + this.activatedInterfaceName
						+ ". Interface activation cannot continue.");
			}

			// Get tech packs related to this interface.
			final Interfacetechpacks whereInterfaceTechPacks = new Interfacetechpacks(this.dwhrepRockFactory);
			whereInterfaceTechPacks.setInterfacename(this.activatedInterfaceName);
			final InterfacetechpacksFactory interfaceTechPacksFactory = new InterfacetechpacksFactory(
					this.dwhrepRockFactory, whereInterfaceTechPacks);
			final Vector<Interfacetechpacks> interfaceTechPacks = interfaceTechPacksFactory.get();
			if (interfaceTechPacks == null) {
				throw new Exception("Tech packs related to this interface " + this.activatedInterfaceName
						+ " were not found. Interface activation can not continue.");
			}

			// HashMap containing dataformat entries.
			// Key: uniqueId contains of tagid from the DefaultTags table + "#" +
			// dataformatid from DataFormat table.
			// Value: RockObject of DataFormat entry.
			final Map<String, Dataformat> dataFormats = new HashMap<String, Dataformat>();

			for (final Interfacetechpacks currentTechPack : interfaceTechPacks) {
				final String techPackName = currentTechPack.getTechpackname();
				// Get the activated tech pack activation.
				final Tpactivation whereTPActivation = new Tpactivation(this.dwhrepRockFactory);
				whereTPActivation.setTechpack_name(techPackName);
				whereTPActivation.setStatus("ACTIVE");
				final TpactivationFactory tpActivationFactory = new TpactivationFactory(this.dwhrepRockFactory,
						whereTPActivation);
				final Vector<Tpactivation> tpActivations = tpActivationFactory.get();
				if ((tpActivations == null) || (tpActivations.size() == 0)) {
					System.out
							.println("Tech pack "
									+ techPackName
									+ " activation for interface "
									+ this.activatedInterfaceName
									+ " was not found. "
									+ techPackName
									+ " specific measurements can be added after TP is installed with reactivation of this interface.");
					continue;
				}

				for (final Tpactivation currentTpActivation : tpActivations) {
					// VersionId is used to map TypeActivation entries to table
					// DataFormat.
					final String techPackVersionId = currentTpActivation.getVersionid();

					// Get the TypeActivations of this TPActivation.
					final Typeactivation whereTypeActivation = new Typeactivation(this.dwhrepRockFactory);
					whereTypeActivation.setTechpack_name(techPackName);
					final TypeactivationFactory typeActivationFactory = new TypeactivationFactory(
							this.dwhrepRockFactory, whereTypeActivation);
					final Vector<Typeactivation> typeActivations = typeActivationFactory.get();
					if (typeActivations == null) {
						logger.info("Type activations for tech pack " + techPackName + " for interface "
								+ this.activatedInterfaceName + " were not found.");
						continue;
					}

					for (final Typeactivation currentTypeActivation : typeActivations) {
						final String typeName = currentTypeActivation.getTypename();
						// TypeId in table DataFormat is in format
						// VERSIONID:TYPENAME:DATAFORMATTYPE.
						final String DataFormatTypeId = techPackVersionId + ":" + typeName + ":" + dataFormatType;

						logger.info("Looking for entries with dataformatid: " + DataFormatTypeId);

						final Dataformat whereDataFormat = new Dataformat(this.dwhrepRockFactory);
						whereDataFormat.setDataformatid(DataFormatTypeId);
						final DataformatFactory dataFormatFactory = new DataformatFactory(this.dwhrepRockFactory,
								whereDataFormat);
						final Vector<Dataformat> dataFormatVector = dataFormatFactory.get();
						if (dataFormatVector == null) {
							logger.info("Dataformat type " + typeName + " of " + techPackName
									+ " was not found.");
							continue;
						}

						for (final Dataformat currentDataFormat : dataFormatVector) {
							// Get the tagid's used by this dataformat.
							final Defaulttags whereDefaultTags = new Defaulttags(this.dwhrepRockFactory);
							whereDefaultTags.setDataformatid(currentDataFormat.getDataformatid());
							final DefaulttagsFactory defaultTagsFactory = new DefaulttagsFactory(
									this.dwhrepRockFactory, whereDefaultTags);
							final Vector<Defaulttags> defaultTagsVector = defaultTagsFactory.get();
							if (defaultTagsVector == null) {
								logger.info("Default tags ids of " + currentDataFormat.getDataformatid()
										+ " of " + techPackName + " was not found.");
								continue;
							}

							for (final Defaulttags currentDefaultTag : defaultTagsVector) {
								// Found related dataformat with unique tagid. Add it to
								// comparable dataformat entries.

								logger.info("Adding dataFormat: " + currentDefaultTag.getTagid() + "#"
										+ currentDataFormat.getDataformatid());

								dataFormats.put(
										currentDefaultTag.getTagid() + "#" + currentDataFormat.getDataformatid(),
										currentDataFormat);
							}
						}
					}
				}
			}
			if (dataFormats.size() == 0) {
				throw new Exception("No Tech packs were activated for interface " + this.activatedInterfaceName
						+ ". Interface activation can not proceed.");
			}

			// At this point dataformat entries are collected to dataFormats hashmap.
			// Remove the old entries from InterfaceMeasurement if they exist.
			final Interfacemeasurement whereInterfaceMeasurement = new Interfacemeasurement(this.dwhrepRockFactory);
			whereInterfaceMeasurement.setInterfacename(this.activatedInterfaceName);
			final InterfacemeasurementFactory interfaceMeasurementFactory = new InterfacemeasurementFactory(
					this.dwhrepRockFactory, whereInterfaceMeasurement);
			final Vector<Interfacemeasurement> interfaceMeasurementsVector = interfaceMeasurementFactory.get();
			if (interfaceMeasurementsVector == null) {
				logger.info("Measurements for this interface " + this.activatedInterfaceName
						+ " were not found.");
			} else {
				for (final Interfacemeasurement currentInterfaceMeasurement : interfaceMeasurementsVector) {
					currentInterfaceMeasurement.deleteDB();
					logger.info("Removed old InterfaceMeasurement "
							+ currentInterfaceMeasurement.getDataformatid());
				}
			}

			final Date currentTime = new Date();
			final Timestamp currentTimeTimestamp = new Timestamp(currentTime.getTime());

			// Start inserting the values collected from DataFormat table.
			final Set<String> dataFormatsSet = dataFormats.keySet();

			for (final String uniqueId : dataFormatsSet) {
				final Dataformat currentDataFormat = dataFormats.get(uniqueId);
				final String currentTagId = uniqueId.substring(0, uniqueId.indexOf("#"));

				// Get the defaultTag for this dataformat from DefaultTags table.
				// Long tagId = new Long(0);

				final Defaulttags whereDefaultTag = new Defaulttags(this.dwhrepRockFactory);
				whereDefaultTag.setDataformatid(currentDataFormat.getDataformatid());
				final DefaulttagsFactory defaultTagsFactory = new DefaulttagsFactory(this.dwhrepRockFactory,
						whereDefaultTag);
				final Vector<Defaulttags> defaultTagsVector = defaultTagsFactory.get();
				if (defaultTagsVector == null) {
					logger.info("Getting default tags for current dataformat " + currentDataFormat
							+ " this interface " + this.activatedInterfaceName
							+ " did not success. Interface activation cannot continue.");
					continue;
				}
				final String description;
				if (defaultTagsVector.size() == 0) {
					logger.info("No tagid found for dataformat " + currentDataFormat.getDataformatid());
					return false;
				} else {
					final Defaulttags targetDefaultTag = defaultTagsVector.get(0);
					// tagId = targetDefaultTag.getTagid();
					description = targetDefaultTag.getDescription();
				}

				// Create a new row to table InterfaceMeasurement.
				final Interfacemeasurement newInterfaceMeasurement = new Interfacemeasurement(this.dwhrepRockFactory);
				// String currentTagId = uniqueId.substring(0, uniqueId.indexOf("#"));

				newInterfaceMeasurement.setTagid(currentTagId);
				newInterfaceMeasurement.setDescription(description);

				newInterfaceMeasurement.setDataformatid(currentDataFormat.getDataformatid());
				newInterfaceMeasurement.setInterfacename(this.activatedInterfaceName);

				// R6 change
				if ((activatedInterfaceVersion == null) || activatedInterfaceVersion.equals("")) {
					newInterfaceMeasurement.setInterfaceversion("N/A");
				} else {
					newInterfaceMeasurement.setInterfaceversion(activatedInterfaceVersion);
				}

				newInterfaceMeasurement.setTechpackversion("N/A");

				// Check if the TransformerId exists in table Transformer.
				// If it doesn't exist in the Transformer, insert null to the column
				// TRANFORMERID in the InterfaceMeasurement.
				final Transformer whereTransformer = new Transformer(this.dwhrepRockFactory);
				whereTransformer.setTransformerid(currentDataFormat.getDataformatid());
				final TransformerFactory transformerFactory = new TransformerFactory(this.dwhrepRockFactory,
						whereTransformer);
				final Vector<Transformer> targetTransformerVector = transformerFactory.get();

				if ((targetTransformerVector != null) && (targetTransformerVector.size() > 0)) {
					// TransformerId exists in table Transformer.
					newInterfaceMeasurement.setTransformerid(currentDataFormat.getDataformatid());
				} else {
					// No transformerId found. Set null to the InterfaceMeasurement's
					// TransformerId.
					newInterfaceMeasurement.setTransformerid(null);
				}

				newInterfaceMeasurement.setStatus((long) 1);
				newInterfaceMeasurement.setModiftime(currentTimeTimestamp);

				// Save the new InterfaceMeasurement to database table
				newInterfaceMeasurement.insertDB();

			}
			return true;
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Function activateInterface failed.", e);
		}

	}

	/**
	 * This function copies the existing interface's set, adds the OSS Name to it and updates interface set's parser
	 * action's indir.
	 * 
	 * @return TRUE if the copy was ok, FALSE otherwise
	 * @throws org.apache.tools.ant.Exception
	 *             On Errors
	 */
	/*private boolean copyInterfaceSet(final ITransferEngineRMI engineRMI) throws Exception {
		try {
			// Variable containing
			final Meta_collection_sets targetMetaCollectionSet;

			// First get the tech pack (or in this case interface) entry.
			final Meta_collection_sets whereMetaCollectionSets = new Meta_collection_sets(this.etlrepRockFactory);
			whereMetaCollectionSets.setCollection_set_name(this.activatedInterfaceName);
			final Meta_collection_setsFactory metaCollectionSetsFactory = new Meta_collection_setsFactory(
					this.etlrepRockFactory, whereMetaCollectionSets);
			final Vector<Meta_collection_sets> metaCollectionSetVector = metaCollectionSetsFactory.get();

			if (metaCollectionSetVector.size() > 0) {
				targetMetaCollectionSet = metaCollectionSetVector.get(0);
			} else {
				logger.info("No interface set found for " + this.activatedInterfaceName);
				return false;
			}

			// Get all the sets of this interface.
			final Meta_collections whereMetaCollections = new Meta_collections(this.etlrepRockFactory);
			whereMetaCollections.setCollection_set_id(targetMetaCollectionSet.getCollection_set_id());
			final Meta_collectionsFactory metaCollectionsFactory = new Meta_collectionsFactory(this.etlrepRockFactory,
					whereMetaCollections);
			final Vector<Meta_collections> metaCollectionsVector = metaCollectionsFactory.get();

			// HashMap containing metaCollections of this interface set.
			// Key: collectionId of meta_collections table (primary key).
			// Value: RockObject of Meta_collections entry.
			final Map<Long, Meta_collections> metaCollections = new HashMap<Long, Meta_collections>();
			for (final Meta_collections currentMetaCollection : metaCollectionsVector) {
				// Insert the metaCollection to a hashmap for later usage.
				metaCollections.put(currentMetaCollection.getCollection_id(), currentMetaCollection);
			}

			// Iterate the sets and get their actions.
			Set<Long> metaCollectionsKeySet = metaCollections.keySet();
			Iterator<Long> metaCollectionIdsIterator = metaCollectionsKeySet.iterator();

			// HashMap containing metaTransferActions of this interface's
			// metaCollections.
			// Key: transferActionId of meta_transfer_actions table.
			// Value: RockObject of Meta_transfer_actions entry.
			final Map<Long, Meta_transfer_actions> metaTransferActions = new HashMap<Long, Meta_transfer_actions>();

			while (metaCollectionIdsIterator.hasNext()) {
				final Long metaCollectionId = metaCollectionIdsIterator.next();

				final Meta_transfer_actions whereMetaTransferActions = new Meta_transfer_actions(this.etlrepRockFactory);
				whereMetaTransferActions.setCollection_id(metaCollectionId);
				whereMetaTransferActions.setCollection_set_id(targetMetaCollectionSet.getCollection_set_id());
				final Meta_transfer_actionsFactory metaTransferActionsFactory = new Meta_transfer_actionsFactory(
						this.etlrepRockFactory, whereMetaTransferActions);
				final Vector<Meta_transfer_actions> metaTransferActionsVector = metaTransferActionsFactory.get();

				for (final Meta_transfer_actions currentMetaTransferAction : metaTransferActionsVector) {

					// Insert the metaTransferAction to a hashmap for later usage.
					metaTransferActions.put(currentMetaTransferAction.getTransfer_action_id(),
							currentMetaTransferAction);
				}
			}

			// At this point interface, sets and actions are gathered to hashmaps.
			// Start copying it's values as new entries.
			// Save the new interface first.
			final Long newCollectionSetId = getNewCollectionSetId();

			final Meta_collection_sets newMetaCollectionSet = new Meta_collection_sets(this.etlrepRockFactory);
			newMetaCollectionSet.setCollection_set_id(newCollectionSetId);
			// Use the character - to separate interface name from the OSS name.
			// Create the new interface set with extension "-OSSNAME".
			newMetaCollectionSet.setCollection_set_name(targetMetaCollectionSet.getCollection_set_name() + "-"
					+ this.ossName);
			newMetaCollectionSet.setVersion_number(targetMetaCollectionSet.getVersion_number());
			newMetaCollectionSet.setDescription(targetMetaCollectionSet.getDescription());
			newMetaCollectionSet.setEnabled_flag("Y");
			newMetaCollectionSet.setType(targetMetaCollectionSet.getType());
			newMetaCollectionSet.insertDB(false, false);
			// logger.info("CollectionSet " +
			// newMetaCollectionSet.getCollection_set_name() + " inserted
			// succesfully.");

			// Start saving the sets.
			metaCollectionsKeySet = metaCollections.keySet();
			metaCollectionIdsIterator = metaCollectionsKeySet.iterator();
			while (metaCollectionIdsIterator.hasNext()) {
				final Long currentCollectionId = metaCollectionIdsIterator.next();
				final Meta_collections targetMetaCollection = metaCollections.get(currentCollectionId);
				final Long newCollectionId = getNewCollectionId();

				final Meta_collections newMetaCollection = new Meta_collections(this.etlrepRockFactory);
				newMetaCollection.setCollection_id(newCollectionId);
				newMetaCollection.setCollection_name(targetMetaCollection.getCollection_name().replaceAll(
						"\\$\\{OSS\\}", this.ossName));
				newMetaCollection.setCollection(targetMetaCollection.getCollection());
				newMetaCollection.setMail_error_addr(targetMetaCollection.getMail_error_addr());
				newMetaCollection.setMail_fail_addr(targetMetaCollection.getMail_fail_addr());
				newMetaCollection.setMail_bug_addr(targetMetaCollection.getMail_bug_addr());
				newMetaCollection.setMax_errors(targetMetaCollection.getMax_errors());
				newMetaCollection.setMax_fk_errors(targetMetaCollection.getMax_fk_errors());
				newMetaCollection.setMax_col_limit_errors(targetMetaCollection.getMax_col_limit_errors());
				newMetaCollection.setCheck_fk_error_flag(targetMetaCollection.getCheck_fk_error_flag());
				newMetaCollection.setCheck_col_limits_flag(targetMetaCollection.getCheck_col_limits_flag());
				newMetaCollection.setLast_transfer_date(targetMetaCollection.getLast_transfer_date());
				newMetaCollection.setVersion_number(targetMetaCollection.getVersion_number());
				newMetaCollection.setCollection_set_id(newMetaCollectionSet.getCollection_set_id());
				newMetaCollection.setUse_batch_id(targetMetaCollection.getUse_batch_id());
				newMetaCollection.setPriority(targetMetaCollection.getPriority());
				newMetaCollection.setQueue_time_limit(targetMetaCollection.getQueue_time_limit());
				newMetaCollection.setEnabled_flag("Y");
				newMetaCollection.setSettype(targetMetaCollection.getSettype());
				newMetaCollection.setFoldable_flag(targetMetaCollection.getFoldable_flag());
				newMetaCollection.setMeastype(targetMetaCollection.getMeastype());
				newMetaCollection.setHold_flag(targetMetaCollection.getHold_flag());
				newMetaCollection.setScheduling_info(targetMetaCollection.getScheduling_info());

				// Insert the copied set to database.
				newMetaCollection.insertDB(false, false);

				// logger.info("Collection " +
				// newMetaCollection.getCollection_name() + " inserted succesfully.");

				// Next insert the actions of the new set to database.
				final Set<Long> metaTransferActionsKeySet = metaTransferActions.keySet();
				for (final Long metaTransferActionId : metaTransferActionsKeySet) {
					final Meta_transfer_actions targetMetaTransferAction = metaTransferActions
							.get(metaTransferActionId);

					// Check if this action is related to set we are copying.
					if (targetMetaTransferAction.getCollection_id().longValue() == targetMetaCollection
							.getCollection_id().longValue()) {
						final Long newTransferActionId = getNewTransferActionId();

						// Create a copy of this action for the new set.
						final Meta_transfer_actions newMetaTransferAction = new Meta_transfer_actions(
								this.etlrepRockFactory);
						newMetaTransferAction.setVersion_number(targetMetaTransferAction.getVersion_number());
						newMetaTransferAction.setTransfer_action_id(newTransferActionId);
						newMetaTransferAction.setCollection_id(newMetaCollection.getCollection_id());
						newMetaTransferAction.setCollection_set_id(newMetaCollectionSet.getCollection_set_id());
						newMetaTransferAction.setAction_type(targetMetaTransferAction.getAction_type());
						newMetaTransferAction.setTransfer_action_name(targetMetaTransferAction
								.getTransfer_action_name().replaceAll("\\$\\{OSS\\}", this.ossName));
						newMetaTransferAction.setOrder_by_no(targetMetaTransferAction.getOrder_by_no());
						newMetaTransferAction.setDescription(targetMetaTransferAction.getDescription());

						if (targetMetaTransferAction.getWhere_clause() == null) {
							newMetaTransferAction.setWhere_clause(null);
						} else {
							String newWhereClause = targetMetaTransferAction.getWhere_clause();
							newWhereClause = newWhereClause.replaceAll("\\$\\{OSS\\}", this.ossName);
							newMetaTransferAction.setWhere_clause(newWhereClause);
						}
						if (targetMetaTransferAction.getAction_contents() == null) {
							newMetaTransferAction.setAction_contents(null);
						} else {
							String newActionContents = targetMetaTransferAction.getAction_contents();
							newActionContents = newActionContents.replaceAll("\\$\\{OSS\\}", this.ossName);
							if (newActionContents.contains("${EVENTS}") || newActionContents.contains("${EVENTS_OSS}")) {
								// This is an interface for collecting from a mounted EVENTS server. Get OSS Id mapping
								// to get events id and events oss id
								logger.info("Setting variables EVENTS and EVENTS_OSS in Action Contents.");

								OSSIDMappingCache.initialize("/eniq/data/mapping/ossidMapping.txt");
								final OSSIDMappingCache cache = OSSIDMappingCache.getCache();
								if (null == cache) {
									throw new Exception("OSS ID Mapping not available. Interface "
											+ this.activatedInterfaceName + " cannot be activated.");
								}
								final String[] mapping = cache.getMappingForOssid(this.ossName);
								if (null != mapping) {
									logger.info("OSS ID mapping: " + mapping[OSSIDMappingCache.OSS] + "  -->  "
											+ mapping[OSSIDMappingCache.EVENTS] + " + "
											+ mapping[OSSIDMappingCache.EVENTS_OSS]);
									// Set events id and events_oss id for corresponding variables in action_contents
									// (to provide e.g. complete paths to directories).
									newActionContents = newActionContents.replaceAll("\\$\\{EVENTS\\}",
											mapping[OSSIDMappingCache.EVENTS]);
									newActionContents = newActionContents.replaceAll("\\$\\{EVENTS_OSS\\}",
											mapping[OSSIDMappingCache.EVENTS_OSS]);
								} else {
									throw new Exception("Interface " + this.activatedInterfaceName
											+ " cannot be activated for " + this.ossName
											+ " - this OSS ID is not found in mapping.");
								}
							}
							newMetaTransferAction.setAction_contents(newActionContents);
						}

						newMetaTransferAction.setEnabled_flag("Y");
						newMetaTransferAction.setConnection_id(targetMetaTransferAction.getConnection_id());

						// Save the new action to database.
						newMetaTransferAction.insertDB(false, false);

					}
				}

				// directory checker

				this.directoryChecker(engineRMI);

				// Copy also the schedulings from the interface.
				final Meta_schedulings whereMetaSchedulings = new Meta_schedulings(this.etlrepRockFactory);
				whereMetaSchedulings.setCollection_id(currentCollectionId);
				whereMetaSchedulings.setCollection_set_id(targetMetaCollectionSet.getCollection_set_id());
				final Meta_schedulingsFactory metaSchedulingsFactory = new Meta_schedulingsFactory(
						this.etlrepRockFactory, whereMetaSchedulings);
				final Vector<Meta_schedulings> metaSchedulings = metaSchedulingsFactory.get();

				for (final Meta_schedulings currentMetaScheduling : metaSchedulings) {

					final Meta_schedulings newMetaScheduling = new Meta_schedulings(this.etlrepRockFactory);
					newMetaScheduling.setCollection_id(newCollectionId);
					newMetaScheduling.setCollection_set_id(newCollectionSetId);
					newMetaScheduling.setExecution_type(currentMetaScheduling.getExecution_type());
					newMetaScheduling.setFri_flag(currentMetaScheduling.getFri_flag());
					// Don't just copy hold flag. Instead set the new scheduling as
					// active.
					newMetaScheduling.setHold_flag("N");
					newMetaScheduling.setInterval_hour(currentMetaScheduling.getInterval_hour());
					newMetaScheduling.setInterval_min(currentMetaScheduling.getInterval_min());
					// Set the last execution time way back to the year 1970 so that the
					// scheduling is executed as soon as possible.
					newMetaScheduling.setLast_execution_time(new Timestamp(0));
					newMetaScheduling.setMon_flag(currentMetaScheduling.getMon_flag());
					final String name = currentMetaScheduling.getName().replaceAll("\\$\\{OSS\\}", this.ossName);
					newMetaScheduling.setName(name);
					newMetaScheduling.setOs_command(currentMetaScheduling.getOs_command());
					newMetaScheduling.setPriority(currentMetaScheduling.getPriority());
					newMetaScheduling.setSat_flag(currentMetaScheduling.getSat_flag());
					newMetaScheduling.setScheduling_day(currentMetaScheduling.getScheduling_day());
					newMetaScheduling.setScheduling_hour(currentMetaScheduling.getScheduling_hour());
					newMetaScheduling.setScheduling_min(currentMetaScheduling.getScheduling_min());
					newMetaScheduling.setScheduling_month(currentMetaScheduling.getScheduling_month());
					newMetaScheduling.setScheduling_year(currentMetaScheduling.getScheduling_year());
					newMetaScheduling.setSun_flag(currentMetaScheduling.getSun_flag());
					newMetaScheduling.setThu_flag(currentMetaScheduling.getThu_flag());
					newMetaScheduling.setTrigger_command(currentMetaScheduling.getTrigger_command());
					newMetaScheduling.setTue_flag(currentMetaScheduling.getTue_flag());
					newMetaScheduling.setVersion_number(currentMetaScheduling.getVersion_number());
					newMetaScheduling.setWed_flag(currentMetaScheduling.getWed_flag());

					final Long newMetaSchedulingId = getNewMetaSchedulingsId();

					newMetaScheduling.setId(newMetaSchedulingId);

					// Save the new scheduling to database
					newMetaScheduling.insertDB(false, false);
				}
			}

			return true;

		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Function copyInterfaceSet failed.", e);
		}

	}*/

	/**
	 * This function introduced newly for avoid throwing warning while activating interface for oss. Diskmanager should
	 * be triggered only after directoryChecker
	 * 
	 * @throws Exception
	 */
/*	private void directoryChecker(final ITransferEngineRMI engineRMI) throws Exception {
		final String directoryCheckerSetName = "Directory_Checker_" + this.activatedInterfaceName;
		// Start Directory_Checker action if the interface exists.
		if (directoryCheckerSetExists()) {
			startAndWaitSet(engineRMI, activatedInterfaceName + "-" + ossName, directoryCheckerSetName);
		} else {
			logger.info("Directory checker set not found " + directoryCheckerSetName + ". Set not started.");
		}
	}*/

	/**
	 * This function gets the CollectionSetId for the new MetaCollectionSet entry.
	 * 
	 * @return Returns the new CollectionSetId.
	 * @throws Exception
	 *             On Errors
	 */
	@SuppressWarnings({ "PMD.CloseResource" })
	// eeipca : Dont close it yet, the execute may not be finished
	private Long getNewCollectionSetId() throws Exception {
		ResultSet resultSet = null;
		Statement statement = null;
		Connection connection;
		Long newCollectionSetId = (long) 0;
		for (int i = 0; i < maxRetries; i++) {
			try {

				connection = this.etlrepRockFactory.getConnection();
				statement = connection.createStatement();
				final String sqlQuery = "SELECT MAX(collection_set_id) AS collection_set_id FROM meta_collection_sets ;";

				resultSet = statement.executeQuery(sqlQuery);

				if (resultSet.next()) {
					newCollectionSetId = resultSet.getLong("collection_set_id") + 1;
				}

				return newCollectionSetId;
			} catch (final SQLException sybExc) {

				error = sybExc.getErrorCode();

				// 8405 - Sybase Error Code for row locking issue

				if ((error == 8405)|| sybExc.getMessage().indexOf("SQL Anywhere Error -210") > 0|| sybExc.getMessage().indexOf("ASA Error -210") > 0 || sybExc.getMessage().indexOf("User 'another user' has the row") > 0)  {

					final Random rnd = new Random();

					final int secs = Math.abs(rnd.nextInt() % retryRandom_in_seconds) + retryPeriod_in_seconds;
					logger.info("Message - " + sybExc.getMessage());
					logger.info("SybSQLException: View creation failed to locked view. Retrying in " + secs
							+ " seconds");
					try {
						Thread.sleep(secs * 1000);

					} catch (final Exception ie) {
					}
				}

				else {
					logger.info("SEVERE :Error Code-" + sybExc.getErrorCode() + ", Error Message-"
							+ sybExc.getMessage());
					sybExc.printStackTrace();
				}
			} catch (final Exception e) {
				e.printStackTrace();
				throw new Exception("Failed to generate new collection set id.", e);
			} finally {

				try {
					if (resultSet != null) {
						resultSet.close();
					}
				} catch (final Exception e) {
					//
				}

				try {
					if (statement != null) {
						statement.close();
					}
				} catch (final Exception e) {
					//
				}
			}
		}
		return newCollectionSetId;
	}

	/**
	 * This function gets the CollectionId for the new MetaCollections entry.
	 * 
	 * @return Returns the new CollectionId.
	 * @throws Exception
	 *             On Errors
	 */
	@SuppressWarnings({ "PMD.CloseResource" })
	// eeipca : Dont close it yet, the execute may not be finished
	private Long getNewCollectionId() throws Exception {
		Connection connection;
		Statement statement = null;
		ResultSet resultSet = null;
		Long newCollectionId = (long) 0;
		for (int i = 0; i < maxRetries; i++) {
			try {

				connection = this.etlrepRockFactory.getConnection();
				statement = connection.createStatement();
				final String sqlQuery = "SELECT MAX(collection_id) AS collection_id FROM meta_collections ;";

				resultSet = statement.executeQuery(sqlQuery);

				if (resultSet.next()) {
					newCollectionId = resultSet.getLong("collection_id") + 1;
				}

				return newCollectionId;
			} catch (final SQLException sybExc) {

				error = sybExc.getErrorCode();

				// 8405 - Sybase Error Code for row locking issue

				if ((error == 8405)|| sybExc.getMessage().indexOf("SQL Anywhere Error -210") > 0|| sybExc.getMessage().indexOf("ASA Error -210") > 0 || sybExc.getMessage().indexOf("User 'another user' has the row") > 0) {

					final Random rnd = new Random();

					final int secs = Math.abs(rnd.nextInt() % retryRandom_in_seconds) + retryPeriod_in_seconds;
					logger.info("Message - " + sybExc.getMessage());
					logger.info("SybSQLException: View creation failed to locked view. Retrying in " + secs
							+ " seconds");
					try {
						Thread.sleep(secs * 1000);
					} catch (final Exception ie) {
					}
				}

				else {
					logger.info("SEVERE :Error Code-" + sybExc.getErrorCode() + ", Error Message-"
							+ sybExc.getMessage());
					sybExc.printStackTrace();
				}
			} catch (final Exception e) {
				e.printStackTrace();
				throw new Exception("Failed to generate new collection id.", e);
			} finally {

				try {
					if (resultSet != null) {
						resultSet.close();
					}
				} catch (final Exception e) {
					//
				}

				try {
					if (statement != null) {
						statement.close();
					}
				} catch (final Exception e) {
					//
				}
			}
		}
		return newCollectionId;
	}

	/**
	 * This function gets the TransferActionId for the new MetaTransferActions entry.
	 * 
	 * @return Returns the new TransferActionId.
	 * @throws Exception
	 *             On Errors
	 */
	@SuppressWarnings({ "PMD.CloseResource" })
	// eeipca : Dont close it yet, the execute may not be finished
	private Long getNewTransferActionId() throws Exception {
		Connection connection;
		Statement statement = null;
		ResultSet resultSet = null;
		Long newTransferActionId = (long) 0;
		for (int i = 0; i < maxRetries; i++) {
			try {

				connection = this.etlrepRockFactory.getConnection();
				statement = connection.createStatement();
				final String sqlQuery = "SELECT MAX(transfer_action_id) AS transfer_action_id FROM meta_transfer_actions ;";

				resultSet = statement.executeQuery(sqlQuery);

				if (resultSet.next()) {
					newTransferActionId = resultSet.getLong("transfer_action_id") + 1;
				}

				return newTransferActionId;
			} catch (final SQLException sybExc) {

				error = sybExc.getErrorCode();

				// 8405 - Sybase Error Code for row locking issue

				if ((error == 8405)|| sybExc.getMessage().indexOf("SQL Anywhere Error -210") > 0|| sybExc.getMessage().indexOf("ASA Error -210") > 0 || sybExc.getMessage().indexOf("User 'another user' has the row") > 0) {

					final Random rnd = new Random();

					final int secs = Math.abs(rnd.nextInt() % retryRandom_in_seconds) + retryPeriod_in_seconds;
					logger.info("Message - " + sybExc.getMessage());
					logger.info("SybSQLException: View creation failed to locked view. Retrying in " + secs
							+ " seconds");
					try {
						Thread.sleep(secs * 1000);
					} catch (final Exception ie) {
					}
				}

				else {
					logger.info("SEVERE :Error Code-" + sybExc.getErrorCode() + ", Error Message-"
							+ sybExc.getMessage());
					sybExc.printStackTrace();
				}
			} catch (final Exception e) {
				e.printStackTrace();
				throw new Exception("Failed to generate new transfer action id.", e);

			} finally {

				try {
					if (resultSet != null) {
						resultSet.close();
					}
				} catch (final Exception e) {
					//
				}

				try {
					if (statement != null) {
						statement.close();
					}
				} catch (final Exception e) {
					//
				}
			}
		}
		return newTransferActionId;
	}

	/**
	 * This function returns true if the directory checker set exists for the interface to be activated.
	 * 
	 * @return Returns true if the directory checker exists, otherwise returns false.
	 * @throws Exception
	 *             On Errors
	 */
	public boolean directoryCheckerSetExists() throws Exception {
		try {

			// Get the interface's metaCollectionSetId.
			final Meta_collection_sets whereMetaCollectionSets = new Meta_collection_sets(this.etlrepRockFactory);
			whereMetaCollectionSets.setCollection_set_name(this.activatedInterfaceName + "-" + this.ossName);
			final Meta_collection_setsFactory metaCollectionSetsFactory = new Meta_collection_setsFactory(
					this.etlrepRockFactory, whereMetaCollectionSets);
			final Vector<Meta_collection_sets> metaCollectionSetsVector = metaCollectionSetsFactory.get();
			Long metaCollectionSetId;

			if (metaCollectionSetsVector.size() > 0) {
				final Meta_collection_sets targetMetaCollectionSet = metaCollectionSetsVector.get(0);
				metaCollectionSetId = targetMetaCollectionSet.getCollection_set_id();
			} else {
				logger.info("No set found for " + this.activatedInterfaceName
						+ ". Cannot start Directory_Checker set.");
				return false;
			}

			final Meta_collections targetMetaCollection = new Meta_collections(this.etlrepRockFactory);
			targetMetaCollection.setCollection_name("Directory_Checker_" + this.activatedInterfaceName);
			targetMetaCollection.setCollection_set_id(metaCollectionSetId);
			final Meta_collectionsFactory metaCollectionsFactory = new Meta_collectionsFactory(this.etlrepRockFactory,
					targetMetaCollection);
			final Vector<Meta_collections> targetMetaCollectionsVector = metaCollectionsFactory.get();

			if (targetMetaCollectionsVector.size() > 0) {
				// Directory checker set exists.
				logger.info("Directory checker set found for " + this.activatedInterfaceName + "-"
						+ this.ossName);
				return true;
			} else {
				// Directory checker not found.
				logger.info("Directory checker set not found for " + this.activatedInterfaceName + "-"
						+ this.ossName);
				return false;
			}

		} catch (final Exception e) {
			throw new Exception("Checking of directory checker set failed.", e);
		}
	}

	public String getOnlyActivateInterface() {
		return onlyActivateInterface;
	}

	public void setOnlyActivateInterface(final String onlyActivateInterface) {
		this.onlyActivateInterface = onlyActivateInterface;
	}

	/**
	 * This function gets the Id for the new MetaSchedulings entry.
	 * 
	 * @return Returns the new CollectionId.
	 * @throws Exception
	 *             On Errors
	 */
	@SuppressWarnings({ "PMD.CloseResource" })
	// eeipca : Dont close it yet, the execute may not be finished
	private Long getNewMetaSchedulingsId() throws Exception {
		Connection connection;
		Statement statement = null;
		ResultSet resultSet = null;
		Long newMetaSchedulingsId = (long) 0;
		for (int i = 0; i < maxRetries; i++) {
			try {

				connection = this.etlrepRockFactory.getConnection();
				statement = connection.createStatement();
				final String sqlQuery = "SELECT MAX(id) AS id FROM meta_schedulings ;";

				resultSet = statement.executeQuery(sqlQuery);

				if (resultSet.next()) {
					newMetaSchedulingsId = resultSet.getLong("id") + 1;
				}

				return newMetaSchedulingsId;
			} catch (final SQLException sybExc) {
				error = sybExc.getErrorCode();
				// 8405 - Sybase Error Code for row locking issue
				if ((error == 8405)|| sybExc.getMessage().indexOf("SQL Anywhere Error -210") > 0|| sybExc.getMessage().indexOf("ASA Error -210") > 0 || sybExc.getMessage().indexOf("User 'another user' has the row") > 0) {
					final Random rnd = new Random();
					final int secs = Math.abs(rnd.nextInt() % retryRandom_in_seconds) + retryPeriod_in_seconds;
					logger.info("Message - " + sybExc.getMessage());
					logger.info("SybSQLException: View creation failed to locked view. Retrying in " + secs
							+ " seconds");
					try {
						Thread.sleep(secs * 1000);
					} catch (final Exception ie) {
					}
				} else {
					logger.info("SEVERE :Error Code-" + sybExc.getErrorCode() + ", Error Message-"
							+ sybExc.getMessage());
					sybExc.printStackTrace();
				}
			} catch (final Exception e) {
				e.printStackTrace();
				throw new Exception("Failed to generate new MetaScheduling id.", e);
			} finally {
				try {

					try {
						if (resultSet != null) {
							resultSet.close();
						}
					} catch (final Exception e) {
						//
					}

					if (statement != null) {
						statement.close();
					}
				} catch (final Exception e) {
					//
				}
			}
		}
		return newMetaSchedulingsId;
	}

	/**
	 * This function checks out if the interface is already activated. The checking is done from activated interface
	 * META_COLLECTION_SET_NAME's.
	 * 
	 * @return Returns true if the interface is already activated. Otherwise returns false.
	 * @throws org.apache.tools.ant.Exception
	 *             On Errors
	 */
	private boolean interfaceAlreadyActivated() throws Exception {

		try {
			final Meta_collection_sets whereMetaCollSet = new Meta_collection_sets(this.etlrepRockFactory);
			whereMetaCollSet.setCollection_set_name(this.activatedInterfaceName + "-" + this.ossName);

			final Meta_collection_setsFactory metaCollSetsFactory = new Meta_collection_setsFactory(
					this.etlrepRockFactory, whereMetaCollSet);

			final Vector<Meta_collection_sets> metaCollSets = metaCollSetsFactory.get();

			return metaCollSets.size() > 0;

		} catch (final Exception e) {
			throw new Exception("Failed to check if interface is already activated.", e);
		}

	}

	/**
	 * This function removes the sets of this interface and all activated OSS's interface sets.
	 * @throws Exception 
	 */
	private void removeIntfSets() throws Exception {
		for (int i = 0; i < maxRetries; i++) {
			try {
				final Meta_collection_sets whereCollSets = new Meta_collection_sets(this.etlrepRockFactory);
				final Meta_collection_setsFactory collSetsFactory = new Meta_collection_setsFactory(
						this.etlrepRockFactory, whereCollSets);
				final Vector<Meta_collection_sets> collSets = collSetsFactory.get();

				for (final Meta_collection_sets currentCollSet : collSets) {

					// Remove the previously activated interface's sets.
					// Activated interface's sets are in format INTF_DC_E_XYZ-OSS_NAME.
					if (currentCollSet.getCollection_set_name().equalsIgnoreCase(
							this.activatedInterfaceName + "-" + this.ossName)) {
						logger.info("Deleting interface set " + currentCollSet.getCollection_set_name()
								+ " and it's contents.");

						// This set is the currently installed interface's or some activated
						// OSS's sets.
						// Delete the whole set including everything related to it.
						final Meta_collections whereColls = new Meta_collections(this.etlrepRockFactory);
						whereColls.setCollection_set_id(currentCollSet.getCollection_set_id());

						final Meta_collectionsFactory collFactory = new Meta_collectionsFactory(this.etlrepRockFactory,
								whereColls);
						final Vector<Meta_collections> collections = collFactory.get();

						for (final Meta_collections currentCollection : collections) {

							final Meta_transfer_actions whereTrActions = new Meta_transfer_actions(
									this.etlrepRockFactory);
							whereTrActions.setCollection_id(currentCollection.getCollection_id());
							whereTrActions.setCollection_set_id(currentCollSet.getCollection_set_id());

							final Meta_transfer_actionsFactory trActionsFactory = new Meta_transfer_actionsFactory(
									this.etlrepRockFactory, whereTrActions);
							final Vector<Meta_transfer_actions> trActions = trActionsFactory.get();

							for (final Meta_transfer_actions currTrAction : trActions) {
								currTrAction.deleteDB();
							}

							// Remove also the schedulings from the interface.
							final Meta_schedulings whereMetaSchedulings = new Meta_schedulings(this.etlrepRockFactory);
							whereMetaSchedulings.setCollection_id(currentCollection.getCollection_id());
							whereMetaSchedulings.setCollection_set_id(currentCollSet.getCollection_set_id());
							final Meta_schedulingsFactory metaSchedulingsFactory = new Meta_schedulingsFactory(
									this.etlrepRockFactory, whereMetaSchedulings);
							final Vector<Meta_schedulings> metaSchedulings = metaSchedulingsFactory.get();

							for (final Meta_schedulings currentMetaScheduling : metaSchedulings) {
								currentMetaScheduling.deleteDB();
							}

							// Do not delete the meta_transfer_batches. Old
							// meta_transfer_batches are cleaned up by housekeeping set.
							currentCollection.deleteDB();
						}

						currentCollSet.deleteDB();
					}
				}
			} catch (final SQLException sybExc) {

				logger.info("SybSQLException: View creation failed to locked view. Retry " + retry + " times.");

				error = sybExc.getErrorCode();

				// 8405 - Sybase Error Code for row locking issue

				if ((error == 8405)|| sybExc.getMessage().indexOf("SQL Anywhere Error -210") > 0|| sybExc.getMessage().indexOf("ASA Error -210") > 0 || sybExc.getMessage().indexOf("User 'another user' has the row") > 0) {

					final Random rnd = new Random();

					final int secs = Math.abs(rnd.nextInt() % retryRandom_in_seconds) + retryPeriod_in_seconds;
					logger.info("Message - " + sybExc.getMessage());
					logger.info("SybSQLException: View creation failed to locked view. Retrying in " + secs
							+ " seconds");
					try {
						Thread.sleep(secs * 1000);
					} catch (final Exception ie) {
					}
				}

				else {
					logger.info("SEVERE :Error Code-" + sybExc.getErrorCode() + ", Error Message-"
							+ sybExc.getMessage());
					sybExc.printStackTrace();
				}
			} catch (final Exception e) {
				e.printStackTrace();
				throw new Exception("Failed removing previous installations interface sets.", e);
			}

		}
	}

}

