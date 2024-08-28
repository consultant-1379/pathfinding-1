package com.distocraft.dc5000.etl.engine.executionslots;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_execution_slot;
import com.distocraft.dc5000.etl.rock.Meta_execution_slotFactory;
import com.distocraft.dc5000.etl.rock.Meta_execution_slot_profile;
import com.distocraft.dc5000.etl.rock.Meta_execution_slot_profileFactory;

/**
 * @author savinen
 */
public class ExecutionSlotProfileHandler {

	private static final Logger LOG = Logger.getLogger("etlengine.SlotProfileHandler");

	private Map<String, ExecutionSlotProfile> allExecSlotProfiles;
	private ExecutionSlotProfile activeSlotProfile;

	private int numberOfAdapterSlots = 0;
	private int numberOfAggregatorSlots = 0;
	
	private boolean locked = false;

	private String url;
	private String userName;
	private String password;
	private String dbDriverName;

	/**
	 * constructor
	 * 
	 */
	public ExecutionSlotProfileHandler(final String url, final String userName, final String password,
			final String dbDriverName) {

		this.url = url;
		this.userName = userName;
		this.password = password;
		this.dbDriverName = dbDriverName;

		try {

			allExecSlotProfiles = new HashMap<String, ExecutionSlotProfile>();
			LOG.finer("Init: creating rockEngine");
		
			activeSlotProfile = createActiveProfile(1);
			this.readProfiles();

			LOG.finest("Init: execution slot profiles created");

		} catch (Exception e) {
			LOG.log(Level.WARNING, "Error while initializing ExecutionSlotProfileList", e);
		}

	}

	/**
	 * constructor
	 * 
	 * creates number of default slots..
	 * 
	 */
	public ExecutionSlotProfileHandler(final int nroOfSlots) {

		allExecSlotProfiles = new HashMap<String, ExecutionSlotProfile>();
		activeSlotProfile = createActiveProfile(nroOfSlots);

		LOG.warning("Default Execution slot profile created");

	}

	/**
   * 
   * 
   */
	public void resetProfiles() {
		if (this.locked) {
			LOG.finest("profiles locked");
		} else {
			allExecSlotProfiles = new HashMap<String, ExecutionSlotProfile>();
			this.readProfiles();
			LOG.finest("Execution slot profiles reseted");
		}
	}

	/**
	 * creates default execution profile that contains number of execution slots.
	 * All of created slots accept all possible set types
	 * 
	 * 
	 * @param nro Number of slots to create
	 */
	public final ExecutionSlotProfile createActiveProfile(final int nro) {
		if (this.locked) {
			LOG.finest("Profile is locked");
		} else {
			final ExecutionSlotProfile esp = new ExecutionSlotProfile("DefaultProfile", "0");
			LOG.info("Profile used: " + esp.name());

			for (int i = 0; i < nro; i++) {
				// this is so that nothing will get executed if the default profile is used
				final ExecutionSlot ex = new ExecutionSlot(i, "Default" + i, "Nothing", null);
			
				LOG.info("Default Execution slot (Default " + i + ") created ");
				LOG.info("Execution slot: " + ex.getName() + " is being used");
				LOG.info("Executing Slot: Running Set: " + ex.getRunningSet());

				esp.addExecutionSlot(ex);
			}

			esp.activate();
			return esp;
		}

		return null;
	}

	/**
	 * Reads execution profiles from DB
	 */
	private void readProfiles() {

		RockFactory rockFact = null;
		try {

			rockFact = new RockFactory(url, userName, password, dbDriverName, "ETLExProfile", true);

			if (this.locked) {

				LOG.finest("Profile is locked");

			} else {
				
				resetNumberOfSlots();

				final Meta_execution_slot_profile whereProfile = new Meta_execution_slot_profile(rockFact);
				final Meta_execution_slot_profileFactory espF = new Meta_execution_slot_profileFactory(rockFact, whereProfile);

				for (int ii = 0; ii < espF.size(); ii++) {

					final Meta_execution_slot_profile profile = espF.getElementAt(ii);					
					
					final ExecutionSlotProfile esp = readSlots(profile,rockFact);
					
					allExecSlotProfiles.put(esp.name(), esp);

					if (esp.IsActivate()) {
						setActiveProfile(esp.name(), null);
					}

				}

			}

		} catch (Exception e) {

			LOG.log(Level.WARNING, "Error while creating rockEngine", e);

		} finally {

			try {

				if (rockFact != null && rockFact.getConnection() != null) {
					rockFact.getConnection().close();
				}

			} catch (Exception e) {

				LOG.log(Level.WARNING, "Error while closing rockEngine", e);

			}

		}

	}
	
	private ExecutionSlotProfile readSlots(final Meta_execution_slot_profile profile, final RockFactory rockFact) throws RockException,SQLException {

		LOG.fine("Inside readSlots-->");
		final Meta_execution_slot whereSlot = new Meta_execution_slot(rockFact);
		whereSlot.setProfile_id(profile.getProfile_id());
		final Meta_execution_slotFactory slotF = new Meta_execution_slotFactory(rockFact, whereSlot);

		final ExecutionSlotProfile esp = new ExecutionSlotProfile(profile.getProfile_name(), profile.getProfile_id());

		if (profile.getActive_flag().equalsIgnoreCase("y")) {
			esp.activate();
		}

		for (Meta_execution_slot slot : slotF.get()) {

			final int id = Integer.parseInt(slot.getSlot_id());
			final ExecutionSlot ex = new ExecutionSlot(id, slot.getSlot_name(), slot.getAccepted_set_types(), slot.getService_node());
			updateNumberOfAdapterSlots(slot.getAccepted_set_types());
			
			LOG.config("Execution slot (" + profile.getProfile_name() + "/" + slot.getSlot_name() + ") read \""
					+ slot.getAccepted_set_types() + "\"");

			esp.addExecutionSlot(ex);

		}

		return esp;
		
	}

	/**
   * Reset the number of Adapter and Aggregator Slots.
   */
  private void resetNumberOfSlots() {
	  numberOfAdapterSlots = 0;
	  numberOfAggregatorSlots = 0;
  }

  /**
   * Update the number of Adapter|Aggregator Slots. This method passes in the 
   * setTypes from the Execution Slot. If the set type contains an 
   * adapter, then the count is updated.
   */
  private void updateNumberOfAdapterSlots(final String acceptedSetTypes) {
	  LOG.finest("Inside updateNumberOfAdapterSlots");
	  LOG.finest("Printing acceptedSetTypes -->" +acceptedSetTypes);
	  if (acceptedSetTypes.contains("adapter") || acceptedSetTypes.contains("Adapter")){
		  this.numberOfAdapterSlots = numberOfAdapterSlots + 1;
	  } if (acceptedSetTypes.contains("aggregator") || acceptedSetTypes.contains("Aggregator")){
		  this.numberOfAggregatorSlots = numberOfAggregatorSlots + 1;
	  }
	  LOG.finest("aggregator slots-->"+this.numberOfAggregatorSlots);
	  LOG.finest("adapter slots-->"+this.numberOfAdapterSlots);
  }

  /**
   * Return the number of Adapter Slots.
   */
  public int getNumberOfAdapterSlots() {
	  return this.numberOfAdapterSlots;
  }

  /**
   * Return the number of Aggregator Slots.
   */
  public int getNumberOfAggregatorSlots() {
	  return this.numberOfAggregatorSlots;
  }
	
	/**
	 * Writes profiles to DB
	 */
	public void writeProfile() {

		RockFactory rockFact = null;

		try {

			rockFact = new RockFactory(url, userName, password, dbDriverName, "ETLExProfile", true);

			if (allExecSlotProfiles != null && !allExecSlotProfiles.isEmpty()) {

				final ExecutionSlotProfile aesp = getActiveExecutionProfile();

				final Meta_execution_slot_profile whereProfile = new Meta_execution_slot_profile(rockFact);
				final Meta_execution_slot_profileFactory espF = new Meta_execution_slot_profileFactory(rockFact, whereProfile);

				for (int ii = 0; ii < espF.size(); ii++) {

					final Meta_execution_slot_profile profile = espF.getElementAt(ii);

					if (profile.getProfile_id().equalsIgnoreCase(aesp.ID())) {
						profile.setActive_flag("Y");
					} else {
						profile.setActive_flag("N");
					}

					LOG.config("Execution slot profile (" + profile.getProfile_name() + ") saved to DB with active flag: "
							+ profile.getActive_flag());
					profile.updateDB();

				}
			}

		} catch (Exception e) {

			LOG.log(Level.WARNING, "Error while creating rockEngine", e);

		} finally {

			try {

				if (rockFact != null && rockFact.getConnection() != null) {
					rockFact.getConnection().close();
				}

			} catch (Exception e) {

				LOG.log(Level.WARNING, "Error while closing rockEngine", e);

			}
		}

	}

	public final boolean setActiveProfile(final String profileName, final String messageText) {
		if (messageText != null) {
			LOG.info("Starting to change profile to " + profileName + ". Reason why: " + messageText);
		}

		if (this.locked) {
			LOG.info("Execution Profile is locked");
		} else {

			// TODO Please somebody. Rework this. This is horrible.

			if (allExecSlotProfiles.containsKey(profileName)) {

				final ExecutionSlotProfile newProfile = allExecSlotProfiles.get(profileName);
				newProfile.activate();

				activeSlotProfile.setID(newProfile.ID());
				
				activeSlotProfile.setName(newProfile.name());
				
				final Iterator<ExecutionSlot> newIter = newProfile.getAllExecutionSlots();

				final Iterator<ExecutionSlot> currentIter = activeSlotProfile.getAllExecutionSlots();

				final List<ExecutionSlot> newSlotList = new ArrayList<ExecutionSlot>();

				ExecutionSlot oldExs = null;
				ExecutionSlot newExs;

				// update existing slots to new.
				while (newIter.hasNext() || oldExs != null) {

					// is there rejected slot waiting
					if (oldExs == null) {
						// no slot waiting, get a new one.
						newExs = newIter.next();
					} else {
						// there is a old one, take it.
						newExs = oldExs;
						oldExs = null;
					}

					if (currentIter.hasNext()) {

						// update sets to
						final ExecutionSlot exs = currentIter.next();

						// update only free slots
						if (exs.isFree()) {
							// if set was marked for removal, cancel that
							exs.removeAfterExecution(false);
							exs.setApprovedSettypes(newExs.getApprovedSettypes());
							exs.setName(newExs.getName());
							exs.setSlotId(newExs.getSlotId());
							exs.setSlotType(newExs.getSlotType());
						} else {

							// slot is not free, mark it as to be removed
							exs.setApprovedSettypes("");
							exs.setName("TO_BE_REMOVED");
							exs.removeAfterExecution(true);

							// and try a new slot.
							oldExs = newExs;
						}

					} else {

						// just add new slots to active profile
						final int id = newExs.getSlotId();
						newSlotList.add(new ExecutionSlot(id, newExs.getName(), newExs.getApprovedSettypes(), newExs.getSlotType()));
					}
				}

				// remove extra free sets from active profile, running set stay onboard
				while (currentIter.hasNext()) {
					final ExecutionSlot exs = currentIter.next();
					if (exs.isFree()) {
						activeSlotProfile.removeExecutionSlot(exs);
					} else {
						exs.setApprovedSettypes("");
						exs.setName("TO_BE_REMOVED");
						exs.removeAfterExecution(true);
					}
				}

				// add new sets to active profile
				for (ExecutionSlot exs : newSlotList) {
				  activeSlotProfile.addExecutionSlot(exs);
				}

				return true;
			} else {
				LOG.warning("Profile name not found " + profileName);
			}

		}

		return false;
	}

	/**
	 * retrieves active execution profile
	 * 
	 * @return The active execution profile
	 */
	public ExecutionSlotProfile getActiveExecutionProfile() {
		return this.activeSlotProfile;
	}

	 /**
	  * Retrieves all the execution profiles
	 * @return
	 */
	public Map<String, ExecutionSlotProfile> getAllExecutionProfiles() {
	    return this.allExecSlotProfiles;
	  }
	 
	public void lockProfile() {
		this.locked = true;
	}

	public void unLockProfile() {
		this.locked = false;
	}

	public boolean isProfileLocked() {
		return this.locked;
	}

	/**
	 * 
	 * 
	 * 
	 * @return true if active profile is clean, does not contain any ready for
	 *         removal slots.
	 */
	public boolean isProfileClean() {
		return activeSlotProfile.isProfileClean();
	}

	public void cleanActiveProfile() {

		activeSlotProfile.cleanProfile();

	}

}
