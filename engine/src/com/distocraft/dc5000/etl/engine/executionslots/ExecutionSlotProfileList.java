/*
 * Created on 26.1.2005
 *
 */
package com.distocraft.dc5000.etl.engine.executionslots;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_execution_slot;
import com.distocraft.dc5000.etl.rock.Meta_execution_slotFactory;
import com.distocraft.dc5000.etl.rock.Meta_execution_slot_profile;
import com.distocraft.dc5000.etl.rock.Meta_execution_slot_profileFactory;

/**
 * @author savinen
 * 
 */
public class ExecutionSlotProfileList {

  /* contains ExecutionSlotProfile Objects */
  private List<ExecutionSlotProfile> exeSlotsProfileList;

  // private RockFactory rockFact;
  private boolean locked = false;

  private String url;

  private String userName;

  private String password;

  private String dbDriverName;

  private final Logger log;

  /**
   * constructor
   * 
   */
  public ExecutionSlotProfileList(final String url, final String userName, final String password, final String dbDriverName) {

    this.url = url;
    this.userName = userName;
    this.password = password;
    this.dbDriverName = dbDriverName;

    this.log = Logger.getLogger("etlengine.ExecutionSlotProfile");

    try {

      exeSlotsProfileList = new Vector<ExecutionSlotProfile>();
      log.fine("init: creating rockEngine");

      this.readProfiles();

      log.finest("init: Execution slot profiles created");

    } catch (Exception e) {
      log.log(Level.WARNING, "Error while initializing ExecutionSlotProfileList", e);
    }

  }

  /**
   * constructor
   * 
   * creates number of default slots..
   * 
   */
  public ExecutionSlotProfileList(final int nroOfSlots) {
    this.log = Logger.getLogger("etlengine.ExecutionSlotProfile");

    exeSlotsProfileList = new Vector<ExecutionSlotProfile>();
    this.createProfile(nroOfSlots);
    log.finest("Default Execution slot profile created TEST");

  }

  /**
   * 
   * 
   */
  public void resetProfiles() {

    if (this.locked) {
      log.finest("profiles locked");
    } else {
      exeSlotsProfileList = new Vector<ExecutionSlotProfile>();
      this.readProfiles();
      log.finest("Execution slot profiles reseted");
    }

  }

  /**
   * 
   * creates default execution profile that contains number of execution slots.
   * All of created slots accept all possible set types
   * 
   * 
   * @param nro
   * @throws Exception
   */
  private void createProfile(final int nro) {

    if (this.locked) {
      
      log.finest("profiles locked");

    } else {
      
      final ExecutionSlotProfile esp = new ExecutionSlotProfile("DefaultProfile", "0");
      esp.activate();

      for (int i = 0; i < nro; i++) {

        final ExecutionSlot ex = new ExecutionSlot(i,"Default" + i, "", null);

        log.config("Default Execution slot (Default" + i + ") created ");

        esp.addExecutionSlot(ex);

      }

      exeSlotsProfileList.add(esp);

    }

  }

  /**
   * 
   * reads execution profiles from DB
   * 
   * @throws Exception
   */
  private void readProfiles() {

    RockFactory rockFact = null;
    try {

      rockFact = new RockFactory(url, userName, password, dbDriverName, "ETLExProfile", true);

      if (this.locked) {
        
        log.finest("profiles locked");


      } else {
        
        final Meta_execution_slot_profile whereProfile = new Meta_execution_slot_profile(rockFact);
        final Meta_execution_slot_profileFactory espF = new Meta_execution_slot_profileFactory(rockFact, whereProfile);

        for (int ii = 0; ii < espF.size(); ii++) {

          final Meta_execution_slot_profile profile = espF.getElementAt(ii);

          final Meta_execution_slot whereSlot = new Meta_execution_slot(rockFact);
          whereSlot.setProfile_id(profile.getProfile_id());
          final Meta_execution_slotFactory slotF = new Meta_execution_slotFactory(rockFact, whereSlot);

          final ExecutionSlotProfile esp = new ExecutionSlotProfile(profile.getProfile_name(), profile.getProfile_id());

          if (profile.getActive_flag().equalsIgnoreCase("y")){
            esp.activate();
          }
          
          for (int i = 0; i < slotF.size(); i++) {

            final Meta_execution_slot slot = slotF.getElementAt(i);
            final int id = Integer.parseInt(slot.getSlot_id());
            final ExecutionSlot ex = new ExecutionSlot(id,slot.getSlot_name(), slot.getAccepted_set_types(), slot.getService_node());

            log.config("Execution slot (" + profile.getProfile_name() + "/" + slot.getSlot_name() + ") read");

            esp.addExecutionSlot(ex);

          }

          exeSlotsProfileList.add(esp);

        }

      }

    } catch (Exception e) {

      log.log(Level.FINEST, "Error while creating rockEngine", e);

    } finally {

      try {

        if (rockFact != null && rockFact.getConnection() != null){
          rockFact.getConnection().close();
        }

        rockFact = null;

      } catch (Exception e) {

        log.log(Level.WARNING, "Error while closing rockEngine", e);

      }

    }

  }

  /**
   * writes profile to DB.
   * 
   */
  public void writeProfile() {

    RockFactory rockFact = null;

    try {

      rockFact = new RockFactory(url, userName, password, dbDriverName, "ETLExProfile", true);

      if (exeSlotsProfileList != null && !exeSlotsProfileList.isEmpty()) {

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

          profile.updateDB();

        }

        log.config("Execution slot profile (" + whereProfile.getProfile_name() + ") saved to DB");

      }

    } catch (Exception e) {

      log.log(Level.WARNING, "Error while creating rockEngine", e);

    } finally {

      try {

        if (rockFact != null && rockFact.getConnection() != null){
          rockFact.getConnection().close();
        }
        
        rockFact = null;

      } catch (Exception e) {

        log.log(Level.WARNING, "Error while closing rockEngine", e);

      }
    }

  }

  /**
   * Set a profile to active
   * 
   * @return true if given profile is activated else false
   */
  public boolean setActiveProfile(final String profileName) {

    ExecutionSlotProfile aex = null;
    final List<ExecutionSlotProfile> deActivationList = new ArrayList<ExecutionSlotProfile>();

    if (this.locked) {

      log.finest("profiles locked");
      
    } else {
      
      for (int i = 0; i < exeSlotsProfileList.size(); i++) {
        final ExecutionSlotProfile ex = exeSlotsProfileList.get(i);

        if (ex.name().equals(profileName)) {
          log.finest("Execution slot profile(" + ex.name() + ") set to active");

          // activate this profile
          aex = ex;

        } else {

          // deactivate thease profiles
          deActivationList.add(ex);
        }
      }

      // if activated profile is not found, nothing is done.
      if (aex != null) {

        // activate
        aex.activate();

        // deactivate
        for (ExecutionSlotProfile ex : deActivationList) {
          ex.deactivate();
        }

        return true;
      }


    }

    return false;

  }

  /**
   * retrieves active execution profile
   * 
   * @return
   */
  public ExecutionSlotProfile getActiveExecutionProfile() {

    for (int i = 0; i < exeSlotsProfileList.size(); i++) {
      final ExecutionSlotProfile ex = exeSlotsProfileList.get(i);
      if (ex.IsActivate()) {
        return ex;
      }
    }

    log.finest("No active execution profile set.");
    return null;
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

}
