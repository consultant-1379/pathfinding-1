package com.distocraft.dc5000.etl.engine.system;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.CommonUtils;
import com.ericsson.eniq.common.Utils;
import com.ericsson.eniq.common.lwp.LwProcess;
import com.ericsson.eniq.common.lwp.LwpException;
import com.ericsson.eniq.common.lwp.LwpOutput;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import ssc.rockfactory.RockFactory;

public class DirSetPermissions extends TransferActionBase {
  /**
   * Logger
   */
  private final Logger log;

  /**
   * Set Name, used to generate the script name
   */
  private final String name;
  public DirSetPermissions(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
                           final Long transferActionId, final Long transferBatchId, final Long connectionId,
                           final RockFactory etlrep,
                           final Meta_transfer_actions trActions) throws EngineMetaDataException {
    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectionId, etlrep, trActions);
    name = collection.getCollection_name();
    log = Logger.getLogger("etlengine.DirSetPermissions." + name);
  }

  /**
   * Get all the CreateDirActions the collection_id and collection_set_id match and generate one script that
   * creates, sets permissions etc. for all directories that are part of the Directory_Checker_<TtechPack> set
   * 
   * @throws Exception
   */
  @Override
  public void execute() throws Exception {
    final Meta_transfer_actions where = new Meta_transfer_actions(getRockFact());
    where.setCollection_set_id(getCollectionSetId());
    where.setCollection_id(getCollectionId());
    final Meta_transfer_actionsFactory factory = new Meta_transfer_actionsFactory(
      getRockFact(), where, "ORDER BY ORDER_BY_NO");
    final List<Meta_transfer_actions> dirs = factory.get();
    final StringBuilder sb = new StringBuilder();
    final int etlExpandCount = CommonUtils.getNumOfDirectories(log);
    log.info("ETL Data spread set to " + etlExpandCount);
    int totalFileCount = 0;
    int totalCmdCount = 0;

    if(dirs.isEmpty()){
      log.info("No Transfer Actions found matching CollectinSetID:" + getCollectionSetId() + " and " +
        "CollectionID:" + getCollectionId());
      return;
    }
    
    for (Meta_transfer_actions action : dirs) {
      final String _dir = action.getWhere_clause();
      final List<File> allDirs = CommonUtils.expandEtlPathWithMountPoints(_dir, etlExpandCount, false);
      totalFileCount += allDirs.size();
      final Properties props = Utils.stringToProperty(action.getAction_contents());
      final String owner = props.getProperty("owner", null);
      final String group = props.getProperty("group", null);
      final String permission = props.getProperty("permission", null);

      for (File f : allDirs) {
        sb.append("mkdir -p ").append(f.getPath());
        sb.append('\n');
        if (permission != null && permission.trim().length() != 0) {
          sb.append("chmod ").append(permission).append(" ").append(f.getPath());
          sb.append('\n');
          ++totalCmdCount;
        }
        if (owner != null && owner.trim().length() != 0) {
          sb.append("chown ").append(owner).append(" ").append(f.getPath());
          sb.append('\n');
          ++totalCmdCount;
        }
        if (group != null && group.trim().length() != 0) {
          sb.append("chgrp ").append(group).append(" ").append(f.getPath());
          sb.append('\n');
          ++totalCmdCount;
        }
      }
    }
    final String scriptContents = sb.toString().trim();
    final long start = System.currentTimeMillis();
    runProcess(scriptContents);
    final long total = System.currentTimeMillis() - start;
    log.info("Script checked " + totalFileCount + " directories ("+totalCmdCount+") in " + total + " msec");
  }

  /**
   * Run the scripts if any lines were generated and log errors if script fails to run (exit != 0)
   * @param commands Script contents
   * @throws IOException IO errors
   * @throws InterruptedException Interruption errors
   */
  void runProcess(final String commands) throws IOException, InterruptedException {
    if (commands.length() == 0) {
      log.info("No changes generated.");
    } else {
      final String sName = getClass().getSimpleName() + "_" + name + ".bsh";
      final File script = new File(System.getProperty("java.io.tmpdir"), sName);
      script.deleteOnExit();
      final BufferedWriter writer = new BufferedWriter(new FileWriter(script, false));
      try {
        writer.write("#!/bin/bash");
        writer.newLine();
        writer.append(commands);
        writer.newLine();
      } finally {
        writer.close();
      }

      final List<String> command = Arrays.asList("bash", script.getPath());
      try {
        final LwpOutput result = LwProcess.execute(command, true, log);
        log.fine("Returned from " + script.getPath() + " with exit " + result.getExitCode());
        if (result.getExitCode() != 0) {
          log.severe(result.getStdout());
        }
      } catch (LwpException e) {
        log.log(Level.SEVERE, e.getMessage(), e);
      }

      if (!script.delete()) {
        log.warning("Couldn't delete " + script.getPath() + ", next run should over-write it.");
      }
    }
  }
}
