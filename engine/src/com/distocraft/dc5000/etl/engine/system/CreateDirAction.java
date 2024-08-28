package com.distocraft.dc5000.etl.engine.system;

import com.ericsson.eniq.common.lwp.LwProcess;
import com.ericsson.eniq.common.lwp.LwpException;
import com.ericsson.eniq.common.lwp.LwpOutput;
import java.io.File;
import java.util.Properties;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * 
 * creates directory (or all directories leading to it) . Path is read from
 * Action_contents.
 * 
 * /1/2/3/4
 * 
 * if directories 1,2 and exists only 4 is created. if only 1 is allready
 * created then 2,3 and 4 are created.
 * 
 * 
 * 
 * TODO usage TODO used databases/tables TODO used properties
 * 
 * @author savinen Copyright Distocraft 2005
 * 
 * $id$
 */
public class CreateDirAction extends TransferActionBase {

  private static Logger log = Logger.getLogger("etlengine.CreateDirAction");

  /**
   * Empty protected constructor
   * 
   */
  @SuppressWarnings({"UnusedDeclaration"})
  protected CreateDirAction() {
  }

  /**
   * Constructor
   * 
   * @param version
   *          metadata version
   * @param collectionSetId
   *          primary key for collection set
   * @param collection collection
   * @param transferActionId
   *          primary key for transfer action
   * @param transferBatchId
   *          primary key for transfer batch
   * @param connectId
   *          primary key for database connections
   * @param rockFact
   *          metadata repository connection object
   * @param trActions
   *          object that holds transfer action information (db contents)
   *
   * @throws com.distocraft.dc5000.etl.engine.common.EngineMetaDataException errors
   */
  public CreateDirAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
      final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
      final Meta_transfer_actions trActions) throws EngineMetaDataException {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

  }

  /**
   * 
   */
  public void execute() throws EngineException {
    String directory = this.getTrActions().getWhere_clause();

    if(directory.contains("${")) {
      final int start = directory.indexOf("${");
      final int end = directory.indexOf("}",start);
      
      if(end >= 0) {
        final String variable = directory.substring(start+2,end);
        final String val = System.getProperty(variable);
        directory = directory.substring(0,start) + val + directory.substring(end+1);
      }
    }
    
    try {

      final Properties prop = TransferActionBase.stringToProperties(this.getTrActions().getAction_contents());

      final File dir = new File(directory);

      if (!dir.isDirectory()) {

        if (!dir.mkdirs()) {

          throw new Exception("Error while executing mkdir " + directory);

        }

      } else {

        log.info("Directory: " + directory + " allready exists ");
      }

      chmod(prop.getProperty("permission", "750"), dir);

      if (!prop.getProperty("group", "").equalsIgnoreCase("")) {
        chgrp(prop.getProperty("group", ""), dir);
      }

      if (!prop.getProperty("owner", "").equalsIgnoreCase("")) {
        chown(prop.getProperty("owner", ""), dir);
      }

    } catch (Exception e) {
      throw new EngineException("Error handling directory " + directory, new String[] { directory }, e, this, this
          .getClass().getName(), EngineConstants.ERR_TYPE_SYSTEM);
    }

  }

  private void chmod(final String cmd, final File file) {

    log.info("changing " + file.getAbsolutePath() + " permissions to " + cmd);
    execCmd("chmod " + cmd + " " + file.getAbsolutePath());
  }

  private void chown(final String cmd, final File file) {
    log.info("changing " + file.getAbsolutePath() + " owner to " + cmd);
    execCmd("chown " + cmd + " " + file.getAbsolutePath());
  }

  private void chgrp(final String cmd, final File file) {
    log.info("changing " + file.getAbsolutePath() + " group to " + cmd);
    execCmd("chgrp " + cmd + " " + file.getAbsolutePath());

  }


  private void execCmd(final String cmd) {
    try {
      final LwpOutput result = LwProcess.execute(cmd, true, null);
      if (result.getExitCode() > 0) {
        log.severe("Error while executing : " + cmd + " :: " + result.getStdout());
      }
    } catch (LwpException e) {
      log.severe("Error while executing : " + cmd);
      // throw new Exception(e);
    }
  }
}

/*
Once JDK7 is being used DirSetPermissions can be removed and the code below can replace CreateDirAction for setting
file permissions etc.

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.attribute.UserPrincipalNotFoundException;
import java.util.HashSet;
import java.util.Set;

public class FilePermissionsExample {
  private static final int MASK_READ = 4;
  private static final int MASK_WRITE = 2;
  private static final int MASK_EXECUTE = 1;

  public static void main(String[] args) {
    final String _file = args[0];
    final String octalPerms = args[1];
    final String fileOwner = args[2];
    final String fileGroup = args[3];
    final File file = new File(_file);
    final Path filePath = file.toPath();

    final char[] bits = octalPerms.toCharArray();
    if (bits.length != 3 && bits.length != 4) {
      throw new RuntimeException("Must be 4 characters ...");
    }
    int startIndex = 0;
    if (bits.length == 4) {
      startIndex = 1;
    }
    final char user = bits[startIndex++];
    final char group = bits[startIndex++];
    final char other = bits[startIndex];


    final Set<PosixFilePermission> permissions = new HashSet<>(7);
    if (canRead(user)) {
      permissions.add(PosixFilePermission.OWNER_READ);
    }
    if (canWrite(user)) {
      permissions.add(PosixFilePermission.OWNER_WRITE);
    }
    if (canExecute(user)) {
      permissions.add(PosixFilePermission.OWNER_EXECUTE);
    }

    if (canRead(group)) {
      permissions.add(PosixFilePermission.GROUP_READ);
    }
    if (canWrite(group)) {
      permissions.add(PosixFilePermission.GROUP_WRITE);
    }
    if (canExecute(group)) {
      permissions.add(PosixFilePermission.GROUP_EXECUTE);
    }

    if (canRead(other)) {
      permissions.add(PosixFilePermission.OTHERS_READ);
    }
    if (canWrite(other)) {
      permissions.add(PosixFilePermission.OTHERS_WRITE);
    }
    if (canExecute(other)) {
      permissions.add(PosixFilePermission.OTHERS_EXECUTE);
    }


    final UserPrincipalLookupService userService = FileSystems.getDefault().getUserPrincipalLookupService();
    try {
      Files.setPosixFilePermissions(filePath, permissions);
      System.out.println("Permissions set on " + filePath);

      setOwner(filePath, fileOwner, userService);
      setGroup(filePath, fileGroup, userService);

    } catch (IOException|UnsupportedOperationException e) {
      e.printStackTrace();
    }
  }

  private static void setGroup(final Path filePath, final String fileGroup, final UserPrincipalLookupService userService){
    try{
      final GroupPrincipal groupPrincipal = userService.lookupPrincipalByGroupName(fileGroup);
      final PosixFileAttributeView postixView = Files.getFileAttributeView(filePath, PosixFileAttributeView.class);
      postixView.setGroup(groupPrincipal);
      System.out.println("Group set tp "+groupPrincipal+" on " + filePath);
    } catch(UserPrincipalNotFoundException e){
      System.out.println("Group '"+fileGroup+"' does not exist.");
    } catch (IOException|UnsupportedOperationException e) {
      e.printStackTrace();
    }
  }

  private static void setOwner(final Path filePath, final String fileOwner, final UserPrincipalLookupService userService){
    try{
      final UserPrincipal ownerPrincipal = userService.lookupPrincipalByName(fileOwner);
      Files.setOwner(filePath, ownerPrincipal);
      System.out.println("Owner set to "+ownerPrincipal+" on " + filePath);
    } catch(UserPrincipalNotFoundException e){
      System.out.println("User '"+fileOwner+"' does not exist.");
    } catch (IOException|UnsupportedOperationException e) {
      e.printStackTrace();
    }
  }


  public static boolean canRead(final char bit) {
    return check(bit, MASK_READ);
  }

  public static boolean canWrite(final char bit) {
    return check(bit, MASK_WRITE);
  }

  public static boolean canExecute(final char bit) {
    return check(bit, MASK_EXECUTE);
  }

  private static boolean check(final char perm, final int mask) {
    final int _int = Character.digit(perm, 8);
    return (_int & mask) == mask;
  }
}

*/