package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;

import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * 
 * @author ejarsok
 * 
 */

public class CreateDirActionTest {

  private static RockFactory rockFact = null;

  private static Long collectionSetId = 1L;

  private static Long transferActionId = 1L;

  private static Long transferBatchId = 1L;

  private static Long connectId = 1L;

  private static Meta_versions version;

  private static Meta_collections collection;

  private static Meta_transfer_actions trActions;

  private Method method;

  private static File file;
  
  private static Statement stm;

  private static final File tmpDir = new File(System.getProperty("java.io.tmpdir"), "CreateDirAction");

  @BeforeClass
  public static void init() throws Exception {
    StaticProperties.giveProperties(new Properties());

    if(!tmpDir.exists() && !tmpDir.mkdirs()){
      fail("Didn't create required directory " + tmpDir.getPath());
    }
    tmpDir.deleteOnExit();
    file = new File(tmpDir, "file");
    file.deleteOnExit();

    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file));
      pw.print("foobar");
      pw.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Can´t write in file!");
    }

    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e2) {
      e2.printStackTrace();
      fail("init() failed, ClassNotFoundException");
    }
    Connection c;
    try {
      c = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");
      stm = c.createStatement();

      stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
          + "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");

      stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");

    } catch (SQLException e1) {
      e1.printStackTrace();
      fail("init() failed, SQLException");
    }

    try {
      rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
    } catch (SQLException e) {
      e.printStackTrace();
      fail("init() failed, SQLException");
    } catch (RockException e) {
      e.printStackTrace();
      fail("init() failed, RockException");
    }
    version = new Meta_versions(rockFact);
    collection = new Meta_collections(rockFact);
    trActions = new Meta_transfer_actions(rockFact);
  }

  @AfterClass
  public static void afterClass(){
    delete(tmpDir);
  }

  @Test
  public void testExecuteRegression_ETLDIR() throws Exception {

    final String relDir = "dim_e_sgeh/raw";

    System.setProperty("ETLDATA_DIR", tmpDir.getPath());
    System.setProperty("DIRECTORY_STRUCTURE.NoOfDirectories", "4");

    final Meta_transfer_actions mta = new Meta_transfer_actions(null);
    mta.setWhere_clause("${ETLDATA_DIR}/" + relDir);

    final Properties actionContents = new Properties();
    actionContents.setProperty("permission", "664");

    final String acString = TransferActionBase.propertiesToString(actionContents);
    mta.setAction_contents(acString);


    final CreateDirAction action = new CreateDirAction(version, collectionSetId, collection, transferActionId,
          transferBatchId, connectId, rockFact, mta);
    action.execute();

    final File eFile = new File(tmpDir, relDir);
    Assert.assertTrue("Expected directory not created", eFile.exists());
    Assert.assertTrue("Expected directory is not a directory", eFile.isDirectory());
  }

  private static boolean delete(final File dir){
    if(dir.isFile()){
      return dir.delete();
    } else {
      final File[] children = dir.listFiles();
      for(File child : children){
        if(child.isDirectory()){
          delete(child);
        } else {
          if(!child.delete()){
            return false;
          }
        }
      }
      return dir.delete();
    }
  }

  @Test
  public void testChmod() {
    try {
      CreateDirAction instance = new CreateDirAction(version, collectionSetId, collection, transferActionId,
          transferBatchId, connectId, rockFact, trActions);
      Class secretClass = instance.getClass();

      try {
        method = secretClass.getDeclaredMethod("chmod", new Class[] { String.class, File.class });
      } catch (NoSuchMethodException nE) {
        nE.printStackTrace();
        fail("testChmod() secretClass.getDeclaredMethod failed");
      }
      method.setAccessible(true);

      method.invoke(instance, "600", file);
    } catch (Exception e) {
      e.printStackTrace();
      fail("testChmod() failed");
    }
  }

  @Test
  public void testChown() {
    try {
      CreateDirAction instance = new CreateDirAction(version, collectionSetId, collection, transferActionId,
          transferBatchId, connectId, rockFact, trActions);
      Class secretClass = instance.getClass();

      try {
        method = secretClass.getDeclaredMethod("chown", new Class[] { String.class, File.class });
      } catch (NoSuchMethodException nE) {
        nE.printStackTrace();
        fail("testChown() secretClass.getDeclaredMethod failed");
      }
      method.setAccessible(true);

      method.invoke(instance, "testOwner", file);
    } catch (Exception e) {
      e.printStackTrace();
      fail("testChown() failed");
    }
  }

  @Test
  public void testChgrp() {
    try {
      CreateDirAction instance = new CreateDirAction(version, collectionSetId, collection, transferActionId,
          transferBatchId, connectId, rockFact, trActions);
      Class secretClass = instance.getClass();

      try {
        method = secretClass.getDeclaredMethod("chgrp", new Class[] { String.class, File.class });
      } catch (NoSuchMethodException nE) {
        nE.printStackTrace();
        fail("testChgrp() secretClass.getDeclaredMethod failed");
      }
      method.setAccessible(true);

      method.invoke(instance, "testGroup", file);
    } catch (Exception e) {
      e.printStackTrace();
      fail("testChgrp() failed");
    }
  }

  @AfterClass
  public static void clean() {
    try {
      stm.execute("DROP TABLE Meta_collection_sets");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

}
