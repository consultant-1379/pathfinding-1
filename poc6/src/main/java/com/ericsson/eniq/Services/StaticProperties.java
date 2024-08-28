package com.ericsson.eniq.Services;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created on Feb 18, 2005
 * 
 * A property implementation that lies as static entity on ETLC.
 * 
 * @author lemminkainen
 */
public class StaticProperties {

  public static final String DC5000_CONFIG_DIR = "dc5000.config.directory";

  private static final String PROPERTYFILENAME = "static.properties";

  private static Logger log = Logger.getLogger("etlengine.common.StaticProperties");

  private static java.util.Properties props = null;

  private StaticProperties(){ }

  /**
   * Returns the value of defined property
   * 
   * @param name
   *          name of property
   * @return value of property
   * @throws NoSuchFieldException
   *           is thrown if property is not defined
   */
  public static String getProperty(final String name) throws NoSuchFieldException {
    if (props == null) {
      throw new NullPointerException("StaticProperties class is not initialized"); //NOPMD
    }
      
    final String value = props.getProperty(name);
    if(value == null){
      throw new NoSuchFieldException("Property " + name + " not defined in " + PROPERTYFILENAME);
    } else {
      return value;
    }
  }

  /**
   * Returns the value of defined property with default value.
   * 
   * @param name
   *          name of property
   * @param defaultValue
   *          default value of property that is used if property is not defined
   *          on config-file.
   * @return value of property
   */
  public static String getProperty(final String name, final String defaultValue) {
    if (props == null) {
      throw new NullPointerException("StaticProperties class is not initialized"); //NOPMD
    }
      
    return props.getProperty(name, defaultValue);
  }

  /**
   * Reloads the configuration file
   * 
   * @throws IOException
   *           thrown in case of failure
   */
  public static void reload() throws IOException {
    try {

    	final java.util.Properties nprops = new java.util.Properties();
      final File confFile = getStaticFile();
      FileInputStream fis = null;
      try {
        fis = new FileInputStream(confFile);
        nprops.load(fis);
      } finally {
        if(fis != null) {
          try {
            fis.close();
          } catch(Exception e) {
            log.log(Level.WARNING,"Error closing file",e);
          }
        }
      }
      
      props = nprops;
      
      log.info("Configuration successfully (re)loaded.");

    } catch (IOException e) {
      log.log(Level.WARNING,"Reading config file failed",e);
      throw e;
    }

  }


  private static File getStaticFile() throws IOException {
    String confDir = "/eniq/installer/";
    
    if (confDir == null) {
      throw new NullPointerException("System property " + DC5000_CONFIG_DIR + " must be defined"); //NOPMD
    }
    if (!confDir.endsWith(File.separator)) {
      confDir += File.separator;
    }
    final File confFile = new File(confDir + PROPERTYFILENAME);
    if (!confFile.exists() && !confFile.isFile() && !confFile.canRead()) {
      throw new IOException("Unable to read configFile from " + confFile.getCanonicalPath());
    }
    return confFile;
  }

  public static void save() throws IOException {
    final File confFile = getStaticFile();
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(confFile);
      props.store(fos, "Saving static.properties");

    } catch (Exception e) {
      log.log(Level.WARNING, "Error saving file", e);
    } finally {
      if(fos != null){
        try{
          fos.close();
        } catch (Throwable t){/**/}
      }
    }
  }

  /**
   * Reloads the configuration file
   * @param nprops To set
   */
  public static void giveProperties(final java.util.Properties nprops) {
    props = nprops;
  }

   /**
   * Updates the property name and value
   *
   * @param updatedName - Property name
   * @param updatedValue - Property value
    * @return TRUE is property set and saved, false otherwise
   */
  public static boolean setProperty(final String updatedName, final String updatedValue) {
    if (props == null) {
      log.log(Level.WARNING, "Error setting properties. Static properties not initialized.");
      return false;
    }
    // run the new save method here
    try {
      props.setProperty(updatedName, updatedValue);
      save();
      reload();
      return true;
    } catch (Exception e) {
      log.log(Level.WARNING, "Error setting properties", e);
      return false;
    }
  }
  
}

