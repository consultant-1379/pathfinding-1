package com.distocraft.dc5000.repository.cache;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;


/**
 * Parser and loader specific cache implementation for accessing DataFormats
 * 
 * @author lemminkainen
 * 
 */
public class DataFormatCache {

  private static final Logger LOG = Logger.getLogger("etlengine.repository.DataFormatCache");

  private static String defaultSchema = "dwhrep.";
  static {
	  DataFormatCacheCore.setDefaultSchema(System.getProperty(defaultSchema, defaultSchema));	  
  }

  /**
   * Used for testing only....
   * @param newSchema The DWHREP schema name, defaults to 'dwhrep.'
   */
  public static void resetSchema(final String newSchema){	
	  DataFormatCacheCore.resetSchema(newSchema);    
  }  

  private static DataFormatCache dfc = null;

  private DataFormatCache() {
  }

  public static void initialize(final RockFactory rock, final String dburl, final String dbusr, final String dbpwd) {
	  DataFormatCache.initialize(rock.getDriverName(), dburl, dbusr, dbpwd);
  }
  
  public static void initialize(final String driver, final String dburl, final String dbusr, final String dbpwd) {

    dfc = new DataFormatCache();
    DataFormatCacheCore.initialize(driver, dburl, dbusr, dbpwd);
  }

  
  public static void initialize(final RockFactory rock) {	  
	  dfc = new DataFormatCache();
	  DataFormatCacheCore.initialize(rock);
  }

  public static DataFormatCache getCache() {
    return dfc;
  }

  /**
   * Method to init this cache. Testing purposes only.
   */
  public static void testInitialize(final Map<String, DFormat> it_map, final Map<String, List<DFormat>> id_map,
      final Set<String> if_names, final Map<String, DFormat> folder_map) {

    dfc = new DataFormatCache();
    DataFormatCacheCore.testInitialize(it_map, id_map, if_names, folder_map);    
  }

  public DFormat getFormatWithTagID(final String interfaceName, final String tagID) {
	 return DataFormatCacheCore.getFormatWithTagID(interfaceName, tagID);
  }

  public DFormat getFormatWithTagIDWithoutInterface(final String tagID) {
	    return DataFormatCacheCore.getFormatWithTagIDWithoutInterface(tagID);
	  }
  
  public List<DFormat> getFormatWithFormatID(final String interfaceName, final String dataFormatID) {
    return DataFormatCacheCore.getFormatWithFormatID(interfaceName, dataFormatID);
  }
  
  public Vector<String> getInterfaceNames() {
	  return DataFormatCacheCore.getInterfaceNames();
  }
  
  /**
   * This method returns the data format object (DFormat) of the measurement type who's load file would be put into directory folderName
   * 
   * @param folderName  String with directory name of a load file (not the path).
   * @return  The DFormat that maps to folderName
   */
  public DFormat getFormatWithFolderName(final String folderName){	 
    return DataFormatCacheCore.getFormatWithFolderName(folderName);
  }

  public boolean isAnInterface(final String ifname) {
    return DataFormatCacheCore.isAnInterface(ifname);
  }

  public void revalidate() throws Exception {
	  DataFormatCacheCore.revalidate();	 
  }
}
