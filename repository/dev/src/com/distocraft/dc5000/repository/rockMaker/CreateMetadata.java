package com.distocraft.dc5000.repository.rockMaker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

public class CreateMetadata {


  private static final Vector engines = new Vector(); // VEngine instance cache

  private static final String propertyFILE = "/conf/createMetadata.properties";
  private static final String propertyETLC = "/conf/ETLCServer.properties";
  protected Properties property;
  protected Properties etlcProperties;
  protected String propertyKey = "";
  
  public Properties readpropertyFile(String propertyFilename) throws Exception {

    System.out.println("working dir = " + System.getProperty("user.dir"));
    Properties prop = new Properties();
    BufferedReader br = new BufferedReader(new FileReader(propertyFilename));
    String line;
    while ((line = br.readLine()) != null) {

      if (!line.equalsIgnoreCase("")) {

        String[] splitted = line.split("=");
        final String name = splitted[0].trim();
        String value = "";
        if (splitted.length > 1) {
          value = splitted[1].trim();
        }
        prop.setProperty(name, value);

      }
    }

    br.close();
    return prop;

  }


  private Collection stringToCollection(final String str, final String delim) {
    final Collection<String> result = new ArrayList<String>();
    StringTokenizer token = new StringTokenizer(str, delim);
    while (token.hasMoreElements()) {
      result.add(token.nextToken());
    }
    return result;
  }


  public void init(String key, String propertyFilename) throws Exception {

    
	  if (!key.endsWith("."))
      key += ".";
    this.propertyKey = key;

    if (propertyFilename == null || propertyFilename.equalsIgnoreCase("")) {

      property = readpropertyFile(System.getProperty("user.dir") + propertyFILE);

    } else {

      property = readpropertyFile(propertyFilename);

    }
    etlcProperties=readpropertyFile(System.getProperty("user.dir") + propertyETLC);
  }


  /**
   * Static accessor for VelocityEngine instance pooling. After acquired
   * engine is no longer used it shall be returned into pool by adding it into
   * vector engines.
   *
   * @return a VelocityEngine
   * @throws Exception if engine initialization fails
   */
  private static VelocityEngine getVelocityEngine() throws Exception {

    synchronized (engines) {
      if (engines.size() > 0) {
        return (VelocityEngine) engines.remove(0);
      }
    }

    VelocityEngine ve = new VelocityEngine();
    ve.init();
    return ve;

  }

  @SuppressWarnings({"ResultOfMethodCallIgnored"})
  public void execute() throws Exception {

    // Construct meta-connection and metadata-provider
    MetaConnection metaConnect =
      new MetaConnection(property.getProperty(propertyKey + "url"), etlcProperties.getProperty("ENGINE_DB_DRIVERNAME"), property.getProperty(propertyKey + "username"), property.getProperty(propertyKey + "password"));

    MetaDataProvider mdp =
      metaConnect.getMetaDataProvider(
        property.getProperty(propertyKey + "catalog"),
        property.getProperty(propertyKey + "schema"),
        property.getProperty(propertyKey + "tablePattern"),
        stringToCollection(property.getProperty(propertyKey + "sequenceColumns"), ","),
        false);

    VelocityEngine ve = getVelocityEngine();
    VelocityContext context = new VelocityContext();

    MetaTable[] table = mdp.getTables();
    System.out.println("Number of tables fetched: " + table.length);

    final File outDir = new File(property.getProperty(propertyKey + "outDir"));

    for (MetaTable aTable : table) {
      System.out.println(aTable.getCapitalizedName());

      String name = aTable.getCapitalizedName();

      MetaColumn[] col = aTable.getColumns();
      MetaColumn[] pkcol = aTable.getPrimaryKeyColumns();
      MetaSequence[] seqcol = aTable.getSequences();
      MetaTable[] impTable = aTable.getImportingTables();


      final File javaFile = new File(outDir, name + ".java");
      if (!javaFile.getParentFile().exists()) {
        javaFile.getParentFile().mkdirs();
      }
      final File javaFacFile = new File(outDir, name + "Factory.java");
      if (!javaFacFile.getParentFile().exists()) {
        javaFacFile.getParentFile().mkdirs();
      }


      BufferedWriter bwBasic = new BufferedWriter(new FileWriter(javaFile));
      BufferedWriter bwFactory = new BufferedWriter(new FileWriter(javaFacFile));

      BufferedReader brBasic = new BufferedReader(new FileReader(property.getProperty(propertyKey + "basicTemplateIn")));
      BufferedReader brFactory = new BufferedReader(new FileReader(property.getProperty(propertyKey + "factoryTemplateIn")));

      context.put("packageName", property.get(propertyKey + "packageName"));
      context.put("className", name);
      context.put("columns", col);
      context.put("seqColumns", seqcol);
      context.put("impTables", impTable);
      context.put("pkColumns", pkcol);

      ve.evaluate(context, bwBasic, "", brBasic);
      ve.evaluate(context, bwFactory, "", brFactory);

      bwBasic.close();
      bwFactory.close();
      brBasic.close();
      brFactory.close();

    }

  }


  public static void main(String[] args) {

    try {

      if (args.length >= 1) {


        String key = args[0];
        String file = "";
        if (args.length > 1) file = args[1];

        CreateMetadata c = new CreateMetadata();
        c.init(key, file);
        c.execute();

      } else {
        System.out.println("Usage:\n propertyKey [propertyfile]");
      }

    } catch (Exception e) {
      System.out.println("ERROR: " + e);
    }

  }

}
