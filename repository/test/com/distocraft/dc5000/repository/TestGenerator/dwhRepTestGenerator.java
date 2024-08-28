package com.distocraft.dc5000.repository.TestGenerator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.StringTokenizer;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import com.distocraft.dc5000.repository.rockMaker.CreateMetadata;
import com.distocraft.dc5000.repository.rockMaker.MetaColumn;
import com.distocraft.dc5000.repository.rockMaker.MetaConnection;
import com.distocraft.dc5000.repository.rockMaker.MetaDataProvider;
import com.distocraft.dc5000.repository.rockMaker.MetaSequence;
import com.distocraft.dc5000.repository.rockMaker.MetaTable;

/**
 * Class which auto generates test classes for dwhrep classes.<br>
 * <br>
 * Define database connection details from where the meta data is fetched in
 * "createTestData.properties" file located in test directory. Template used in
 * creating test files is also located in the same directory. Tests are created
 * to com.distocraft.dc5000.repository.dwhrep in test directory.
 * 
 * @author EJAAVAH
 * 
 */
public class dwhRepTestGenerator extends CreateMetadata {

  private Collection stringToCollection(String str, String delim) {
    final Collection<String> result = new ArrayList<String>();
    final StringTokenizer token = new StringTokenizer(str, delim);
    while (token.hasMoreElements()) {
      result.add(token.nextToken());
    }
    return result;
  }
	@Override
  public void execute() {

    try {
      
      /* Connection to database using connection details defined in properties */
      final MetaConnection metaConnect = new MetaConnection(property.getProperty(propertyKey + "url"), property
          .getProperty(propertyKey + "driver"), property.getProperty(propertyKey + "username"), property
          .getProperty(propertyKey + "password"));

      /* Initializing object which gives us meta data from the database */
      final MetaDataProvider mdp = metaConnect.getMetaDataProvider(property.getProperty(propertyKey + "catalog"), property
          .getProperty(propertyKey + "schema"), property.getProperty(propertyKey + "tablePattern"), stringToCollection(
          property.getProperty(propertyKey + "sequenceColumns"), ","), false);

      /* Creating and initializing velocity engine & context */
      final VelocityContext context = new VelocityContext();
      final VelocityEngine ve = new VelocityEngine();
      ve.init();

      /* Table object including meta data of all the tables */
      MetaTable[] table = mdp.getTables();

      final boolean includeTimestamps = Boolean.valueOf(property.getProperty(propertyKey + "includetimestamp"));

      /* Counter for created files */
      int createdFileCounter = 0;

      for (MetaTable aTable : table) {

        /* name of the table */
        final String name = aTable.getCapitalizedName();

        /* Metadata from columns */
        final MetaColumn[] col = aTable.getColumns();
        final MetaColumn[] pkcol = aTable.getPrimaryKeyColumns();
        final MetaSequence[] seqcol = aTable.getSequences();
        final MetaTable[] impTable = aTable.getImportingTables();

        final File outDir = new File(property.getProperty(propertyKey + "outDir"));
        if (!outDir.exists()) {
          outDir.mkdirs();
        }

        final File javaTest = new File(outDir, name + "Test.java");
        final File javaFacTest = new File(outDir, name + "FactoryTest" + ".java");


        /* Opening writer to the created test file */
        final BufferedWriter bwDwhRepTest = new BufferedWriter(new FileWriter(javaTest, false));
        final BufferedWriter bwDwhRepTestFactory = new BufferedWriter(new FileWriter(javaFacTest, false));
        final BufferedReader brDwhRepTestTemplate = new BufferedReader(new FileReader(property.getProperty(propertyKey
          + "JUnitTestTemplate")));
        final BufferedReader brDwhRepTestFactoryTemplate = new BufferedReader(new FileReader(property.getProperty(propertyKey
          + "JUnitTestFactoryTemplate")));


        /* Adding values to context object */
        context.put("packageName", property.get(propertyKey + "packageName"));
        context.put("testClassName", name + "Test");
        context.put("testedClassName", name);
        context.put("testFactoryClassName", name + "FactoryTest");
        context.put("testedFactoryClassName", name + "Factory");
        context.put("columns", col);
        context.put("seqColumns", seqcol);
        context.put("impTables", impTable);
        context.put("pkColumns", pkcol);
        context.put("includeTimestamps", includeTimestamps);

        /* Create the files */
        ve.evaluate(context, bwDwhRepTest, "", brDwhRepTestTemplate);
        System.out.println(javaTest.getName());
        ve.evaluate(context, bwDwhRepTestFactory, "", brDwhRepTestFactoryTemplate);
        System.out.println(javaFacTest.getName());

        /* Closing streams */
        bwDwhRepTest.close();
        brDwhRepTestTemplate.close();
        bwDwhRepTestFactory.close();
        brDwhRepTestFactoryTemplate.close();

        createdFileCounter += 2;
      }

      System.out.println("Generate succeeded!");
      System.out.println(createdFileCounter + " file(s) were created.");

    } catch (Exception e) {
      System.out.println("Exception caught in generate! \n");
      e.printStackTrace();
    }
  }

  /**
   * Main method for generating the test classes
   *
   * @param args
   */
  public static void main(String[] args) {

    try {
      if (args.length >= 1) {
        final String key = args[0];
        String file = "";
        if (args.length > 1) {
          file = args[1];
        }
        final dwhRepTestGenerator drtg = new dwhRepTestGenerator();
        drtg.init(key, file);
        drtg.execute();
      } else {
        System.out.println("Usage:\n propertyKey [propertyfile]");
      }
    } catch (Exception e) {
      System.out.println("Exception caught in main! \n");
      e.printStackTrace();
    }
  }
}
