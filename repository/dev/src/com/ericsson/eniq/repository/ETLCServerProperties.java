package com.ericsson.eniq.repository;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("serial")
public class ETLCServerProperties extends Properties {

  public final static String DBURL = "ENGINE_DB_URL";

  public final static String DBUSERNAME = "ENGINE_DB_USERNAME";

  public final static String DBPASSWORD = "ENGINE_DB_PASSWORD";

  public final static String DBDRIVERNAME = "ENGINE_DB_DRIVERNAME";

  static final private String[] CRYPTEDPROPS = new String[]{DBPASSWORD};

  public static final String DC_CONFIG_DIR_PROPERTY_NAME = "dc.config.dir";
  public static final String DC5000_CONFIG_DIR_PROPERTY_NAME = "dc5000.config.directory";

  public static final String CONFIG_DIR_PROPERTY_NAME = "CONF_DIR";

  /**
   * Default constructor
   *
   * @throws IOException
   */
  public ETLCServerProperties() throws IOException {
    super();

    final StringBuffer confDirPath = new StringBuffer(System.getProperty(DC_CONFIG_DIR_PROPERTY_NAME, "/eniq/sw/conf"));

    if (!confDirPath.toString().endsWith(File.separator)) {
      confDirPath.append(File.separator);
    }
    confDirPath.append("ETLCServer.properties");
    init(confDirPath.toString());
  }

  public ETLCServerProperties(final String filepath) throws IOException {
    super();
    init(filepath);
  }

  private void init(final String filepath) throws IOException {
    final File file = new File(filepath);

    final FileInputStream fis = new FileInputStream(file);
    try {
      load(fis);
    } finally {
      fis.close();
    }
  }

  @Override
  public void load(final InputStream inStream) throws IOException {
    synchronized (this) {
      super.load(inStream);
      decryptCrypted();
    }
  }

  @Override
  public void load(final Reader reader) throws IOException {
    synchronized (this) {
      super.load(reader);
      decryptCrypted();
    }
  }

  @Override
  public void loadFromXML(final InputStream istream) throws IOException,
    InvalidPropertiesFormatException {
    synchronized (this) {
      super.loadFromXML(istream);
      decryptCrypted();
    }
  }

  /**
   * Decrypt the crypted properties
   *
   * @throws Exception
   */
  private void decryptCrypted() {

    for (String propName : CRYPTEDPROPS) {
      final String cryptedValue = getProperty(propName);
      if (cryptedValue != null && cryptedValue.length() > 0) {
        String decryptedValue;
        try {
          decryptedValue = AsciiCrypter.getInstance().decrypt(cryptedValue);
        } catch (Exception e) {
          decryptedValue = "";
        }
        if (decryptedValue != null && decryptedValue.length() > 0) {
          setProperty(propName, decryptedValue);
        }
      }
    }
  }

  /**
   * @param propertyName
   * @param propertyValue
   */
  public void addProperty(final String propertyName,
                          final String propertyValue) {
    super.setProperty(propertyName, propertyValue);
  }

  /**
   * This function reads the database connection details from the file
   * ${configurationDirectory}/ETLCServer.properties
   *
   * @return Returns a HashMap with the database connection details.
   */
  public Map<String, String> getDatabaseConnectionDetails() {
    final HashMap<String, String> dbConnDetails = new HashMap<String, String>();

    dbConnDetails.put("etlrepDatabaseUrl", getProperty(DBURL));
    dbConnDetails.put("etlrepDatabaseUsername", getProperty(DBUSERNAME));
    dbConnDetails.put("etlrepDatabasePassword", getProperty(DBPASSWORD));
    dbConnDetails.put("etlrepDatabaseDriver", getProperty(DBDRIVERNAME));

    return dbConnDetails;
  }

}
