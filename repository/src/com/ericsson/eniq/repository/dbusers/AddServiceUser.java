package com.ericsson.eniq.repository.dbusers;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.ericsson.eniq.repository.DBUsersGet;
import com.ericsson.eniq.repository.ETLCServerProperties;
import com.distocraft.dc5000.etl.gui.config.ServiceNames;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import static com.ericsson.eniq.common.Constants.CONF_DIR_PROPERTY_NAME;
import static com.ericsson.eniq.common.Constants.ENGINE_DB_DRIVERNAME;
import static com.ericsson.eniq.common.Constants.ENGINE_DB_PASSWORD;
import static com.ericsson.eniq.common.Constants.ENGINE_DB_URL;
import static com.ericsson.eniq.common.Constants.ENGINE_DB_USERNAME;

public class AddServiceUser {

  private static final int EXIT_OK = 0;
  private static final int EXIT_USAGE = 2;
  private static final int EXIT_COUGHT_ERROR = 4;
  private static final int EXIT_UNCOUGHT_ERROR = 5;
  private static final String DCUSER = "dcuser";
  private static final String ADD_SERVICE_USER_CONN_NAME = "asu-";
  private static final String ENGINE_SERVICE_NAME = "engine";
  private long lastModifiedTime;
  protected Map<String, List<String>> hostToServiceMap;

  private static final String GROUP_REGEX_SERVICENAMES = "(.*)::(.*)::(.*)";
  private static final String RW_IQNAME = "^dwh_(reader|writer)_\\d+$";
  private static final String EC_NAME = "^ec_\\w+$";
  private static final String DATE_FORMAT = "yyyyMMdd'T'HHkkmmssSSS";


  private String getUsage(){
    return "Add a service user info to repdb\n\t-all | -u username -p password -s service_name\n";
  }

  void usage() {
    log(getUsage());
    exit();
  }

  private static void __exit(final int code){
    System.exit(code);
  }

  void exit() {
    __exit(EXIT_USAGE);
  }

  public static void main(final String[] args) {
    final AddServiceUser addUsers = new AddServiceUser();
    if (args == null || args.length == 0) {
      addUsers.usage();
    }
    try {
      addUsers.addServiceUsers(args);
      __exit(EXIT_OK);
    } catch (TmpException e) {
      System.out.println("Error adding service entries");
      e.printStackTrace(System.out);
      __exit(EXIT_COUGHT_ERROR);
    } catch (Throwable t) {
      System.out.println("Error adding service entries");
      t.printStackTrace(System.out);
      __exit(EXIT_UNCOUGHT_ERROR);
    }
  }

  public void addServiceUsers(final String[] args) {
    if (args == null || args.length == 0) {
      throw getException(getUsage());
    }
    boolean addAll = false;
    String username = null;
    String password = null;
    String service_name = null;

    if (args.length == 1 && args[0].equals("-all")) {
      addAll = true;
    } else if (args.length == 6) {
      username = getArgValue("-u", args);
      password = getArgValue("-p", args);
      service_name = getArgValue("-s", args);
    } else {
      throw getException(getUsage());
    }
    final SimpleDateFormat dateFomratter = new SimpleDateFormat(DATE_FORMAT);
    final String connName = ADD_SERVICE_USER_CONN_NAME + dateFomratter.format(new Date());
    final RockFactory etlrep = getEtlrep(connName);
    try {
      final Meta_databases where = new Meta_databases(etlrep);
      final Meta_databasesFactory mFac = new Meta_databasesFactory(etlrep, where);
      final List<Meta_databases> all = mFac.get();
      final AtomicLong connectionIdHolder = new AtomicLong(all.size() + 1);
      log("New connectionIds will start start from " + connectionIdHolder.get());
      if (addAll) {
        addAllServices(connectionIdHolder, etlrep);
      } else {
        addService(connectionIdHolder, username, password, service_name, etlrep);
      }
      try {
        etlrep.getConnection().commit();
      } catch (SQLException e) {
        throw getException("Failed to commit changes", e);
      }
    } catch (SQLException e) {
      throw getException("Failed to get current user entries", e);
    } catch (RockException e) {
      throw getException("Failed to get current user entries", e);
    } finally {
      close(etlrep);
    }
  }

  private void addAllServices(final AtomicLong startConnId, final RockFactory etlrep) {
    final String CONF_DIR = System.getProperty(CONF_DIR_PROPERTY_NAME, "/eniq/sw/conf");
    final File serviceNames = new File(CONF_DIR, "service_names");
    BufferedReader reader = null;
    //service_names file format is <ip_address>::<hostname>::<service>

    final Pattern splitter = Pattern.compile(GROUP_REGEX_SERVICENAMES);
    try {
      reader = new BufferedReader(new FileReader(serviceNames));
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0 || line.charAt(0) == '#') {
          //comment/empty line
          continue;
        }
        final Matcher matcher = splitter.matcher(line);
        if (matcher.matches() && matcher.groupCount() == 3) {
          final String service_name = matcher.group(3);
          try {
			  String password = null;
			  
			  if((password=getPassword(DCUSER,ENGINE_SERVICE_NAME)) == null) {
				  password=DCUSER;
			  }
			  
        	 addService(startConnId, DCUSER, password, service_name, etlrep);  
		} catch (Exception e) {
		  throw getException("Cannot add " +service_name+ "due to ", e );
		}
        } else {
          log("Unknown service_names line format '" + line + "'");
        }
      }
    } catch (FileNotFoundException e) {
      throw getException("service_names file not found!", e);
    } catch (IOException e) {
      throw getException("Problems reading service_names file", e);
    } finally {
      if(reader != null){
        try {
          reader.close();
        } catch (Throwable e) {/*--*/}
      }
    }
  }
  
  private static String getPassword(String userName, String servicesname) throws Exception {
		List<Meta_databases> metadList = DBUsersGet.getMetaDatabases(userName,servicesname);
		if(metadList.isEmpty())
			return null;
		return metadList.get(0).getPassword();
	}

  private void addDbService(final AtomicLong nextConnectionId, final String serviceName, final RockFactory etlrep) {
    final Meta_databases where = new Meta_databases(etlrep);
    where.setConnection_name("dwh");
    long connId = nextConnectionId.get();
    String username = "";
    try {
      final Meta_databasesFactory fac = new Meta_databasesFactory(etlrep, where);
      final List<Meta_databases> templates = fac.get();
      if (templates.isEmpty()) {
        throw getException("No predefined dwh entries found in Meta_Databases?");
      }
      for (Meta_databases entry : templates) {
        final Meta_databases whereCheck = new Meta_databases(etlrep);
        whereCheck.setConnection_name(serviceName);
        whereCheck.setUsername(entry.getUsername());
        whereCheck.setType_name(entry.getType_name());
        final Meta_databasesFactory facCheck = new Meta_databasesFactory(etlrep, whereCheck);
        final List<Meta_databases> existing = facCheck.get();
        if (existing.isEmpty()) {

          final Meta_databases metaDatabaseObject = new Meta_databases(etlrep);
          username = entry.getUsername();
          metaDatabaseObject.setUsername(username);
          metaDatabaseObject.setVersion_number("0");
          metaDatabaseObject.setType_name(entry.getType_name());
          connId = nextConnectionId.getAndIncrement();
          metaDatabaseObject.setConnection_id(connId);
          metaDatabaseObject.setConnection_name(serviceName);
          metaDatabaseObject.setPassword(Base64.getEncoder().encodeToString(entry.getPassword().trim().getBytes()));
          metaDatabaseObject.setEncryption_flag("Y");
          metaDatabaseObject.setDecryptionRequired(false);
          metaDatabaseObject.setDescription("DataWareHouse " + serviceName + " Database");
          metaDatabaseObject.setDriver_name(entry.getDriver_name());

          final String[] urlParts = entry.getConnection_string().split(";", 2);
          final String readerPort = System.getProperty("DWH_READER_SETTINGS.PortNumber", "2642");
          String newUrl = "jdbc:sqlanywhere:host=" + serviceName + ":" + readerPort;
          if (urlParts.length > 1) {
            newUrl += ";"+urlParts[1];
          }
          metaDatabaseObject.setConnection_string(newUrl);
          metaDatabaseObject.insertDB();
          log("Added db entry for " + serviceName + ":" + username);
        }
      }
    } catch (SQLException e) {
      throw getException("Failed to add db entry " + connId + ":" + serviceName + ":" + username, e);
    } catch (RockException e) {
      throw getException("Failed to add db entry " + connId + ":" + serviceName + ":" + username, e);
    }
  }

  private void addEcService(final AtomicLong nextConnectionId, final String serviceName, final RockFactory etlrep) {
	    final Meta_databases where = new Meta_databases(etlrep);
	    String key = null;
	    where.setType_name(DCUSER);
	    try {
	      final Meta_databasesFactory fac = new Meta_databasesFactory(etlrep, where);
	      final List<Meta_databases> templates = fac.get();
	      if (templates.isEmpty()) {
	        throw getException("No predefined ec entries found in Meta_Databases?");
	      }
	      for (Meta_databases entry : templates) {
	    	if (entry.getConnection_name().startsWith("ec_") && (entry.getConnection_name().equals(serviceName))){
	            final Meta_databases whereServiceName = new Meta_databases(etlrep);
	           	whereServiceName.setConnection_name(serviceName);
	           	final Meta_databasesFactory facCheck = new Meta_databasesFactory(etlrep, whereServiceName);
	           	if (facCheck.size() > 0) {
	           	final Meta_databases existing = facCheck.getElementAt(0);
	           	final String password = existing.getPassword();
	           	if (password.isEmpty() || password.equals(DCUSER)) {
	           		populateHostNameToTypeMapping();
	           		for (Map.Entry<String, List<String>> entryMap : hostToServiceMap.entrySet()) {
	           			List<String> value = entryMap.getValue();
	           			if (value.contains(serviceName)){
	           				key = entryMap.getKey();
	           			}
	           		}
	        	      
	           		List<String> servicesForThisHost = hostToServiceMap.get(key);
	           		for (String service : servicesForThisHost) {
	           			String service_name = service.trim();
	           			final Meta_databases whereServiceNameCheck = new Meta_databases(etlrep);
	        			whereServiceNameCheck.setConnection_name(service_name);
	        			whereServiceNameCheck.setUsername(DCUSER);
	        			whereServiceNameCheck.setType_name(DCUSER);
	        			final Meta_databasesFactory serviceCheck = new Meta_databasesFactory(etlrep, whereServiceNameCheck);
	        			if (serviceCheck.size() > 0) {
	        				final Meta_databases present = serviceCheck.getElementAt(0);
	        				final String ecPassword = present.getPassword();
	        				if ((ecPassword.isEmpty() || (ecPassword.equals(DCUSER)))){
	        					continue;
	        			}
	        			else{
	        				final Meta_databases whereEcCheck = new Meta_databases(etlrep, existing.getVersion_number(), existing.getConnection_id());
	        				whereEcCheck.setPassword(Base64.getEncoder().encodeToString(ecPassword.trim().getBytes()));
	        				whereEcCheck.setEncryption_flag("Y");
	        				whereEcCheck.setDecryptionRequired(false);
	        				whereEcCheck.updateDB();
		    	       		log("Update db entry for " + serviceName + " : dcuser" );
		    	       		break;
	        			   }
	        			  }
	        			
	        			}
	           		}
	           	}
	    	}
	        else{
	        	continue;
	        }
	      }
	      }catch (SQLException e) {
	      throw getException("Failed to update db entry " + serviceName + ".", e);
	    } catch (RockException e) {
	      throw getException("Failed to update db entry " + serviceName + ".", e);
	    }
	  }
  
  /**
	 * Function to parse the service_names file and populate a map with host
	 * name and its corresponding services
	 */
	protected void populateHostNameToTypeMapping() {
		if (hostToServiceMap == null) {
			hostToServiceMap = new HashMap<String, List<String>>();
		}
		final File file = new File(System.getProperty("CONF_DIR", "/eniq/sw/conf"),
				System.getProperty("service_names", "service_names"));
		try {
			if (file.exists()) {
				if (lastModifiedTime != file.lastModified()) {
					lastModifiedTime = file.lastModified();
					hostToServiceMap.clear();
				} else {
					log("Already populated hostToServiceMap: " + hostToServiceMap.toString());
					return;
				}
				final FileReader fr = new FileReader(file);
				final BufferedReader br = new BufferedReader(fr);
				try {
					String line = null;
					StringTokenizer tokenizer = null;
					while ((line = br.readLine()) != null) {
						if (!(line.contains(ServiceNames.SPECIAL_CHARECTER1)
								|| line.contains(ServiceNames.SPECIAL_CHARECTER2))) {
							tokenizer = new StringTokenizer(line, ServiceNames.DELIMITER);
							if (tokenizer.hasMoreTokens()) {
								tokenizer.nextToken(); // IP, skip it
								if (tokenizer.hasMoreTokens()) {
									final String hostName = tokenizer.nextToken();
									List<String> serviceList = hostToServiceMap.get(hostName);
									//hostToServiceMap.put(serviceList, hostName);
									if (serviceList != null) {
										if (tokenizer.hasMoreTokens()) {
											final String serviceName = tokenizer.nextToken();
											serviceList.add(serviceName);
											hostToServiceMap.put(hostName, serviceList);
										}

									} else {
										if (tokenizer.hasMoreTokens()) {
											final String serviceName = tokenizer.nextToken();
											serviceList = new ArrayList<String>();
											serviceList.add(serviceName);
											hostToServiceMap.put(hostName, serviceList);
										}
									}

								}
							}
						}
					}
				} finally {
					br.close();
				}
				log("Host to service Map populated: " + hostToServiceMap);
			} else {
				log("File: " + file.getAbsolutePath() + " does not exist.");
			}
		} catch (final Exception e) {
			log("Exception while parsing file: " + file.getAbsolutePath(), e);
		}
	}
  
  private void addService(final AtomicLong connectionId, final String username, final String password,
                          final String service_name, final RockFactory etlrep) {
    if (username == null || username.length() == 0) {
      throw getException("username cant be null/zero-length");
    }
    if (password == null || password.length() == 0) {
      throw getException("password cant be null/zero-length");
    }
    if (service_name == null || service_name.length() == 0) {
      throw getException("service_name cant be null/zero-length");
    }
    addServiceUser(connectionId, service_name, username, password, etlrep);
    if (Pattern.matches(RW_IQNAME, service_name)) {
      addDbService(connectionId, service_name, etlrep);
    }
    if (Pattern.matches(EC_NAME, service_name)) {
    	addEcService(connectionId, service_name, etlrep);
    }
  }

  private void addServiceUser(final AtomicLong nextConnectionId, final String connectionName,
                              final String username, final String password,
                              final RockFactory etlrep) {
    final Meta_databases where = new Meta_databases(etlrep);
    where.setConnection_name(connectionName);
    where.setUsername(username);
    where.setType_name(DCUSER);

    final Meta_databasesFactory mFac;
    long connIdUsed = -1;
    try {
      mFac = new Meta_databasesFactory(etlrep, where);
      final List<Meta_databases> currentEntry = mFac.get();

      if (currentEntry.isEmpty()) {
        connIdUsed = nextConnectionId.getAndIncrement();
        final Meta_databases metaDatabaseObject = new Meta_databases(etlrep);
        metaDatabaseObject.setUsername(username);
        metaDatabaseObject.setVersion_number("0");
        metaDatabaseObject.setType_name(DCUSER);
        metaDatabaseObject.setConnection_id(connIdUsed);
        metaDatabaseObject.setConnection_name(connectionName);
        metaDatabaseObject.setConnection_string("");
        metaDatabaseObject.setPassword(Base64.getEncoder().encodeToString(password.trim().getBytes()));
        metaDatabaseObject.setEncryption_flag("Y");
        metaDatabaseObject.setDecryptionRequired(false);
        metaDatabaseObject.setDescription(username + " for service " + connectionName);
        metaDatabaseObject.setDriver_name("");
        metaDatabaseObject.insertDB();
        log("Added service entry for " + connectionName);
      } 
    } catch (SQLException e) {
      throw getException("Failed to insert new entry " + connIdUsed + ":" + connectionName + ":" + username, e);
    } catch (RockException e) {
      throw getException("Failed to insert new entry " + connIdUsed + ":" + connectionName + ":" + username, e);
    }
  }

  private RockFactory getEtlrep(final String connName) {
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    final ETLCServerProperties props;
    try {
      props = new ETLCServerProperties();
    } catch (IOException e) {
      throw new TmpException("Failed to open database connection, failed to read properties", e);
    }
    try {
      return new RockFactory(
        props.getProperty(ENGINE_DB_URL),
        props.getProperty(ENGINE_DB_USERNAME),
        props.getProperty(ENGINE_DB_PASSWORD),
        props.getProperty(ENGINE_DB_DRIVERNAME), connName, false);
    } catch (SQLException e) {
      throw getException("Failed to connect to repdb:etlrep", e);
    } catch (RockException e) {
      throw getException("Failed to connect to repdb:etlrep", e);
    }
  }

  private void close(final RockFactory rockFactory) {
    if (rockFactory == null || rockFactory.getConnection() == null) {
      return;
    }
    final Connection connection = rockFactory.getConnection();
    try {
      connection.rollback();
    } catch (SQLException e) {
      log("Close failed to rollback connection changes!", e);
    }
    try {
      connection.close();
    } catch (SQLException e) {
      log("failed to close database connection", e);
    }
  }

  private void log(final String message) {
    log(message, null);
  }

  private void log(final String message, final Throwable error) {
    final StringBuilder log = new StringBuilder(message);
    if (error != null) {
      log.append(" : ").append(error.getMessage());
    }
    System.out.printf("%s\n", log.toString());
  }

  private String getArgValue(final String argName, final String[] args) {
    for (int i = 0; i < args.length; i++) {
      if (argName.equalsIgnoreCase(args[i])) {
        return getArgValue(i, args);
      }
    }
    return null;
  }

  private String getArgValue(final int argIndex, final String[] args) {
    if (argIndex + 1 < args.length && args[argIndex + 1] != null && !args[argIndex + 1].startsWith("-")) {
      return args[argIndex + 1];
    }
    return null;
  }

  private TmpException getException(final String msg) {
    return getException(msg, null);
  }

  private TmpException getException(final String msg, final Throwable cause) {
    Throwable __cause = cause;
    if (cause != null && cause instanceof SQLException) {
      SQLException sqlException = (SQLException) cause;
      if (sqlException.getNextException() != null) {
        sqlException = sqlException.getNextException();
      }
      __cause = sqlException;
    }
    if (__cause == null) {
      return new TmpException(msg);
    } else {
      return new TmpException(msg, __cause);
    }
  }


  class TmpException extends RuntimeException {
    public TmpException(final String message, final Throwable cause) {
      super(message, cause);
    }

    public TmpException(final String message) {
      super(message);
    }
  }
}
