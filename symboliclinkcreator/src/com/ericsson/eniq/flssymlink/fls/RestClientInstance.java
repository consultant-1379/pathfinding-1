package com.ericsson.eniq.flssymlink.fls;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.ericsson.eniq.common.RemoteExecutor;
import com.ericsson.eniq.flssymlink.StaticProperties;
import com.ericsson.eniq.repository.DBUsersGet;
import com.jcraft.jsch.JSchException;

/**
 * RestClientInstance creates a Client instance and open the ENM session for
 * getting FLS Responses
 * 
 * @author xdhanac
 * 
 */
public class RestClientInstance {

	private Client client;
	private String HOST = null;
	private Boolean sessionCheck = false;
	private String USERNAME = null;
	private String PASSWORD = null;
	Logger log = null;
	RockFactory dwhdb;
	private HttpClientConnectionManager clientConnectionManager;
	private static int TIMEOUT = 20000; // Value in milliseconds for Timeout
	private FlsTask task;

	public RestClientInstance(FlsTask task) {
		this.task = task;
	}

	public boolean sessionCheck() {
		return sessionCheck;
	}

	/*
	 * This function makes restClientInstance as null
	 */
	void refreshInstance() {
		this.closeSession();
		if (client != null) {
			// client.close();
			client = null;
		}

	}

	
// For EQEV-66104	
	public String getSecurityProtocol() {
		String securityProtocol = null;
		Pattern p = Pattern.compile(".*sslEnabledProtocols=(.*)");

		try {
			BufferedReader bufReader = null;
			File serverFile = new File(
					"/eniq/sw/runtime/tomcat/conf/server.xml");
			bufReader = new BufferedReader(new FileReader(serverFile));
			String line = bufReader.readLine();
			while (line != null) {
				if (line.contains("sslEnabledProtocols")) {
					Matcher m = p.matcher(line);
					if (m.find()) {
						
						securityProtocol = m.group(1).split("\\s")[0];
						securityProtocol = securityProtocol.substring(1,
								securityProtocol.length() - 1);
						break;
					}
				}
				line = bufReader.readLine();
			}
			bufReader.close();
		}

		catch (IOException e) {
			log.info("Exception occured in getting the security protocol version:"
					+ e);
		}
		

		return securityProtocol;
	}

	/**
	 * This function creates the client instance and returns the Client
	 * instance.
	 * 
	 * @param cache
	 *            contains the details of ENMServerDetails class object.
	 * @param log
	 *            contains the Logger class instance.
	 * @return Returns the instance of Client registered with session cookies.
	 */
	public Client getClient(final ENMServerDetails cache, final Logger log)
			throws IOException {

		if (client == null) {
			try {
				this.log = log;
				// log.info("debug=Inside getting rest client instance");

				HOST = "https://" + cache.getHost();
				USERNAME = cache.getUsername();
				final String Password_decrypt = cache.getPassword();

				// byte[] dec = new
				// BASE64Decoder().decodeBuffer(Password_decrypt);
				byte[] dec = Base64.getMimeDecoder().decode(Password_decrypt);
				final String PASSWORD1 = new String(dec, "UTF8");
				PASSWORD = PASSWORD1.trim();
				log.fine("host username:" + HOST + " " + USERNAME);

				// To validate server CA certificate
				// In order to import server CA certificate
				// keytool -import -file cacert.pem -alias ENM -keystore
				// truststore.ts -storepass secret
				// And give the location of the keystore

				String keyStorePassValue = getValue("keyStorePassValue").trim();
				String securityProtocol = getSecurityProtocol();

				final SslConfigurator sslConfig = SslConfigurator
						.newInstance()
						.trustStoreFile(
								StaticProperties
										.getProperty("TRUSTSTORE_DIR",
												"/eniq/sw/runtime/jdk/jre/lib/security/truststore.ts"))
						.trustStorePassword(keyStorePassValue)
						.securityProtocol(securityProtocol);

				log.fine("we are using "+securityProtocol+" securityProtocol.");
				
				final SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(
						sslConfig.createSSLContext());

				final Registry<ConnectionSocketFactory> registry = RegistryBuilder
						.<ConnectionSocketFactory> create()
						.register("http",
								PlainConnectionSocketFactory.getSocketFactory())
						.register("https", sslSocketFactory).build();

				// Pooling HTTP Client Connection manager.
				// connections will be re-used from the pool
				// also can be used to enable
				// concurrent connections to the server and also
				// to keep a check on the number of connections
				//

				clientConnectionManager = new BasicHttpClientConnectionManager(
						registry);
				// clientConnectionManager.setMaxTotal(50);
				// clientConnectionManager.setDefaultMaxPerRoute(20);

				final ClientConfig clientConfig = new ClientConfig();
				clientConfig.property(ClientProperties.READ_TIMEOUT, TIMEOUT);
				clientConfig
						.property(ClientProperties.CONNECT_TIMEOUT, TIMEOUT);

				clientConfig.property(
						ApacheClientProperties.CONNECTION_MANAGER,
						clientConnectionManager);
				clientConfig.connectorProvider(new ApacheConnectorProvider());

				client = ClientBuilder.newBuilder().withConfig(clientConfig)
						.build();

			} catch (Exception e) {
				log.warning("Exception while creating client instance :::   "
						+ e);
			}

		}
		if (sessionCheck == false && client != null) {
			getSession(0);
		}
		if (client == null) {
			log.warning("Client object initialisation failed.");
		} else {
			log.finest("client instance near return " + client);
		}

		return client;
	}

	public boolean getSessionCheck() {
		return sessionCheck;
	}

	/**
	 * This function opens the ENM session for sending the REST request.
	 * 
	 * @param cache
	 *            contains the details of ENMServerDetails class object.
	 * @param log
	 *            contains the Logger class instance.
	 * @return Returns the instance of Client registered with session cookies.
	 */
	void getSession(int c) {
		// log.info("c is = "+c);
		if (c == 3) {
			log.warning("Session creation request sent to server three times but failed to create session");
			return;
		}

		try {
			log.info("login URL :  "
					+ client.target(HOST).path("login"));

			final WebTarget target = client.target(HOST).path("login")
					.queryParam("IDToken1", USERNAME)
					.queryParam("IDToken2", PASSWORD);

			final Response response = target.request(MediaType.WILDCARD_TYPE)
					.post(Entity.json(""));

			try {
				task.setLastDate(new Date());
				log.info("Login Response Status :" + response.getStatus()
						+ " \n Login Response Status Information :"+ response.getStatusInfo());
				log.finest("Login Response Headers :" + response.getHeaders());
				// if the response is client error 401 or any exception that can
				// be
				// successful by retrying,
				// send request for login again
				// and then send the request again for 2 more times
				if (response.getStatus() == 302) {
					sessionCheck = true;
					log.info("Session established...Response code :"
							+ response.getStatus());
					return;
				} else {
					log.info("Failed to create session hence will re-try again.");
				}
			} finally { // closing response objects to release the consumed
						// resources
				// log.info("debug=closing response");
				if (response != null)
					response.close();
			}
		} catch (Exception e) {
			if (e.getCause() instanceof ConnectTimeoutException) {
				log.warning("TIMEOUT Exception while logging in : "
						+ e.getMessage());
			} else {
				log.warning("Exception while logging in : " + e.getMessage());
			}
		}
		getSession(c + 1);

	}

	public String getValue(String command) {
		String output = "";
		try {

			String systemCommandString = "";
			final String user = "dcuser";
			final String service_name = "engine";
			List<Meta_databases> mdList = DBUsersGet.getMetaDatabases(user,
					service_name);
			if (mdList.isEmpty()) {
				mdList = DBUsersGet.getMetaDatabases(user, service_name);
				if (mdList.isEmpty()) {
					throw new Exception("Could not find an entry for " + user
							+ ":" + service_name
							+ " in engine! (was is added?)");
				}
			}
			final String password = mdList.get(0).getPassword();
			systemCommandString = ". /eniq/home/dcuser; . ~/.profile; "
					+ "cat /eniq/sw/conf/niq.ini |grep -i " + command;
			output = RemoteExecutor.executeComand(user, password, service_name,
					systemCommandString);

			if (!output.contains("\n")) {
				output = output.substring(output.indexOf("=") + 1);
			} else {
				String[] outputArray = output.split("\n");
				boolean encryptionflag = false;
				for (String str : outputArray) {
					String key = str.substring(0, str.indexOf("=")).trim();
					String value = str.substring(str.indexOf("=") + 1).trim();
					if (key.contains("_Encrypted") && value.equalsIgnoreCase("Y")) {
						encryptionflag = true;
					} else if (key.equalsIgnoreCase(command)) {
						output = value;
					}
				}

				if (encryptionflag) {
					output = new String(Base64.getDecoder().decode(output.trim()));
				}
			}
			
			return output.trim();

		} catch (final JSchException e) {
			log.warning("JschException:" + e);
		} catch (final Exception e) {
			log.warning("Exception:" + e);
		}
		return output.trim();
	}

	public void closeSession() {

		if (sessionCheck == true) {
			log.info("Closing the session.");
			log.fine("client instance" + client);
			try {
				int status_code;
				String response = null;
				final Response response3 = client.target(HOST).path("logout")
						.request("application/json").get();
				try {
					status_code = response3.getStatus();
					response = response3.toString();
					log.finest("log out URL : \n  "
							+ client.target(HOST).path("logout"));
					log.finest("log out response : " + response3
							+ " \n Response status :" + response3.getStatus()
							+ " \n Response Headers :" + response3.getHeaders());
				} finally {
					if (response3 != null)
						response3.close();
				}
				if (status_code == 200) {
					sessionCheck = false;
					clientConnectionManager.shutdown();
					client.close();
					if (client != null)
						client = null;
					log.info("Successfully logged out");
				} else {
					sessionCheck = true;
					log.warning("Error in closing the session..session exists and logout response from enm server:"
							+ response);
				}
			} catch (Exception e) {
				log.info("Exception while logging out : " + e);
			}

		} else {
			if (client != null) {
				clientConnectionManager.shutdown();
				client = null;
			}
		}
		task.setLastDate(new Date());

	}

	/**
	 * This function will logout from the current session and login again if
	 * there is no response for a get query for more than 30 seconds.
	 * 
	 * @param client
	 *            instance of the Client object
	 */

	void session() {
		log.info("Closing the session as no response recieved for get query for 30 seconds,hence will skip query for remaining nodes");
		closeSession();
	}

}
