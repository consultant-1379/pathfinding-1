package com.distocraft.dc5000.etl.engine.structure;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EMail;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.RemoveDataException;
import com.distocraft.dc5000.etl.engine.system.SetListener;
import com.distocraft.dc5000.etl.engine.system.StatusEvent;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

public class TransferActionBase implements ITransferAction {

	// metadata version
	private Meta_versions version;

	// primary key for collection set
	private Long collectionSetId;

	// primary key for collection
	private Meta_collections collection;

	// primary key for transfer action
	private Long transferActionId;

	// name of the transfer action
	private String transferActionName;

	// primary key for transfer batch
	private Long transferBatchId;

	// primary key for database connections
	private Long connectionId;

	// metadata repository connection object
	private RockFactory rockFact;

	// object that holds transfer action information (db contents)
	private Meta_transfer_actions trActions;

	// Collection set
	private Meta_collection_sets collSet;

	private final Logger log;

	// Set listener. It can be accessed through
	// The default non-functional set listener is temporarily replaced during the
	// method call execute(SetListener setListener).
	protected SetListener setListener = SetListener.NULL;

	/**
	 * Empty protected constructor
	 * 
	 */
	protected TransferActionBase() {
		this.log = Logger.getLogger("TransferActionBase");
	}

	/**
	 * Constructor
	 * 
	 * @param versionNumber
	 *          metadata version
	 * @param collectionSetId
	 *          primary key for collection set
	 * @param collectionId
	 *          primary key for collection
	 * @param transferActionId
	 *          primary key for transfer action
	 * @param transferBatchId
	 *          primary key for transfer batch
	 * @param connectionId
	 *          primary key for database connections
	 * @param rockFact
	 *          metadata repository connection object
	 * @param trActions
	 *          object that holds transfer action information (db contents)
	 * 
	 * @author Jukka Jaaheimo
	 * @since JDK1.1
	 */
	public TransferActionBase(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectionId, final RockFactory rockFact,
			final Meta_transfer_actions trActions) throws EngineMetaDataException {

		this.version = version;
		this.collectionSetId = collectionSetId;
		this.collection = collection;
		this.transferActionId = transferActionId;
		this.transferBatchId = transferBatchId;
		this.connectionId = connectionId;
		this.rockFact = rockFact;
		this.trActions = trActions;
		this.transferActionName = trActions.getTransfer_action_name();

		try {
			// Get collection set name
			final Meta_collection_sets whereCollSet = createMetaCollectionSets(rockFact);
			whereCollSet.setEnabled_flag("Y");
			whereCollSet.setCollection_set_id(collectionSetId);
			this.collSet = createMetaCollectionsSets(rockFact, whereCollSet);
		} catch (Exception e) {
			throw new EngineMetaDataException("Cannot resolve set name",e,"TransferActionBase.constructor");
		}

		this.log = Logger.getLogger("etl." + this.collSet.getCollection_set_name() + "." + collection.getSettype() + "."
				+ collection.getCollection_name() + "." + this.transferActionName + ".Engine");

	}

	/**
	 * extracted out to help get under test
	 * 
	 * @param rockFact
	 * @param whereCollSet
	 * @return
	 * @throws SQLException
	 * @throws RockException
	 */
	private Meta_collection_sets createMetaCollectionsSets(final RockFactory rockFact,
			final Meta_collection_sets whereCollSet) throws SQLException, RockException {
		return new Meta_collection_sets(rockFact, whereCollSet);
	}

	/**
	 * extracted out to help get under test
	 * 
	 * @param rockFact
	 * @return
	 */
	protected Meta_collection_sets createMetaCollectionSets(final RockFactory rockFact) {
		return new Meta_collection_sets(rockFact);
	}

	/**
	 * Just to implement the method, the real operation is implemented through
	 * inheritance
	 */
	public void execute() throws Exception { // NOPMD

	}

	/**
	 * Sets the member variable TransferActionBase.setListener before calling the
	 * execute() method.
	 * 
	 * The classes that inherit and override the execute() method can use
	 * TransferActionBase.setListener to report their progress.
	 * 
	 * @param setListener
	 */
	public final void execute(final SetListener setListener) throws Exception { // NOPMD
		try {
			this.setListener = setListener;
			execute();
		} finally {
			this.setListener = SetListener.NULL;
		}
	}

	/**
	 * Just to implement the method, the real operation is implemented through
	 * inheritance
	 * 
	 */
	public void removeDataFromTarget() throws EngineMetaDataException, RemoveDataException {
	}

	/**
	 * A method that is overwritten in SQLFkFactory
	 * 
	 */
	public int executeFkCheck() throws EngineException {
		return -1;
	}

	/**
	 * A method that is overwritten in SQLColConstraint
	 * 
	 */
	public int executeColConstCheck() throws EngineException {
		return -1;
	}

	/**
   * 
   */
	public boolean isGateClosed() {
		return false;
	}

	/**
	 * Writes debug information into the database
	 * 
	 * @param debugText
	 *          text to write into db.
	 */
	public void writeDebug(String debugText) throws EngineMetaDataException {
		try {
			if (debugText == null) {
				debugText = EngineConstants.NO_DEBUG_TEXT;
			}

			log.fine("DEBUG: " + debugText);

		} catch (Exception e) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_WRITE_DEBUG, e, this.getClass().getName());
		}
	}

	/**
	 * Protected method to write error information into the database
	 * 
	 * @param errorText
	 *          text to write into db.
	 * @param errType
	 *          the type of the error.
	 */
	protected void writeErrorProt(String errorText, final String methodName, final String errType)
			throws EngineMetaDataException {
		try {
			if (errorText == null) {
				errorText = EngineConstants.NO_ERROR_TEXT;
			}

			log.severe("ERROR in action: " + methodName + " " + errType + " " + errorText);

		} catch (Exception e) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_WRITE_ERROR, e, this.getClass().getName());
		}

		if ((version.getMail_server() != null) && (version.getMail_server_port() != null)) {
			sendMail(errType, errorText);
		}
	}

	private void sendMail(final String errType, final String errorText) throws EngineMetaDataException {
		final EMail em = new EMail();

		// Your own mailserver
		em.setMailServer("" + this.version.getMail_server() + "");
		em.setPort(this.version.getMail_server_port().intValue());

		em.setSenderName(EngineConstants.ERR_MAIL_SENDER);

		// Something about sender
		em.setDomain("" + this.version.getMail_server() + "");
		em.setSenderAddress(EngineConstants.ERR_MAIL_SENDER + "@" + this.version.getMail_server() + "");

		// Recipient
		final List<String> v = new Vector<String>();
		if (errType.equals(EngineConstants.ERR_TYPE_DEFINITION)) {
			v.add("" + this.collection.getMail_bug_addr());
		} else if (errType.equals(EngineConstants.ERR_TYPE_EXECUTION)) {
			v.add("" + this.collection.getMail_bug_addr());
		} else if (errType.equals(EngineConstants.ERR_TYPE_VALIDATION)) {
			v.add("" + this.collection.getMail_fail_addr());
		} else if (errType.equals(EngineConstants.ERR_TYPE_SQL)) {
			v.add("" + this.collection.getMail_bug_addr());
		} else if (errType.equals(EngineConstants.ERR_TYPE_WARNING)) {
			v.add("" + this.collection.getMail_error_addr());
		} else if (errType.equals(EngineConstants.ERR_TYPE_SYSTEM)) {
			v.add("" + this.collection.getMail_bug_addr());
		}

		em.setRecipients(v);

		String errStr = "COLLECTION:       " + this.collection.getCollection_name() + "\n";
		errStr += "TRANSFER ACTION:  " + this.transferActionName + "\n";
		errStr += "ERROR MESSAGE:   " + "\n";
		errStr += errorText;
		em.setSubject(EngineConstants.ERR_MAIL_SUBJECT + errType);
		em.setMessage(errStr);

		if (!em.sendMail()) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_SEND_MAIL, new String[] { this.version.getMail_server(),
					this.version.getMail_server_port().toString() }, null, this.getClass().getName());
		}
	}

	/**
	 * Writes error information into the database
	 * 
	 * @param errorText
	 *          text to write into db.
	 * @param errType
	 *          the type of the error.
	 */
	public void writeError(final String errorText, final String errType) throws EngineMetaDataException {
		writeErrorProt(errorText, null, errType);
	}

	/**
	 * Writes error information into the database
	 * 
	 * @param errorText
	 *          text to write into db.
	 * @param methodName
	 *          method that caused the error.
	 * @param errType
	 *          the type of the error.
	 */
	public void writeError(final String errorText, final String methodName, final String errType)
			throws EngineMetaDataException {
		writeErrorProt(errorText, methodName, errType);
	}

	public String getVersionNumber() {
		return this.version.getVersion_number();
	}

	public Long getCollectionSetId() {
		return this.collectionSetId;
	}

	public Long getCollectionId() {
		return this.collection.getCollection_id();
	}

	public Long getTransferActionId() {
		return this.transferActionId;
	}

	public String getTransferActionName() {
		return this.transferActionName;
	}

	public Long getTransferBatchId() {
		return this.transferBatchId;
	}

	public Long getConnectionId() {
		return this.connectionId;
	}

	public RockFactory getRockFact() {
		return this.rockFact;
	}

	public Meta_transfer_actions getTrActions() {
		return this.trActions;
	}

	/**
	 * Creates a status event, and sends it to this action's listener.
	 */
	protected final void sendEventToListener(final String message) {
		final String dispatcher = this.transferActionName;
		final Date currentTime = new Date(System.currentTimeMillis());

		final StatusEvent statusEvent = new StatusEvent(dispatcher, currentTime, message);
		this.setListener.addStatusEvent(statusEvent);
	}

	/**
	 * Add properties from str to existing properties object.
	 */
	public static Properties stringToProperties(final String str, final Properties prop) throws EngineMetaDataException {
		if (str != null && str.length() > 0) {
			try {
				final ByteArrayInputStream bais = new ByteArrayInputStream(str.getBytes());
				prop.load(bais);
				bais.close();
			} catch (Exception e) {
				throw new EngineMetaDataException("Error parsing properties string", e, "stringToProperty");
			}
		}
		return prop;
	}
	
	/**
	 * Tries to create Properties object from a String.
	 */
	public static Properties stringToProperties(final String str) throws EngineMetaDataException {
		return stringToProperties(str,new Properties());
	}

	/**
	 * Tries to create String from Properties object.
	 */
	public static String propertiesToString(final Properties props) throws EngineMetaDataException {

		try {
			if (props != null) {
				final ByteArrayOutputStream baos = new ByteArrayOutputStream();
				props.store(baos, "");

				final String ret = baos.toString();
				baos.close();
				
				return ret;

			}
		} catch (final IOException ioe) {
			throw new EngineMetaDataException("Error converting Properties to string", ioe, "propertiesToString");
		}

		return "";

	}

	/**
	 * Resolves String with ${var} with system properties. If corresponding system
	 * property is not found ${var} is just removed.
	 */
	public static String resolveSysProperties(final String string) {

		String directory = string;

		if (directory.indexOf("${") >= 0) {
			final int start = directory.indexOf("${");
			final int end = directory.indexOf("}", start);

			if (end >= 0) {
				final String variable = directory.substring(start + 2, end);
				final String val = System.getProperty(variable);
				final String result = directory.substring(0, start) + val + directory.substring(end + 1);
				directory = result;
			}
		}

		return directory;

	}

}