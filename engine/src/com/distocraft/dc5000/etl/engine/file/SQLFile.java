package com.distocraft.dc5000.etl.engine.file;

import java.io.File;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_files;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * A Class that implements common file handling
 * 
 * 
 * @author Jukka Jaaheimo
 * @since JDK1.1
 */
public class SQLFile extends TransferActionBase {
	// Name of input file
	private String fileName;
	// File object of the input file
	private File file;
	// Row deliminator
	private String rowDelim;
	// Column deliminator
	private String columnDelim;
	// If Source_FILE_TYPE = FIXED , then = true
	private boolean fillWithBlanks;
	// how many rows are inserted together
	private int commitAfterNRows;
	// Batch column name
	private final String batchColumnName;
	// Database file object
	private Meta_files metaFiles;

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
	 * @param connectId
	 *          primary key for database connections
	 * @param rockFact
	 *          metadata repository connection object
	 * @param connectionPool
	 *          a pool for database connections in this collection
	 * @param trActions
	 *          object that holds transfer action information (db contents)
	 * 
	 * @author Jukka Jaaheimo
	 * @since JDK1.1
	 */
	public SQLFile(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions, final String batchColumnName) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		this.batchColumnName = batchColumnName;

		try {

			this.metaFiles = new Meta_files(rockFact, (Long) null, collectionSetId, collection.getCollection_id(),
					version.getVersion_number(), transferActionId);

			this.fileName = this.metaFiles.getFile_name();
			if (this.fileName == null) {
				throw new EngineMetaDataException(EngineConstants.NO_FILE_NAME, null, this, this.getClass().getName());
			}

			this.file = new File(this.fileName);

			final String sourceType = this.metaFiles.getFile_content_type();

			if (sourceType.equals(EngineConstants.FILE_TYPE_FIXED)) {
				this.fillWithBlanks = true;
			}

			this.rowDelim = this.metaFiles.getRow_delim();
			this.columnDelim = this.metaFiles.getColumn_delim();
			if (this.metaFiles.getCommit_after_n_rows() == null) {
				this.commitAfterNRows = 0;
			} else {
				this.commitAfterNRows = this.metaFiles.getCommit_after_n_rows().intValue();
			}
		} catch (Exception e) {
			throw new EngineMetaDataException(EngineConstants.CANNOT_READ_METADATA, new String[] { "META_FILES" }, e, this,
					this.getClass().getName());
		}

	}

	/**
	 * GET methods for member variables
	 * 
	 * 
	 */
	public String getFileName() {
		return this.fileName;
	}

	public File getFile() {
		return this.file;
	}

	public String getRowDelim() {
		if (this.rowDelim == null) {
			return "";
		}

		return this.rowDelim;
	}

	public String getColumnDelim() {
		if (this.columnDelim == null) {
			return "";
		}
		return this.columnDelim;
	}

	public boolean getFillWithBlanks() {
		return this.fillWithBlanks;
	}

	public int getCommitAfterNRows() {
		return this.commitAfterNRows;
	}

	public String getBatchColumnName() {
		return batchColumnName;
	}

}
