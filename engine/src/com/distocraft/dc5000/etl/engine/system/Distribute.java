package com.distocraft.dc5000.etl.engine.system;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * FileDistributor action
 */
public class Distribute extends TransferActionBase {

	private final Meta_transfer_actions actions;
	
	private String inDir = "";
  private long minFileAge = 0;
	private String pattern;
	private String type;
	private String method;
	private int bufferSize;
	
	protected static final String DISTRIB_IGNORE = "distributor.ignore_error_sets";

	private static final Logger log = Logger.getLogger("Distribute");

	/**
	 * Empty protected constructor
	 */
	protected Distribute() {
		this.actions = null;
	}

	/**
	 * Constructor
	 * 
	 * This action distributes (move or copy) files locally from directory to
	 * another
	 * 
	 * Distributor uses regular expression to filter files. Only files matching to
	 * a pattern are distributet. Pattern is retrieved from the filename.
	 * 
	 * Tags are read from ACTION_CONTENT column as a Propertis object.
	 * 
	 * Tag: Description:
	 * 
	 * indir Where files are read defaultOutDir If nothing matches directory where
	 * files are moved/copied method Is files copied or moved (copy , move)
	 * minFileAge minumun age of files to be distributed. bufferSize Number of
	 * characters read to the buffer where matching is done. pattern Pattern to be
	 * found outDir If pattern is found where to move/copy (see method) type
	 * Defines where the buffer is read (filename, content)
	 * 
	 * @param version
	 *          metadata version
	 * @param collectionSetId
	 *          primary key for collection set
	 * @param collection
	 *          the Meta_collections
	 * @param transferActionId
	 *          primary key for transfer action
	 * @param transferBatchId
	 *          primary key for transfer batch
	 * @param connectId
	 *          primary key for database connections
	 * @param rockFact
	 *          metadata repository connection object
	 * @param trActions
	 *          object that holds transfer action information (db contents)
	 * 
	 */
	public Distribute(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final Meta_transfer_actions trActions) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, trActions);

		this.actions = trActions;

	}

	public void execute() throws EngineException {
		final List<String> meastypesMoved = new Vector<String>();
		boolean result = false;
		boolean found = false;

		try {

			final Properties properties = TransferActionBase.stringToProperties(this.actions.getAction_contents());
			
			this.method = properties.getProperty("method");

			this.inDir = updateDirs(properties.getProperty("inDir"));

			final String defaultOutDir = updateDirs(properties.getProperty("defaultOutDir"));

			final String minFileAge = properties.getProperty("minFileAge");
			this.minFileAge = Long.parseLong(minFileAge);
			this.pattern = properties.getProperty("pattern");
			this.type = properties.getProperty("type");
			final String bufferSize = properties.getProperty("bufferSize");
			this.bufferSize = Integer.parseInt(bufferSize);

			final String outDir = updateDirs(properties.getProperty("outDir"));
			
			/* Read all the file from in directory */
			final List<File> sourceFiles = createFileList();
			/* Loop all files */
			for (File file : sourceFiles) {

				/* By default we did not found what we where looking */
				found = false;
				try {

					/* Get buffer */
					final String buffer = getBuffer(file);

					/* Match the pattern to buffer */
					final Pattern pattern = Pattern.compile(this.pattern);
					final Matcher matcher = pattern.matcher(buffer);
					result = matcher.find();

					if (result) {
						/* Pattern found, move/copy to out directory */
						distributeFile(file, outDir);
						log.finest("Pattern(" + this.pattern + ") found in " + file.getName());
						log.finer("Moving to (" + outDir);
						meastypesMoved.add(outDir);
						found = true;

					}

					/* No matches found, move to default */
					if (!found) {
						/*
						 * Pattern not found in any of candidates, move/copy files to
						 * default out directory
						 */
						distributeFile(file, defaultOutDir);
						log.finer("No Pattern(s) found in " + file.getName());
					}

				} catch (Exception e) {
					log.log(Level.WARNING, "Error while distributing file " + file.getAbsolutePath(), e);
				}

			}

		} catch (Exception e) {
			throw new EngineException("Exception in Distributor", new String[] { "" }, e, this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_SYSTEM);

		}

	}

	/**
	 * Move or copy to defined directory
	 * 
	 * @param file
	 *          file to copy/move
	 * @param directory
	 *          directory to copy/move file to
	 */
	private void distributeFile(final File file, final String directory) throws FileNotFoundException, IOException {
		if (this.method.equalsIgnoreCase("move")) {
			moveFile(file, directory);
		}

		if (this.method.equalsIgnoreCase("copy")) {
			copyFile(file, directory);
		}
		
	}

	private void moveFile(final File file, final String directory) throws FileNotFoundException, IOException {
		final boolean ok = file.renameTo(new File(directory + file.getName()));

		if (!ok) {
			copyFile(file, directory);
			file.delete();
		}
	}

	private void copyFile(final File inFile, final String directory) throws FileNotFoundException, IOException {

		final File outFile = new File(directory + inFile.getName());

		final FileReader fr = new FileReader(inFile);
		final BufferedReader br = new BufferedReader(fr);
		final FileWriter fw = new FileWriter(outFile);
		final BufferedWriter bw = new BufferedWriter(fw);

		int c;
		while ((c = br.read()) != -1) {
			bw.write(c);
		}

		bw.close();
		fw.close();
		br.close();
		fr.close();

	}

	protected boolean errorNonExistingInDir(final String actioName) {
		final String propValue = StaticProperties.getProperty(DISTRIB_IGNORE, "");
		final StringTokenizer st = new StringTokenizer(propValue, ";");
		while (st.hasMoreTokens()) {
			final String token = st.nextToken();
			if (token.equals(actioName)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Creates a list of files.
	 * 
	 * @return a List of SourceFiles.
	 * @throws Exception
	 */
	protected List<File> createFileList() throws FileNotFoundException {
		final File inDir = new File(this.inDir);
		final List<File> vec = new ArrayList<File>();
		if (!inDir.exists() || !inDir.canRead()) {
			final String taName = actions.getTransfer_action_name();
			if (errorNonExistingInDir(taName)) {
				throw new FileNotFoundException("inDir doesn't exist or cannot be read.");
			} else {
				return new ArrayList<File>(0);
			}
		}
		final File files[] = inDir.listFiles();
		for (File file : files) {
			if (file.isFile() && file.canRead()) {
				if (isOldEnough(file)) {
					vec.add(file);
				}
			}
		}
		return vec;
	}

	/**
	 * Determines weather this SourceFile is old enough
	 * 
	 * @return true if this file is old enough false otherwise
	 */
	private boolean isOldEnough(final File file) {
		try {

			if ((System.currentTimeMillis() - file.lastModified()) >= (this.minFileAge * 60000)) {
				return true;
			}

		} catch (Exception e) {
			log.fine("File modification time comparison failed.");
		}

		return false;
	}

	private String getBuffer(final File file) throws FileNotFoundException, IOException {
		String buffer = "";

		if (file.isFile() && file.canRead() && file.exists() && file.length() > 0) {

			if (this.type.equalsIgnoreCase("filename")) {
				buffer = file.getName();
			}

			if (this.type.equalsIgnoreCase("content")) {

				final char[] buff = new char[this.bufferSize];

				final FileReader sourceReader = new FileReader(file);
				final BufferedReader br = new BufferedReader(sourceReader);

				br.read(buff);

				br.close();
				sourceReader.close();

				buffer = String.copyValueOf(buff);

			}

		}

		return buffer;

	}

	private String updateDirs(final String directory) {

		String ret = directory;
		
		if (directory.indexOf("${") >= 0) {
			final int start = directory.indexOf("${");
			final int end = directory.indexOf("}", start);

			if (end >= 0) {
				final String variable = directory.substring(start + 2, end);
				final String val = System.getProperty(variable);
				final String result = directory.substring(0, start) + val + directory.substring(end + 1);
				ret = result;
			}
		}

		return ret;
	}

}
