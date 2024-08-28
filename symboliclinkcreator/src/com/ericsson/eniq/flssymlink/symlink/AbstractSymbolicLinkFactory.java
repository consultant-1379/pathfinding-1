package com.ericsson.eniq.flssymlink.symlink;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.*;

import com.ericsson.eniq.flssymlink.StaticProperties;
import com.ericsson.eniq.flssymlink.fls.Main;

public abstract class AbstractSymbolicLinkFactory {

	/**
	 * Map<NeType, @SymbolicLinkSubDirConfiguration> Holds the @SymbolicLinkSubDirConfiguration
	 * for each NeType fetched from the @SymLinkDataSAXParser
	 */
	static ConcurrentHashMap<String,List<Map<String,SymbolicLinkSubDirConfiguration>>> symbolicLinkSubDirConfigurations;
	
	/**
	 * The @SymbolicLinkWriter used for actual symbolic link creation
	 */
	private SymbolicLinkWriter symbolicLinkWriter = null;

	/**
	 * Specific to this factory and assigned a value only once
	 */
	public boolean isFactoryEnabled;
	
	Logger log;

	private String eniqPMDataDirectory;

	private String eniqImportDataDirectory;
	
	private String ossId;
	
	protected static List<String> subNetworkExclusionList ;
	
	public AbstractSymbolicLinkFactory(String ossId) {
		this.ossId = ossId;
	}

	/**
	 * Subclasses can implement this to do any specific initialization they want
	 * to do
	 */
	abstract void finalizeConfiguration();
	
	/**
	 * Method to create required sub directories under the target mount point
	 * This is done only once during the factory initialization.
	 * 
	 * @param dirName
	 *            - NeType directory name
	 * @param subDirNames
	 *            - List of sub directory names to be created under dirName
	 */
	void createDirectories(final String dirName, final List<String> subDirNames) {
		for (final String subDirName : subDirNames) {
			final String targetSubDirName = eniqPMDataDirectory + dirName
					+ subDirName;
			makeDirectory(targetSubDirName);
		}
	}

	/**
	 * This method is called once during the factory initialization. It goes
	 * through all the @SymbolicLinkSubDirConfiguration and creates the sub
	 * directories as required.
	 */
	abstract void createRequiredSubDirectories();

	public void createSymbolicLink(final String nodeType, final String fileName, final Date startDate, final String neType) {
		try{
			final Path filePath=Paths.get(fileName);
			final String fileNameToBeChecked = filePath.getFileName().toString();
			log.fine("FileName extracted is "+fileNameToBeChecked+" from "+fileName);
			log.fine("createSymbolicLink method is initiated");
			if (isFactoryEnabled()) {
				final List<Map<String,SymbolicLinkSubDirConfiguration>> mapList = getSymbolicLinkSubDirConfiguration(nodeType, log);
				if (mapList != null) {
					for( Map<String,SymbolicLinkSubDirConfiguration> map : mapList ) {
		        		for (SymbolicLinkSubDirConfiguration symbolicLinkSubDirConfiguration : map.values()) {
		        			if(symbolicLinkSubDirConfiguration!=null){
		        				final String fileFilter=symbolicLinkSubDirConfiguration.getFileFilter();
		        				log.fine("Node Type: "+nodeType);	
		        				if(!fileFilter.isEmpty()) //EQEV-60726
		        				{
		        					log.fine("File Filter is applied in the eniq.xml for the node type "+nodeType);
		        					log.fine("File to be lookedup for pattern matching: "+fileNameToBeChecked);
			        				Pattern filterPattern=Pattern.compile(fileFilter);
			        				Matcher filterMatcher=filterPattern.matcher(fileNameToBeChecked);
			        				
		        					if(!filterMatcher.matches())
			        				{
			        					continue;
			        				}
		        					else
		        					{
		        					    log.fine("Matched the pattern: "+fileFilter);
		        					}
		        				}
		        				else
		        				{
		        					log.fine("File Filter is not applied in the eniq.xml for the node type "+nodeType);
		        				}
		        				
		        				final String linkNodeTypeDir = symbolicLinkSubDirConfiguration.getNodeTypeDir();
		        				log.fine("NodeType Directory where symlink will be created: "+linkNodeTypeDir);
		    					final String linkSubDir = getLinkSubDir(symbolicLinkSubDirConfiguration, nodeType, linkNodeTypeDir);
		    					final EniqSymbolicLink eniqSymbolicLink = new EniqSymbolicLink(
		    							eniqPMDataDirectory, linkNodeTypeDir, linkSubDir,
		    							eniqImportDataDirectory, fileName, log, subNetworkExclusionList.contains(nodeType));

		    					writeSymbolicLink(eniqSymbolicLink, startDate, neType, fileName);
		    				}
		    				else{
		    					log.warning("indir not found for nodeType: "+nodeType + ". Symlink will not be creating for: " + fileName );
		    				}
		        		}
					}
				} else {
					log.warning("indir not found for nodeType: "+nodeType + ". Symlink will not be creating for: " + fileName );
				}
			}
		}
		catch(Exception e){
			log.log(Level.WARNING,"will not be creating for symlink: " + fileName + ". Exception at createSymbolicLink method ",e);
		}
	}


	/**
	 * This method returns the appropriate sub directory for creating the
	 * symbolic link. If the current directory has reached its maximum capacity,
	 * it checks for the next directories until it finds one. It throws
	 * 
	 * @SymbolicLinkCreationFailedException if all sub directories are full.
	 * 
	 * @param symbolicLinkSubDirConfiguration
	 *            for the NeType
	 * @param nodeName
	 *            for which Symbolic Link has to be created
	 * 
	 * @return a sub directory that can be used for symbolic link creation
	 * 
	 * @throws SymbolicLinkCreationFailedException
	 *             if all sub directories are full
	 */
	abstract String getLinkSubDir(
			SymbolicLinkSubDirConfiguration symbolicLinkSubDirConfiguration,
			String nodeName, String nodeTypeDir);

	/**
	 * @return - The map where NeType is the key and corresponding
	 * @SymbolicLinkSubDirConfiguration is the value
	 */
	protected ConcurrentHashMap<String, List<Map<String,SymbolicLinkSubDirConfiguration>>> getSymbolicLinkConfiguration() {
		return DirectoryConfigurationFileParser.getInstance(log)
				.getSymbolicLinkSubDirConfigurations();
	}

	/**
	 * @return the folder underneath which the symbolic links will be written
	 */
	final String getSymbolicLinkParentFolder() {
		return eniqPMDataDirectory;
	}

	
	/**
	 * @param nodeType
	 * @param log
	 * @return
	 */
	abstract List<Map<String, SymbolicLinkSubDirConfiguration>> getSymbolicLinkSubDirConfiguration(final String nodeType, Logger log);

	/**
	 * Method to be called first before calling any other methods in this
	 * factory. Almost like a constructor, but required to silence PMD.
	 * 
	 * Populates the @SymbolicLinkSubDirConfiguration for all NeType from
	 * 
	 * @SymLinkDataSAXParser. Creates the sub directories as required.
	 */
	public void initialiseFactory(Logger log) {
		this.log = log;
		isFactoryEnabled = isSubDirConfigurationAvailable() && Main.isFlsEnabled();
		if (isFactoryEnabled) {
			setDirectories();
			finalizeConfiguration();
			createRequiredSubDirectories();
		}
		else{
			log.info("Failed to Initialize!!");
		}
	}

	/**
	 * Loads the map symbolicLinkSubDirConfigurations
	 */
	private void initialiseSubDirectoryConfiguration() {
		symbolicLinkSubDirConfigurations = getSymbolicLinkConfiguration();
	}

	private void setDirectories() {
		eniqPMDataDirectory = StaticProperties.getProperty("PMDATA_PATH", "/eniq/data/pmdata" );
		if (!eniqPMDataDirectory.endsWith(File.separator)) {
			eniqPMDataDirectory += File.separator;
		}
		eniqImportDataDirectory = StaticProperties.getProperty("IMPORTDATA_PATH", "/eniq/data/importdata" );
		if (!eniqImportDataDirectory.endsWith(File.separator)) {
			eniqImportDataDirectory += File.separator;
		}
		eniqPMDataDirectory = eniqPMDataDirectory + ossId + File.separator;
		eniqImportDataDirectory = eniqImportDataDirectory + ossId + File.separator;
		log.info("symbolicLinkParentFolder  :::  "  + eniqPMDataDirectory);
		log.info("segmentOnEniq  :::  "  + eniqImportDataDirectory);
	}

	/**
	 * @return true if the factory is enabled
	 */
	boolean isFactoryEnabled() {
		return isFactoryEnabled;
	}

	/**
	 * @return true if the eniq.xml file is existing
	 */
	boolean isSubDirConfigurationAvailable() {
		initialiseSubDirectoryConfiguration();
		if (symbolicLinkSubDirConfigurations.isEmpty()) {
			// The xml is not present or blank
			log.info("eniq.xml file is either not present or blank!");
			return false;
		}
		// the map is not empty so the values are present
		return true;
	}

	/**
	 * Creates the directory named by dirName, including any necessary but
	 * nonexistent parent directories.
	 * 
	 * @param dirName
	 *            - Absolute path of the directory to be created
	 */
	private void makeDirectory(final String dirName) {
		try {
			final File result = new File(dirName);
			if (!result.exists()) {
				result.mkdirs();
				log.info("AbstractSymbolicLinkFactory : created directory :"+dirName);
			}
		} catch (Exception e) {
			log.warning("makeDirectories exception ::: " +  e.getMessage());
			
		}
	}

	/**
	 * Calling @SymbolicLinkWriter to write the symbolic link to disk.
	 * 
	 * @param eniqSymbolicLink
	 */
	protected void writeSymbolicLink(final EniqSymbolicLink eniqSymbolicLink, final Date startDate, final String neType, final String fileLocation) {
		symbolicLinkWriter = new SymbolicLinkWriter(eniqSymbolicLink, log, startDate, neType, fileLocation);
		symbolicLinkWriter.createSymbolicLink();
	}


}