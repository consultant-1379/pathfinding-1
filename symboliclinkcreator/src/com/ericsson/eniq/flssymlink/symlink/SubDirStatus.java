package com.ericsson.eniq.flssymlink.symlink;

import java.io.File;

import  com.ericsson.eniq.flssymlink.symlink.SymbolicLinkCreationFailedException;
import java.util.logging.Logger;

/**
 * 
 * @author xnagdas
 *
 */
public class SubDirStatus {

    /**
     * Instance of the factory which has created and using this object
     */
    private final EniqSymbolicLinkFactory symbolicLinkFactory;

    /**
     * Represents the current sub directory number
     */
    int subDirNumber = 0;

    int numberOfLinks = 0;
    
    Logger log;

    /**
     * 
     * @param symbolicLinkFactory - factory which has created and using this object
     * @param numberOfLinks - Maximum number of links defined as per eniq.xml
     * @param subDirNumber - current sub directory number selected for link creation
     */
    SubDirStatus(final EniqSymbolicLinkFactory symbolicLinkFactory, final int numberOfLinks, final int subDirNumber, Logger log) {
        this.symbolicLinkFactory = symbolicLinkFactory;
        this.numberOfLinks = numberOfLinks;
        this.subDirNumber = subDirNumber;
        this.log=log;
    }
    
    
    /**
     * Finds the next available sub directory
     * based on the rule set and number of links in the directory.
     * @param symbolicLinkSubDirConfiguration
     * @throws SymbolicLinkCreationFailedException
     */
    void findNextAvailableSubDirectory(final SymbolicLinkSubDirConfiguration symbolicLinkSubDirConfiguration) {
    	try{
        int numberOfFoldersTried = 0;
        // Loop until a valid subdir found
        while (numberOfLinks > symbolicLinkSubDirConfiguration.getMaxNumLinks()) {
            if (symbolicLinkFactory.symLinkDirectoryRule == EniqSymbolicLinkFactory.MAX_PER_DIRECTORY) {
                /* MAX PER DIRECTORY RULE
                 * To save time we don't check the number of files every time but just count beginning with the
                 * number we so the last time we check the directory (in this method)
                 * Note how 1 gets added since if we choose this directory it will include the Symbolic Link
                 * we are about to create 
                 */
                numberOfLinks = 1 + getNumberOfExistingLinks(getSymbolicLinkSubFolder(symbolicLinkSubDirConfiguration,
                        subDirNumber));

                // Check if we are still above the maximum
                if (numberOfLinks > symbolicLinkSubDirConfiguration.getMaxNumLinks()) {
                    // if so go to the next folder
                    increaseSubDirNumber(symbolicLinkSubDirConfiguration);
                    // Set initial number of files to maxint so that next iteration will update it with ACTUAL #files 
                    numberOfLinks = Integer.MAX_VALUE;
                    // Count how many folders tried, after we have tried them all we need to exit and log the problem
                    if (++numberOfFoldersTried > symbolicLinkSubDirConfiguration.getSubDirs().size()) {
                    	log.warning("All sub directories have reached their maximum limit."+symbolicLinkSubDirConfiguration);
                    	
                    }
                }
            } else {
                /* MAX PER ITERATION RULE
                 * Don't care how many links are still in the folder once we counted to the Maximum we start with 
                 * the next folder in a round robin fashion  
                 */
                increaseSubDirNumber(symbolicLinkSubDirConfiguration);
                numberOfLinks = 1; // Again count the Symbolic Link we are about to create
            }
        }
    	}
    	catch(Exception e){
    		log.warning("Exception at findNextAvailableSubDirectory "+e.getMessage());
    	}
    }

    /**
     * Increment the sub directory number to look for
     *  
     * @param symbolicLinkSubDirConfiguration
     */
    private void increaseSubDirNumber(final SymbolicLinkSubDirConfiguration symbolicLinkSubDirConfiguration) {
        if (++subDirNumber >= symbolicLinkSubDirConfiguration.getSubDirs().size()) {
            subDirNumber = 0;
        }
    }


    /**
     * Returns the absolute path using the parameters
     *  
     * @param symbolicLinkSubDirConfiguration 
     * @param subDirectoryNumber - sub directory number
     * @return - absolute path of sub directory represented by subDirNumber
     */
    private String getSymbolicLinkSubFolder(final SymbolicLinkSubDirConfiguration symbolicLinkSubDirConfiguration,
            final int subDirectoryNumber) {
        final StringBuilder symbolicLinkSubFolder = new StringBuilder();
        symbolicLinkSubFolder.append(symbolicLinkFactory.getSymbolicLinkParentFolder());
        symbolicLinkSubFolder.append(symbolicLinkSubDirConfiguration.getNodeTypeDir());
        symbolicLinkSubFolder.append(symbolicLinkSubDirConfiguration.getSubDirs().get(subDirectoryNumber));
        return symbolicLinkSubFolder.toString();
    }

  
    /**
     * This is called only ONCE for each sub directory and then 
     * storing the count in this object for efficiency. 
     * 
     * @param directory - absolute name of the directory to look for
     * @return - number of current symbolic links in the directory
     */
    protected int getNumberOfExistingLinks(final String directoryName) {
        return new File(directoryName).listFiles().length;
    }


	@Override
	public String toString() {
		return "SubDirStatus [symbolicLinkFactory=" + symbolicLinkFactory + ", subDirNumber=" + subDirNumber
				+ ", numberOfLinks=" + numberOfLinks + "]";
	}
}
