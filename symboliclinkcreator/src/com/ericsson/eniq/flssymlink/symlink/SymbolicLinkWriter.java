package com.ericsson.eniq.flssymlink.symlink;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.logging.Logger;


/**
 * 
 * @author xnagdas
 *
 */
class SymbolicLinkWriter{

    
    /**
     * Represents the link to be created by this object
     */
    final EniqSymbolicLink eniqSymbolicLink;
    
    Logger log;
    
    Date startDate;
    
    String nodeType;
    
    String fileLocation;

    /**
     * @param eniqSymbolicLink
     */
    public SymbolicLinkWriter(final EniqSymbolicLink eniqSymbolicLink, Logger log, final Date startDate, final String nodeType, final String fileLocation) {
        this.eniqSymbolicLink = eniqSymbolicLink;
        this.log = log;
        this.startDate = startDate;
        this.nodeType = nodeType;
        this.fileLocation = fileLocation;
        
    } 
   
	/**
	 * Calls the NIO Files API to create Symbolic links
	 */
	public void createSymbolicLink() {
		try {

			/*
			 * Convert the string paths to Path using Paths utility and pass to
			 * NIO Files utility to create symbolic links
			 */						
			Files.createSymbolicLink(Paths.get(eniqSymbolicLink.getNewName()),Paths.get(eniqSymbolicLink.getOldName()));
			log.info("Successfully created symbolic link :: " + eniqSymbolicLink.getNewName().substring( eniqSymbolicLink.getNewName().lastIndexOf(File.separator) +1 ));
			log.finest("Successfully created symbolic link!!" + "     \n   " + "  ImportData Path : " + eniqSymbolicLink.getOldName() + "  \n  " +"Simlink Path    : " + eniqSymbolicLink.getNewName() );
			
			Date endTime = new Date(System.currentTimeMillis());
			Long diff = endTime.getTime() - startDate.getTime();
			if( nodeType.contains("_Topology") ){
				log.info("NodeType: " + nodeType.substring(0,nodeType.indexOf("_")) + " Time taken(in ms) to create symlink for TOPOLOGY file: "+ fileLocation +
						" is : " + diff.toString());
			}
			else if ( nodeType.contains("_PM") ){
				log.info("NodeType: " + nodeType.substring(0,nodeType.indexOf("_")) + " Time taken(in ms) to create symlink for PM file: "+ fileLocation +
						" is : " + diff.toString());
			}
			
			
		} catch (final FileAlreadyExistsException faee) {
			if( nodeType.contains("_Topology") ){
				log.info("FileAlreadyExistsException: Will not be creating Symbolic Link for TOPOLOGY file - "+  eniqSymbolicLink.getNewName());
			}
			else if ( nodeType.contains("_PM") ){
				log.info("FileAlreadyExistsException: Will not be creating Symbolic Link for PM file - "+  eniqSymbolicLink.getNewName());
			}
		} catch (final Exception ex) {
			if( nodeType.contains("_Topology") ){
				log.warning(" Exception: TOPOLOGY file Symbolic Link creation is failed ex ::: " + ex);
			}
			else if ( nodeType.contains("_PM") ){
				log.warning(" Exception: PM file Symbolic Link creation is failed ex ::: " + ex);
			}
		}

	}

}
