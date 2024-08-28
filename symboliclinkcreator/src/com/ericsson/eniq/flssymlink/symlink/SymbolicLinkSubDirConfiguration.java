package com.ericsson.eniq.flssymlink.symlink;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xnagdas
 *  
 * A holder class for data read from the XML file for ENIQ-S. The
 * directories nodeTypeDir/subdir[] exist below the directories defined
 * in the eniq.xml file.
 * 
 * An object of this class represent the complete details for a netype in eniq.xml
 * Used by @DirectoryConfigurationFileParser and @AbstractSymbolicLinkFactory for creating
 * Symbolic links in ENIQ-S
 * 
 *
 */
class SymbolicLinkSubDirConfiguration {

    /**
     * NeType as defined in eniq.xml
     */
    private String name = "";

    /**
     * Maximum number of symbolic links for the NeType
     * Read from eniq.xml
     */
    private int maxNumLinks = -1;

    /**
     * Directory where links should be created for the NeType
     * Read from eniq.xml
     */
    private String nodeTypeDir = "";
    
    /**
     *  File filter Regex, to match the symbolic link creation of MINI-LINK node type
     */
   // EQEV-60726
    private String fileFilter="";

    /**
     * List of sub directories under NeType nodeTypeDir.
     * Using the ArrayList to maintain the order in
     * which its read from the eniq.xml file
     */
    private final List<String> subDirs = new ArrayList<String>();
    
    /**
     * Get the regex of the filefilter for MINI-LINK node types
     */
    //EQEV-60726
    public String getFileFilter() {
		return fileFilter;
	}
    
    
    /**
     * Set the regex of the filefilter for MINI-LINK node types
     */
    
    //EQEV-60726
	public void setFileFilter(String fileFilter) {
		this.fileFilter = fileFilter;
	}

    /**
     * Set the maxNumLinks attribute.
     * @param maxNumLinks int the new value for maxNumLinks
     */
    public void setMaxNumLinks(final int maxNumLinks) {
        this.maxNumLinks = maxNumLinks;
    }


	/**
     * Get the value of the maxNumLinks attribute.
     * @return int the value of the maxNumLinks attribute
     */
    public int getMaxNumLinks() {
        return maxNumLinks;
    }

    /**
     * Set the neType attribute.
     * @param name String the new value for neType
     */
    public void setName(final String configurationName) {
        this.name = configurationName;
    }

    /**
     * @return - name
     */
    public String getName() {
        return name;
    }

    /**
     * Set the directory name appending a File separator.
     * @param nodeTypeDir String the new value for nodeTypeDir
     */
    public void setNodeTypeDir(final String dir) {
        this.nodeTypeDir = dir + File.separator;
    }

    /**
     * Get the value of the nodeTypeDir attribute.
     * @return String the value of the nodeTypeDir attribute
     */
    public String getNodeTypeDir() {
        return nodeTypeDir;
    }

    /**
     * Add a sub directory to the subdir list appending a File separator
     * @param subDir String the new sub directory to add to the subdir list.
     */
    public void addSubDir(final String subDir) {
        subDirs.add(subDir + File.separator);
    }

    /**
     * Get the List of the sub directories.
     * @return String[] the value of the subdir attribute
     */
    public List<String> getSubDirs() {
        return subDirs;
    }


	@Override
	public String toString() {
		return "SymbolicLinkSubDirConfiguration [name=" + name + ", maxNumLinks=" + maxNumLinks + ", nodeTypeDir="
				+ nodeTypeDir + ", fileFilter=" + fileFilter + ", subDirs=" + subDirs + "]";
	}

}
