package com.ericsson.eniq.flssymlink.symlink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.ericsson.eniq.flssymlink.StaticProperties;



public class DirectoryConfigurationFileParser extends DefaultHandler {

    /**
     * Default value of eniq.xml file in the server
     * /eniq/sw/conf
     */
    private static final String ENIQ_DIRECTORY_CONFIGURATION_XML = StaticProperties.getProperty("DIRECTORY_CONFIGURATION_XML_PATH", "/eniq/sw/conf/eniq.xml" );

    /**
     * Singleton instance of this class
     */
    private static DirectoryConfigurationFileParser instance = null;

    /**
     * An object which represents all details from the eniq.xml file
     * for a single neType
     */
    private SymbolicLinkSubDirConfiguration symbolicLinkSubDirConfiguration = null;

    /**
     * This entire map represents the whole XML file.
     * A map where NeType is the key and SymbolLinkSubDirConfiguration for that
     * NeType/configurationName as the value for easy and fast retrieval
     */
    static ConcurrentHashMap<String,List<Map<String,SymbolicLinkSubDirConfiguration>>> symbolicLinkSubDirConfigurations = new ConcurrentHashMap<>();

    /**
     * To hold the current XML tag @SupportedTags object
     */
    private SupportedTags currentTag = SupportedTags.noValue;

    /**
     * Value of the current tag
     */
    private String currentTagValue = "";
    
    private boolean isInitSuccessful;

    Logger log;
    
    /**
     * Supported XML tags by this parser
     */
    private enum SupportedTags {
    	//EQEV-60726
        InterfaceData, Interface, neType, maxNumLinks, nodeTypeDir, subdir, noValue, fileFilter;

        public static SupportedTags getTag(final String str) {
            try {
                return valueOf(str);
            } catch (final IllegalArgumentException e) {
            	//System.out.println("SupportedTags  :: "  + e);
                return noValue;
            }
        }
    }


    /**
     * Obtain an instance to this singleton.
     * @return SymLinkDataSAXParser an instance to this singleton
     */
    public static DirectoryConfigurationFileParser getInstance(Logger log) {
        if (instance == null) {
            instance = new DirectoryConfigurationFileParser(log);
        }
        return instance;
    }
    
    /**
     * Default constructor which parses the eniq.xml file
     */
	protected DirectoryConfigurationFileParser(Logger log) {
		try {
			this.log = log;
			SAXParserFactory.newInstance().newSAXParser().parse(new File(ENIQ_DIRECTORY_CONFIGURATION_XML), this);
			isInitSuccessful = true;
			log.log(Level.INFO, "Eniq.xml successfully parsed, result : "+symbolicLinkSubDirConfigurations);
		} catch (final FileNotFoundException e) {
			handleException(e,
					".ENIQ-M might not be installed on the server, ENIQ-S will not create the symbolic links");
		} catch (final Exception e) {
			handleException(e, e.getMessage());
		}
	}

    /**
     * Return the map of @SymbolicLinkSubDirConfiguration stored
     * per node type which is populated from the eniq.xml file
     * 
     * @return - the symbolicLinkSubDirConfigurations
     */
    public ConcurrentHashMap<String, List<Map<String,SymbolicLinkSubDirConfiguration>>> getSymbolicLinkSubDirConfigurations() {
        return symbolicLinkSubDirConfigurations;
    }

    /**
     * Called when starting to parse an element. If the element is an
     * interface, i.e. the main object being parsed, then create a new instance
     * and add it to the list.
     * 
     * @param - qName String the XML tag for this element
     * 
     * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
     */
    @Override
    public void startElement(final String uri, final String localName, final String qName, final Attributes inAttributes) {
    	currentTagValue = new String();
        currentTag = SupportedTags.getTag(qName);
        if (currentTag.equals(SupportedTags.Interface)) {
            // data for new NeType
            symbolicLinkSubDirConfiguration = new SymbolicLinkSubDirConfiguration();
        }
    }

    /**
     * Called when finished parsing an element, i.e. the value of the element was
     * read by the characters() method. Set the appropriate attribute in the holder
     * to the value read by the characters() method.
     * 
     * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void endElement(final String namespaceURL, final String lName, final String qName){
    	//log.info("currentTag:"+currentTag+" "+"currentTagValue:"+currentTagValue);
        try{
    	switch (currentTag) {
        case neType:
        	symbolicLinkSubDirConfiguration.setName(currentTagValue);
            break;
        case maxNumLinks:
            try {
            	symbolicLinkSubDirConfiguration.setMaxNumLinks(Integer.valueOf(currentTagValue));
            } catch (final NumberFormatException e) {
            	log.warning("endElement   ::: " + e);
                //ignore it, for some ne types there is no max limit
            }
            break;
        case fileFilter:   //FLS MINI-LINK-Indoor fileFilter checks EQEV-60726
        	symbolicLinkSubDirConfiguration.setFileFilter(currentTagValue);
        	break;
        case nodeTypeDir:
        	symbolicLinkSubDirConfiguration.setNodeTypeDir(currentTagValue);
            List<Map<String,SymbolicLinkSubDirConfiguration>> ntdList = symbolicLinkSubDirConfigurations.get(symbolicLinkSubDirConfiguration.getName());
            ConcurrentHashMap<String,SymbolicLinkSubDirConfiguration> map = new ConcurrentHashMap<>();
            map.put(currentTagValue, symbolicLinkSubDirConfiguration);
    	    if(ntdList != null) { // Mapping for neType is already present
    	    	ntdList.add(map);
    	    } else {
    	    	ntdList = new ArrayList<>();
    	    	ntdList.add(map);
    	    }
            symbolicLinkSubDirConfigurations.put(symbolicLinkSubDirConfiguration.getName(), ntdList);
            break;
        	
        case subdir:
        	symbolicLinkSubDirConfiguration.addSubDir(currentTagValue);
            break;
        default:
            break;
        }
        currentTag = SupportedTags.noValue;
        }
        catch(Exception e){
        	log.warning("Exception at endElement "+e.getMessage());
        }
    }

    /**
     * Read the value from the XML file. This value is then set in the holder object
     * by the endElement() method.
     * 
     * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
     */
    @Override
    public void characters(final char buf[], final int offset, final int len) {
    	final StringBuffer charBuffer = new StringBuffer(len);
		for (int i = offset; i < (offset + len); i++) {
			// If no control char
			if ((buf[i] != '\\') && (buf[i] != '\n') && (buf[i] != '\r') && (buf[i] != '\t')) {
				charBuffer.append(buf[i]);
			}
		}
		currentTagValue += charBuffer;
    	//currentTagValue = new String(buf, offset, len);
    }

    /**
     * Finished parsing the XML.
     * 
     * @see org.xml.sax.helpers.DefaultHandler#endDocument()
     */
    @Override
    public void endDocument() {
        symbolicLinkSubDirConfiguration = null;
    }

    /**
     * Handles the exceptions in this class
     * 
     * @param e - @Throwable object
     * @param type - "error" in case of major error
     * @param message - error message to be logged
     */
    private void handleException(final Throwable e, final String message) {
    	log.log(Level.WARNING,"DirectoryConfigurationFileParser  : " + message,e);
        symbolicLinkSubDirConfigurations.clear();
        symbolicLinkSubDirConfiguration = null;
        currentTagValue = "";
        currentTag = SupportedTags.noValue;
        isInitSuccessful = false;
    }

	public boolean isInitSuccessful() {
		return isInitSuccessful;
	}
}
