/**
 * 
 */
package com.ericsson.eniq.flssymlink.symlink;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.ericsson.eniq.flssymlink.fls.Main;

/**
 * 
 * @author xnagdas
 *
 */

public class SymbolicLinkCreationHelper {
	
	/**
	 * To create symbolic links for files collected
	 */
	private static ConcurrentHashMap<String, EniqSymbolicLinkFactory> eniqstatsSymbolicLinkFactoryMap = new ConcurrentHashMap<String, EniqSymbolicLinkFactory>();

	/**
	 * private constructor
	 */
	private SymbolicLinkCreationHelper(){
		
	}
		
	/**
	 * 	
	 * @param nodeType
	 * @param absoluteOssFilePath
	 * @param log
	 */	
	public static void createSymbolicLink(final String nodeType, final String absoluteOssFilePath, Logger log, final Date startDate, final String neType, String ossId) {

		AbstractSymbolicLinkFactory symbolicLinkFactory = null;
		try {
			symbolicLinkFactory = getEniqstatsSymbolicLinkFactory(log, ossId);
			if (symbolicLinkFactory != null) {
				symbolicLinkFactory.createSymbolicLink(nodeType,absoluteOssFilePath, startDate, neType);
			}
		} catch (Exception e) {
			log.warning(e + " Symbolic Link Creation Failed for "+ absoluteOssFilePath);
		}
	}	
	
	/**
	 * 
	 * @return
	 */
	private static EniqSymbolicLinkFactory getEniqstatsSymbolicLinkFactory(Logger log, String ossId) {
		EniqSymbolicLinkFactory eslFactory = null;
		log.finest("Getting symbolic link factory with OssId = "+ossId+" from map :"+eniqstatsSymbolicLinkFactoryMap.toString());
		if ((eslFactory = eniqstatsSymbolicLinkFactoryMap.get(ossId)) != null) {
			return eslFactory;
		} else {
			synchronized(Main.getSlcLock(ossId)) {
				if ((eslFactory = eniqstatsSymbolicLinkFactoryMap.get(ossId)) != null) {
					return eslFactory;
				}
				eslFactory = new EniqSymbolicLinkFactory(ossId);
				eslFactory.initialiseFactory(log);
				eniqstatsSymbolicLinkFactoryMap.put(ossId, eslFactory);
				return eslFactory;
			}

		}
	}
}


