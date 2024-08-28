package com.ericsson.eniq.flssymlink.fls;

import java.util.Date;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.ericsson.eniq.flssymlink.symlink.SymbolicLinkCreationHelper;

/*
 * This class is used to add PmJson objects into PmQueue to handle thread pool executor
 */

public class PmQueueHandler implements Runnable{
	
	private PmJson pmJson;
	Logger log;
	MixedNodeCache mCache;
	private String ossId;
	private static final String FDN_FIRST_PART = ".*";
	private static final String FDN_SECOND_PART = "=(.+?)(,|$).*";
	private static final String MECONTEXT = "MeContext";
	private static final String NETWORKELEMENT = "NetworkElement";
	private static final String MANAGEDELEMENT = "ManagedElement";
	
	
	public PmQueueHandler(PmJson pmJson, Logger log, MixedNodeCache mCache, String ossId) {
		super();
		this.pmJson = pmJson;
		this.log = log;
		this.mCache = mCache;
		this.ossId = ossId;
	}

	@Override
	public void run() {
		try {
			Date startTime = new Date(System.currentTimeMillis());
			String nodeType = pmJson.getNodeType();
			String nodeName = pmJson.getNodeName();
			String fileLocation = pmJson.getFileLocation();
			String nodeNameKey;
			String radioNodeType;

			/***
			 * nodeType: PM_STATISTICAL_RadioNode nodeTypeDir: RadioNode/MIXED
			 *
			 * nodeType: PM_STATISTICAL_RadioNode_WCDMA nodeTypeDir: RadioNode/WRAT
			 *
			 * nodeType: PM_STATISTICAL_RadioNode_LTE nodeTypeDir: RadioNode/LRAT
			 */
			String dataType = NodeTypeDataTypeCache.getDataTypeForSymLink(nodeType, pmJson.getDataType(), log);
			if (dataType.isEmpty()) {
				log.warning("Symboliclink will not be created, "
						+ "No datatypes are found for the given nodeType : "+nodeType+" in NodeTypeDataTypeCache");
				return;
			}

			log.finest("DataType is " + dataType);

			if (nodeType.equalsIgnoreCase("RadioNode")) {
				if (Pattern.compile(".+_LTE.xml$").matcher(fileLocation).matches()) {
					SymbolicLinkCreationHelper.createSymbolicLink(dataType + "_" + nodeType + "_LTE", fileLocation, log,
							startTime, nodeType + "_PM", ossId);
				} else if (Pattern.compile(".+_WCDMA.xml$").matcher(fileLocation).matches()) {
					SymbolicLinkCreationHelper.createSymbolicLink(dataType + "_" + nodeType + "_WCDMA", fileLocation,
							log, startTime, nodeType + "_PM", ossId);
				} else {
					nodeNameKey = getNodeNameKey(nodeName);

					try {
						radioNodeType = mCache.getMixedNodeTechnologyType(nodeNameKey, log);
						if (radioNodeType.equalsIgnoreCase(MixedNodeCache.EMPTY_CACHE)) {
							log.warning("MixedNode cache is empty!! PM file Symlink will not be created for: "
									+ fileLocation);
						} else if (Pattern.compile(MixedNodeCache.MIXED_NODE_PATTERN).matcher(radioNodeType)
								.matches()) {
							SymbolicLinkCreationHelper.createSymbolicLink(dataType + "_" + nodeType, fileLocation, log,
									startTime, nodeType + "_PM", ossId);
						} else if (radioNodeType.equalsIgnoreCase(MixedNodeCache.NO_FDN)) {
							log.warning(nodeName
									+ " Node is not present in the MixedNode cache. PM file Symlink will not be created for: "
									+ fileLocation);
						} else {
							SymbolicLinkCreationHelper.createSymbolicLink(
									dataType + "_" + nodeType + "_" + radioNodeType, fileLocation, log, startTime,
									nodeType + "_PM", ossId);
						}
					} catch (Exception e) {
						log.warning("PM file Symlink creation failed for: " + fileLocation
								+ ". Exception while retrieving MixedNode cache  " + e.getMessage());
					}
				}
			} else {
				SymbolicLinkCreationHelper.createSymbolicLink(dataType + "_" + nodeType, fileLocation, log,
						startTime, nodeType + "_PM", ossId);
			}

		} catch (Exception e) {
			log.warning("Exception occured while creating symbolic link for PM!! " + e.getMessage() + '\t'
					+ pmJson.getNodeType() + "   " + pmJson.getFileLocation());
		}

	}
	
	public static String getNodeNameKey(String fdn ) {
		Matcher m = getMatcher(fdn, MECONTEXT);
		if(m.matches()) {
			return m.group(1);
		} else {
			m = getMatcher(fdn, NETWORKELEMENT);
			if (m.matches()) {
				return m.group(1);
			} else {
				m = getMatcher(fdn, MANAGEDELEMENT);
				if(m.matches()) {
					return m.group(1);
				} else {
					return fdn;
				}
			}
		}
	}
	
	private static Matcher getMatcher(String fdn, String nodeIdentifier) {
		Pattern pattern = Pattern.compile(FDN_FIRST_PART+nodeIdentifier+FDN_SECOND_PART);
		Matcher m = pattern.matcher(fdn);
		return m;
	}
	
		

}
