/**
 * 
 */
package com.ericsson.eniq.flssymlink.fls;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xnagdas
 * 
 * Class for creating persisted token map for FLS Querying
 */

public class PersistedMixedNodeCache implements Serializable {

	/**
	 * serial id
	 */
	private static final long serialVersionUID = 1L;


//	radioNodeToTechnologyTypeMap to be serialized
	ConcurrentHashMap<String, String> radioNodeToTechnologyTypeMap;

	public PersistedMixedNodeCache(ConcurrentHashMap<String, String> radioNodeToTechnologyTypeMap) {
		super();
		this.radioNodeToTechnologyTypeMap = radioNodeToTechnologyTypeMap;
	}

	public ConcurrentHashMap<String, String>  getRadioNodeToTechnologyTypeMap() {
		return radioNodeToTechnologyTypeMap;
	}

	public void setRadioNodeToTechnologyTypeMap(ConcurrentHashMap<String, String>  radioNodeToTechnologyTypeMap) {
		this.radioNodeToTechnologyTypeMap = radioNodeToTechnologyTypeMap;
	}

	public PersistedMixedNodeCache() {
		super();
	}
	
}
