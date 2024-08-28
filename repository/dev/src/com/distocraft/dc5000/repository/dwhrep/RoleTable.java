package com.distocraft.dc5000.repository.dwhrep;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import ssc.rockfactory.RockDBObject;
import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

/**
 * Not required currently. Included in case the class is 
 * designed in future as improvement.
 * @author xarjsin
 *
 */
@SuppressWarnings({"PMD.SuspiciousConstantFieldName"})
public class RoleTable implements Cloneable,RockDBObject {
	
	private String ENIQ_ID;
    private String IP_ADDRESS;
    private String ROLE;
    
    private Set<String> modifiedColumns = new HashSet<String>();
	
	public Set<String> gimmeModifiedColumns() {
		return modifiedColumns;
	}
	
	public void cleanModifiedColumns() {
		modifiedColumns.clear();
	}
	
	public int insertDB() throws SQLException, RockException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int deleteDB() throws SQLException, RockException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int updateDB() throws SQLException, RockException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/**
	   * to enable a public clone method inherited from Object class (private method)
	   */
	public Object clone() {
	    Object o = null;
	    try {
	      o = super.clone();
	    } catch (CloneNotSupportedException e) {
	    }
	    return o;
	  }
}