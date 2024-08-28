package com.ericsson.eniq.flssymlink.fls;

import java.io.Serializable;

public class MaxIdToken implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6496967936942598256L;
	private long id;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "MaxIdToken [maxId=" + id + "]";
	}

}
