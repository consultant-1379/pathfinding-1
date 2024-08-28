package com.ericsson.eniq.flssymlink.fls;

import java.util.ArrayList;

public class PmJsonArray{
	ArrayList<PmJson> files;

	public ArrayList<PmJson> getFiles() {
		return files;
	}

	public void setFiles(ArrayList<PmJson> files) {
		this.files = files;
	}

	public PmJsonArray(ArrayList<PmJson> files) {
		super();
		this.files = files;
	}

	public PmJsonArray() {
		super();
	}
	
	
}