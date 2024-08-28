package com.ericsson.eniq.flssymlink.fls;

import java.util.ArrayList;

public class TopologyJsonArray {
	ArrayList<TopologyJson> files;

	public TopologyJsonArray(ArrayList<TopologyJson> files) {
		super();
		this.files = files;
	}

	public TopologyJsonArray() {
		super();
	}

	public ArrayList<TopologyJson> getFiles() {
		return files;
	}

	public void setFiles(ArrayList<TopologyJson> files) {
		this.files = files;
	}
	
}
