package com.ericsson.eniq.flssymlink.fls;


import java.util.List;

public class MaxIdTokenArray {
	
	List<MaxIdToken> files;
	
	public MaxIdTokenArray(List<MaxIdToken> files) {
		super();
		this.files = files;
	}

	public MaxIdTokenArray() {
		
	}

	public List<MaxIdToken> getFiles() {
		return files;
	}

	public void setFiles(List<MaxIdToken> files) {
		this.files = files;
	}
}
