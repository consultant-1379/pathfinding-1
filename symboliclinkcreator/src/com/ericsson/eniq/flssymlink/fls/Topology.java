package com.ericsson.eniq.flssymlink.fls;

import java.util.ArrayList;
import java.util.List;

public class Topology {
	private String time;
	private ArrayList<Long> ids;
	
	public Topology(){
	}
	
	public Topology(String time , ArrayList<Long> ids){
		this.time = time ;
		this.ids = ids ;
		
	}
	
	public String getTime() {
		return time;
	}
	
	public void setTime(String time) {
		this.time = time;
	}
	
	public ArrayList<Long> getIds() {
		return ids;
	}
	
	public void setIds(ArrayList<Long> ids) {
		this.ids = ids;
	}

}
