package org.sensoriclife.reports.world;

import java.io.Serializable;
import java.util.ArrayList;

public class District implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String name;
	private ArrayList<Street> streets;
	
	public District() {
		streets = new ArrayList<Street>();
	}

	public ArrayList<Street> getStreets() {
		return streets;
	}
	
	public void setStreets(ArrayList<Street> streets) {
		this.streets = streets;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
}
