package org.sensoriclife.reports.world;

import java.util.ArrayList;

public class District implements Cloneable{
	
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
	
	public District clone() throws CloneNotSupportedException{
		District clone = (District)super.clone();
		return clone;
	}
}
