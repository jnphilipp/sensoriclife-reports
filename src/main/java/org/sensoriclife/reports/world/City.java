package org.sensoriclife.reports.world;

import java.util.ArrayList;

public class City implements Cloneable{
	
	private String name;
	private ArrayList<District> districts;

	public City() {
		districts = new ArrayList<District>();
	}
	
	public ArrayList<District> getDistricts() {
		return districts;
	}
	
	public void setDistricts(ArrayList<District> districts) {
		this.districts = districts;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public City clone() throws CloneNotSupportedException{
		City clone = (City)super.clone();
		return clone;
	}
}
