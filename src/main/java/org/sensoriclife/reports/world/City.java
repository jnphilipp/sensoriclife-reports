package org.sensoriclife.reports.world;

import java.io.Serializable;
import java.util.ArrayList;

public class City implements Serializable{
	
	private static final long serialVersionUID = 1L;
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
}
