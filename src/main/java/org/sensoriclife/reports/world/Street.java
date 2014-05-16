package org.sensoriclife.reports.world;

import java.io.Serializable;
import java.util.ArrayList;

public class Street implements Serializable{

	private static final long serialVersionUID = 1L;
	private String name;
	private ArrayList<Building> buildings;

	public Street() {
		buildings = new ArrayList<Building>();
	}

	public ArrayList<Building> getBuildings() {
		return buildings;
	}
	
	public void setBuildings(ArrayList<Building> buildings) {
		this.buildings = buildings;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
}
