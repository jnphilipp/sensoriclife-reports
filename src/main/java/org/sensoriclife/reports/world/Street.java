package org.sensoriclife.reports.world;

import java.util.ArrayList;

public class Street implements Cloneable{

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
	
	public Street clone() throws CloneNotSupportedException{
		Street clone = (Street)super.clone();
		return clone;
	}
}
