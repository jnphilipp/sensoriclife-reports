package org.sensoriclife.reports.world;

import java.util.ArrayList;

public class World implements Cloneable{
	
	private ArrayList<City> cities;

	public World() {
		cities = new ArrayList<City>();
	}
	
	public ArrayList<City> getCities() {
		return cities;
	}
	
	public void setCities(ArrayList<City> cities) {
		this.cities = cities;
	}
	
	public World clone() throws CloneNotSupportedException{
		World clone = (World)super.clone();
		return clone;
	}
}
