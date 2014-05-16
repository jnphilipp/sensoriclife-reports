package org.sensoriclife.reports.world;

import java.io.Serializable;
import java.util.ArrayList;

public class World implements Serializable{
	
	private static final long serialVersionUID = 1L;
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
}
