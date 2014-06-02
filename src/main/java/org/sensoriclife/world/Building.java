package org.sensoriclife.world;

import java.io.Serializable;
import java.util.ArrayList;

public class Building implements Serializable {

	private static final long serialVersionUID = 1L;
	private String name;
	private ArrayList<DeviceUnit> residentialUnits;

	public Building() {
		residentialUnits = new ArrayList<DeviceUnit>();
	}

	public ArrayList<DeviceUnit> getResidentialUnits() {
		return residentialUnits;
	}

	public void setResidentialUnits(ArrayList<DeviceUnit> residentialUnits) {
		this.residentialUnits = residentialUnits;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
