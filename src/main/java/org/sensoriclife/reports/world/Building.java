package org.sensoriclife.reports.world;

import java.io.Serializable;
import java.util.ArrayList;

public class Building implements Serializable {

	private static final long serialVersionUID = 1L;
	private String name;
	private ArrayList<ResidentialUnit> residentialUnits;

	public Building() {
		residentialUnits = new ArrayList<ResidentialUnit>();
	}

	public ArrayList<ResidentialUnit> getResidentialUnits() {
		return residentialUnits;
	}

	public void setResidentialUnits(ArrayList<ResidentialUnit> residentialUnits) {
		this.residentialUnits = residentialUnits;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
