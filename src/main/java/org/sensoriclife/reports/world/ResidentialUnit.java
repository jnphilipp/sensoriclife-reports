package org.sensoriclife.reports.world;

import java.util.ArrayList;

public class ResidentialUnit implements Cloneable {

	private int residentialId;
	private int billUserId;
	private int numberOfResidents;
	private int electricMeterId;
	private int coldWaterId;
	private int warmWaterId;
	private ArrayList<Integer> heatingIds;
	private ElecConsumption elecConsumption;

	public ResidentialUnit() {
		heatingIds = new ArrayList<Integer>();
		elecConsumption = new ElecConsumption();
	}

	public String getAddress() {
		return null;
	}

	public int getResidentialId() {
		return residentialId;
	}

	public void setResidentialId(int residentialId) {
		this.residentialId = residentialId;
	}

	public int getBillUserId() {
		return billUserId;
	}

	public void setBillUserId(int billUserId) {
		this.billUserId = billUserId;
	}

	public int getNumberOfResidents() {
		return numberOfResidents;
	}

	public void setNumberOfResidents(int numberOfResidents) {
		this.numberOfResidents = numberOfResidents;
	}

	public int getElectricMeterId() {
		return electricMeterId;
	}

	public void setElectricMeterId(int electricMeterId) {
		this.electricMeterId = electricMeterId;
	}

	public int getColdWaterId() {
		return coldWaterId;
	}

	public void setColdWaterId(int coldWaterId) {
		this.coldWaterId = coldWaterId;
	}

	public int getWarmWaterId() {
		return warmWaterId;
	}

	public void setWarmWaterId(int warmWaterId) {
		this.warmWaterId = warmWaterId;
	}

	public ArrayList<Integer> getHeatingIds() {
		return heatingIds;
	}

	public void setHeatingIds(ArrayList<Integer> heatingId) {
		this.heatingIds = heatingId;
	}
	
	public ElecConsumption getElecConsumption() {
		return elecConsumption;
	}
	
	public void setElecConsumption(ElecConsumption elecConsumption) {
		this.elecConsumption = elecConsumption;
	}

	public ResidentialUnit clone() throws CloneNotSupportedException {
		ResidentialUnit clone = (ResidentialUnit) super.clone();
		return clone;
	}
}
