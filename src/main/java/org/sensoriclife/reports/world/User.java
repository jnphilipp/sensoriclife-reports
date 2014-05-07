package org.sensoriclife.reports.world;

import java.util.ArrayList;

public class User implements Cloneable {

	private int userId;
	private int billResidentialId;
	private ArrayList<Integer> otherResidentialIds;
	private String name;

	public User() {
		otherResidentialIds = new ArrayList<Integer>();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public int getBillResidentialId() {
		return billResidentialId;
	}

	public void setBillResidentialId(int billResidentialId) {
		this.billResidentialId = billResidentialId;
	}

	public ArrayList<Integer> getOtherResidentialIds() {
		return otherResidentialIds;
	}

	public void setOtherResidentialIds(ArrayList<Integer> otherResidentialIds) {
		this.otherResidentialIds = otherResidentialIds;
	}

	public User clone() throws CloneNotSupportedException {
		User clone = (User) super.clone();
		return clone;
	}

}
