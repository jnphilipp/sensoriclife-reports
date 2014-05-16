package org.sensoriclife.reports.world;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ResidentialUnit implements Writable, Cloneable {

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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(residentialId);
		out.writeInt(billUserId);
		out.writeInt(numberOfResidents);
		out.writeInt(electricMeterId);
		out.writeInt(coldWaterId);
		out.writeInt(warmWaterId);
		elecConsumption.write(out);
		out.writeInt(heatingIds.size());
		for(int i: heatingIds){
			out.writeInt(i);
		}
		
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		residentialId = in.readInt();
		billUserId = in.readInt();
		numberOfResidents = in.readInt();
		electricMeterId = in.readInt();
		coldWaterId = in.readInt();
		warmWaterId = in.readInt();
		elecConsumption.readFields(in);
		heatingIds.clear();
		int cnt = in.readInt();
		for (int x = 0; x < cnt; x++) {
	        heatingIds.add(in.readInt());
	    }
	}
}
