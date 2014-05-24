package org.sensoriclife.world;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class ResidentialUnit implements Serializable, Writable,
		Comparable<ResidentialUnit> {

	private static final long serialVersionUID = 1L;

	private String consumptionID = "";
	private float deviceAmount = -1;
	private String counterType = "";
	private String residentialID = "";
	private int userID = 0;
	private String userResidential = "";
	private long timeStamp = 0;

	private boolean isSetConsumptionID = false;
	private boolean isSetDeviceAmount = false;
	private boolean isSetCounterType = false;
	private boolean isSetResidentialID = false;
	private boolean isSetUserID = false;
	private boolean isSetUserResidential = false;
	private boolean isSetTimeStamp = false;

	public ResidentialUnit() {
	}

	public String getConsumptionID() {
		return consumptionID;
	}

	public void setConsumptionID(String consumptionID) {
		this.isSetConsumptionID = true;
		this.consumptionID = consumptionID;
	}

	public float getDeviceAmount() {
		return deviceAmount;
	}

	public void setDeviceAmount(float deviceAmount) {
		this.isSetDeviceAmount = true;
		this.deviceAmount = deviceAmount;
	}

	public String getResidentialID() {
		return residentialID;
	}

	public void setResidentialID(String residentialID) {
		this.isSetResidentialID = true;
		this.residentialID = residentialID;
	}

	public int getUserID() {
		return userID;
	}

	public void setUserID(int userID) {
		this.isSetUserID = true;
		this.userID = userID;
	}

	public String getUserResidential() {
		return userResidential;
	}

	public void setUserResidential(String userResidential) {

		this.isSetUserResidential = true;
		this.userResidential = userResidential;
	}

	public void setTimeStamp(long timeStamp) {
		this.isSetTimeStamp = true;
		this.timeStamp = timeStamp;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public boolean isSetConsumptionID() {
		return isSetConsumptionID;
	}

	public boolean isSetDeviceAmount() {
		return isSetDeviceAmount;
	}

	public boolean isSetResidentialID() {
		return isSetResidentialID;
	}

	public boolean isSetUserID() {
		return isSetUserID;
	}

	public boolean isSetUserResidential() {
		return isSetUserResidential;
	}

	public boolean isSetTimeStamp() {
		return isSetTimeStamp();
	}

	public String getCounterType() {
		return counterType;
	}

	public void setCounterType(String counterType) {
		this.counterType = counterType;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.userID);
		out.writeFloat(this.deviceAmount);
		out.writeUTF(this.counterType);
		out.writeUTF(this.consumptionID);
		out.writeUTF(this.userResidential);
		out.writeUTF(this.residentialID);
		out.writeLong(this.timeStamp);

		out.writeBoolean(this.isSetConsumptionID);
		out.writeBoolean(this.isSetDeviceAmount);
		out.writeBoolean(this.isSetCounterType);
		out.writeBoolean(this.isSetUserID);
		out.writeBoolean(this.isSetResidentialID);
		out.writeBoolean(this.isSetUserResidential);
		out.writeBoolean(this.isSetTimeStamp);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.userID = in.readInt();
		this.deviceAmount = in.readFloat();
		this.counterType = in.readUTF();
		this.consumptionID = in.readUTF();
		this.userResidential = in.readUTF();
		this.residentialID = in.readUTF();
		this.timeStamp = in.readLong();

		this.isSetConsumptionID = in.readBoolean();
		this.isSetDeviceAmount = in.readBoolean();
		this.isSetCounterType = in.readBoolean();
		this.isSetUserID = in.readBoolean();
		this.isSetResidentialID = in.readBoolean();
		this.isSetUserResidential = in.readBoolean();
		this.isSetTimeStamp = in.readBoolean();
	}

	@Override
	public int compareTo(ResidentialUnit ru) {
		if (ru.getConsumptionID().equals(getConsumptionID())
				&& ru.getCounterType().equals(getCounterType())
				&& ru.getDeviceAmount() == getDeviceAmount()
				&& ru.getResidentialID().equals(getResidentialID())
				&& ru.getTimeStamp() == getTimeStamp()
				&& ru.getUserID() == getUserID()
				&& ru.getUserResidential().equals(getUserResidential())) {
			return 0;
		} else {
			return -1;
		}
	}

	@Override
	public String toString() {
		String output = "residentialId: " + getResidentialID()
				+ " consumptionId: " + getConsumptionID() + " counterType: "
				+ getCounterType() + " deviceAmount: " + getDeviceAmount()
				+ " timestamp: " + getTimeStamp() + " userId: " + getUserID()
				+ " userResidential: " + getUserResidential();
		return output;
	}
}
