package org.sensoriclife.world;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class Consumption implements Serializable, Writable {

	private static final long serialVersionUID = 1L;
	private int consumptionId;
	private String counterType;
	private long timestamp;
	private int dayOfYear;
	private double amount;

	public int getConsumptionId() {
		return consumptionId;
	}

	public void setConsumptionId(int consumptionId) {
		this.consumptionId = consumptionId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	public double getAmount() {
		return amount;
	}

	public int getDayOfYear() {
		return dayOfYear;
	}

	public void setDayOfYear(int dayOfYear) {
		this.dayOfYear = dayOfYear;
	}
	
	public String getCounterType() {
		return counterType;
	}
	
	public void setCounterType(String counterType) {
		this.counterType = counterType;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(consumptionId);
		out.writeUTF(counterType);
		out.writeLong(timestamp);
		out.writeInt(dayOfYear);
		out.writeDouble(amount);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		consumptionId = in.readInt();
		counterType = in.readUTF();
		timestamp = in.readLong();
		dayOfYear = in.readInt();
		amount = in.readDouble();
	}

}
