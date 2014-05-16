package org.sensoriclife.reports.world;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class Consumption implements Serializable, Writable{

	private static final long serialVersionUID = 1L;
	private int consumptionId;
	private long timestamp;
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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(consumptionId);
		out.writeLong(timestamp);
		out.writeDouble(amount);	
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		consumptionId = in.readInt();
		timestamp = in.readLong();
		amount = in.readDouble();
	}
	
}
