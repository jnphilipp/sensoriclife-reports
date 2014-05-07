package org.sensoriclife.reports.world;

import org.apache.accumulo.core.data.Value;

public class Consumption {
	
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
	
}
