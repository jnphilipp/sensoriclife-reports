package org.sensoriclife.reports.world;

import java.util.Date;

public class Consumption {
	
	private int consumptionId;
	private Date timestamp;
	private int amount;
	
	public int getConsumptionId() {
		return consumptionId;
	}
	
	public void setConsumptionId(int consumptionId) {
		this.consumptionId = consumptionId;
	}
	
	public Date getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}
	
	public void setAmount(int amount) {
		this.amount = amount;
	}
	
	public int getAmount() {
		return amount;
	}
	
}
