package org.sensoriclife.world;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class Consumption implements Serializable, Writable {

	private static final long serialVersionUID = 1L;
	private int consumptionId;
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

	public Object deepCopy(Object oldObj) throws Exception {
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(bos);
			// serialize and pass the object
			oos.writeObject(oldObj);
			oos.flush();
			ByteArrayInputStream bin = new ByteArrayInputStream(
					bos.toByteArray());
			ois = new ObjectInputStream(bin);
			// return the new object
			return ois.readObject();
		} catch (Exception e) {
			System.out.println("Exception = " + e);
			throw (e);
		} finally {
			oos.close();
			ois.close();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(consumptionId);
		out.writeLong(timestamp);
		out.writeInt(dayOfYear);
		out.writeDouble(amount);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		consumptionId = in.readInt();
		timestamp = in.readLong();
		dayOfYear = in.readInt();
		amount = in.readDouble();
	}

}
