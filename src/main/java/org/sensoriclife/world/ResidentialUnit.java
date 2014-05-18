package org.sensoriclife.world;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ResidentialUnit implements Serializable, Writable{

	private static final long serialVersionUID = 1L;
	private int residentialId;
	private int billUserId;
	private int numberOfResidents;
	private String electricMeterId;
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

	public String getElectricMeterId() {
		return electricMeterId;
	}

	public void setElectricMeterId(String electricMeterId) {
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
	
	public Object deepCopy(Object oldObj) throws Exception
	   {
	      ObjectOutputStream oos = null;
	      ObjectInputStream ois = null;
	      try
	      {
	         ByteArrayOutputStream bos = 
	               new ByteArrayOutputStream();
	         oos = new ObjectOutputStream(bos);
	         // serialize and pass the object
	         oos.writeObject(oldObj);
	         oos.flush();  
	         ByteArrayInputStream bin = 
	               new ByteArrayInputStream(bos.toByteArray());
	         ois = new ObjectInputStream(bin);
	         // return the new object
	         return ois.readObject();
	      }
	      catch(Exception e)
	      {
	         System.out.println("Exception = " + e);
	         throw(e);
	      }
	      finally
	      {
	         oos.close();
	         ois.close();
	      }
	   }

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(residentialId);
		out.writeInt(billUserId);
		out.writeInt(numberOfResidents);
		out.writeUTF(electricMeterId);
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
		electricMeterId = in.readUTF();
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
