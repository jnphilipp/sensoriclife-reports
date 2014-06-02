package org.sensoriclife.reports.helper;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.DeviceUnit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConvertMapper extends Mapper<Key, Value, Text, DeviceUnit> {
	
	private float minAmount = Float.MAX_VALUE;
	private float maxAmount = Float.MIN_VALUE;
	private float nextAmount = Float.MAX_VALUE;
	private String residentialID = "";
	private String consumptionID = "";
	
	private boolean isSetMinAmount = false;
	private boolean isSetMaxAmount = false;
	private boolean isSetNextAmount = false;
	private boolean isSetResidentialID = false;
	
	private String rowID = "";
	
	public void map(Key k, Value v, Context c) throws IOException, InterruptedException {
		
		consumptionID = k.getRow().toString();
		String family = k.getColumnFamily().toString();
		String qualifier = k.getColumnQualifier().toString();
		
		if(rowID.equals("")){
			rowID = consumptionID;
		}
		else if(!rowID.equals(consumptionID)){
			
			DeviceUnit flat = new DeviceUnit();
			flat.setConsumptionID(rowID);
			if(isSetMinAmount){
				flat.setDeviceMinAmount(minAmount);
			}
			if(isSetMaxAmount){
				flat.setDeviceMaxAmount(maxAmount);
			}
			if(isSetNextAmount){
				flat.setDeviceSecontAmount(nextAmount);
			}
			if(isSetResidentialID){
				flat.setResidentialID(residentialID);
				c.write(new Text(rowID),flat);
			}	
			isSetMinAmount = false;
			isSetMaxAmount = false;
			isSetNextAmount = false;
			isSetResidentialID = false;
			
			minAmount = Float.MAX_VALUE;
			maxAmount = Float.MIN_VALUE;
			nextAmount = Float.MAX_VALUE;
			rowID = consumptionID;
		}
		
		if(family.equals("device") && qualifier.equals("amount"))
		{
			Configuration conf = new Configuration();
			conf = c.getConfiguration();
			long maxTs = conf.getLong("maxTimestamp", Long.MAX_VALUE);
			long minTs = conf.getLong("minTimestamp", 0);
			boolean onlyInTimeRange = conf.getBoolean("onlyInTimeRange", false);
			
			Long timestamp = k.getTimestamp();
			
			if (timestamp > minTs && timestamp <= maxTs) {
				try{
					float currentAmount = (Float)Helpers.toObject(v.get());
					if(minAmount > currentAmount){
						isSetMinAmount = true;
						minAmount = currentAmount;
					}
					if(maxAmount < currentAmount){
						isSetMaxAmount = true;
						maxAmount = currentAmount;
					}
				} catch (ClassNotFoundException e) {}
			}
			else if(!onlyInTimeRange)
			{
				if(timestamp > maxTs)
				{
					try {
						float currentAmount = (Float)Helpers.toObject(v.get());
						if(nextAmount > currentAmount){
							isSetNextAmount = true;
							nextAmount = currentAmount;
						}
					} catch (ClassNotFoundException e) {}
				}
			}
		}
		
		else if(family.equals("residential") && qualifier.equals("id"))
		{
			isSetResidentialID = true;
			residentialID = v.toString();
		}	
	}
	
	
	public void cleanup(Context c) throws IOException, InterruptedException{
		
		if(!rowID.equals("")){
			DeviceUnit flat = new DeviceUnit();
			flat.setConsumptionID(consumptionID);
			if(isSetMinAmount){
				flat.setDeviceMinAmount(minAmount);
			}
			if(isSetMaxAmount){
				flat.setDeviceMaxAmount(maxAmount);
			}
			if(isSetNextAmount){
				flat.setDeviceSecontAmount(nextAmount);
			}
			if(isSetResidentialID){
				flat.setResidentialID(residentialID);
				c.write(new Text(consumptionID),flat);
			}
		}
		
	}
}
