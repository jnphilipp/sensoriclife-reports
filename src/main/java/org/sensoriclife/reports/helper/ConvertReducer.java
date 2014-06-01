package org.sensoriclife.reports.helper;


import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.DeviceUnit;


public class ConvertReducer extends Reducer<Text, DeviceUnit, Text, Mutation> {

	public void reduce(Text key, Iterable<DeviceUnit> values,Context c) 
			throws IOException, InterruptedException {
	
		String counterType = key.toString();//.split("_")[1];
		
		String address = "";
		float maxAmountOutOfTimeRange = Float.MAX_VALUE;
		float minAmount = Float.MAX_VALUE;
		float maxAmount = Float.MIN_VALUE;
		float amount = 0;
		boolean existAmount = false;
		boolean existOutOfTimeRangeAmount = false;
		
		for(DeviceUnit rU : values)
		{
			if(rU.isSetDeviceMinAmount())
			{
				minAmount = rU.getDeviceMinAmount();
				existAmount = true;
			}
			if(rU.isSetDeviceMaxAmount()){
				maxAmount = rU.getDeviceMaxAmount();
				existAmount = true;
			}
			if(rU.isSetDeviceSecontAmount()){
				maxAmountOutOfTimeRange = rU.getDeviceSecontAmount();
				existOutOfTimeRangeAmount = true;
			}
			if(rU.isSetResidentialID())
			{
				address = rU.getResidentialID();
			}
		}
		
		if(!existOutOfTimeRangeAmount)
		{
			amount = maxAmount - minAmount;
		}
		else
		{
			amount = maxAmountOutOfTimeRange - minAmount;
		}
		
		if(!address.equals("") && existAmount)
		{
			Configuration conf = new Configuration();
			conf = c.getConfiguration();
			String outputTableName = conf.getStrings("outputTableName","yearConsumption")[0];
			long reportTimestamp = conf.getLong("reportTimestamp", 0);
			
			Mutation m1 = new Mutation(address+"_"+counterType);
			if(reportTimestamp != 0)
				m1.put("device", "amount",new ColumnVisibility(),reportTimestamp,new Value( Helpers.toByteArray(amount)));	
			else
				m1.put("device", "amount",new Value( Helpers.toByteArray(amount)));	
			
			c.write(new Text(outputTableName), m1);
		}
	}
}
