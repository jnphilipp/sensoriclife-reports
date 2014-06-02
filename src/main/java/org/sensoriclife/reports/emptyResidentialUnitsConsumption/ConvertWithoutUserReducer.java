package org.sensoriclife.reports.emptyResidentialUnitsConsumption;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.DeviceUnit;

public class ConvertWithoutUserReducer extends Reducer<Text, DeviceUnit, Text, Mutation> {

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
		boolean existUser = false;
		for(DeviceUnit rU : values)
		{
			if(rU.isSetDeviceAmount())
			{
				amount = rU.getDeviceAmount();
				if(amount < minAmount)				
					minAmount = amount;
					
				if(amount > maxAmount)
					maxAmount = amount;
					
				existAmount = true;
			}
			if(rU.isSetResidentialID())
			{
				address = rU.getResidentialID();
			}
			
			if(rU.isSetDeviceSecontAmount())
			{
				amount = rU.getDeviceSecontAmount();
				if(amount < maxAmountOutOfTimeRange){
					maxAmountOutOfTimeRange = amount;
				}
				
				existOutOfTimeRangeAmount = true;
			}
			if(rU.isSetUserID())
			{
				existUser = true;
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
		
		if(!address.equals("") && existAmount && !existUser && amount > 0)
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