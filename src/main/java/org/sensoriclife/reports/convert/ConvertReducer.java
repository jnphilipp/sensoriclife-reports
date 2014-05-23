package org.sensoriclife.reports.convert;


import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.ResidentialUnit;


public class ConvertReducer extends Reducer<Text, ResidentialUnit, Text, Mutation> {

	public void reduce(Text key, Iterable<ResidentialUnit> values,Context c) 
			throws IOException, InterruptedException {
	
		String counterType = key.toString();//.split("_")[1];
		
		String address = "";
		float minAmountOutOfYear = Float.MAX_VALUE;
		float minAmount = Float.MAX_VALUE;
		float maxAmount = Float.MIN_VALUE;
		float amount = 0;
		boolean existAmount = false;
		boolean existOutOfYearAmount = false;
		
		for(ResidentialUnit rU : values)
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
				if(amount < minAmountOutOfYear)
					minAmountOutOfYear = amount;
				
				existOutOfYearAmount = true;
			}
			
		}
		
		if(!existOutOfYearAmount)
		{
			/*if(minAmount == maxAmount)
				amount = maxAmount;
			else*/
			amount = maxAmount - minAmount;
		}
		else
			amount = maxAmount - minAmountOutOfYear;
		
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
