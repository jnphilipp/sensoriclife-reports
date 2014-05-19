package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.world.ResidentialUnit;

/**
 * 
 * @author marcel
 * 
 */
public class MinMaxReducer extends
		Reducer<Text, ResidentialUnit, Text, Mutation> {

	public void reduce(Text key, Iterable<ResidentialUnit> values,
			Context c) throws IOException, InterruptedException {
		
		float min = Float.MAX_VALUE;
		String minConID = "";
		String minResID = "";
		long minTimeStamp = 0;
		float max = Float.MIN_VALUE;
		String maxConID = "";
		String maxResID = "";
		long maxTimeStamp = 0;
		
		for(ResidentialUnit value: values)
		{
			if(value.isSetDeviceAmount()){
				if(value.getDeviceAmount() < min)
				{
					minConID = value.getConsumptionID();
					min = value.getDeviceAmount();
					minTimeStamp = value.getTimeStamp();
				}
					
				if(value.getDeviceAmount() > max)
				{
					maxConID = value.getConsumptionID();
					max = value.getDeviceAmount();
					maxTimeStamp = value.getTimeStamp();
				}
			}
		}
		
		for(ResidentialUnit value: values)
		{
			if(value.isSetResidentialID()){
				if(value.getConsumptionID().equals(minConID))
					minResID = value.getResidentialID();
				
				if(value.getConsumptionID().equals(maxConID))
					maxResID = value.getResidentialID();
			}
		}
		
		Mutation m1 = new Mutation(key);
		
		// write minimum/maximum
		m1.put("min", "residentialID",minTimeStamp,new Value(minResID.getBytes()));
		m1.put("min", "amount",minTimeStamp,new Value(new Float(min).toString().getBytes()));
		m1.put("max", "residentialID",maxTimeStamp,new Value(maxResID.getBytes()));
		m1.put("max", "amount",maxTimeStamp,new Value(new Float(max).toString().getBytes()));
		
		c.write(new Text("MinMax"), m1);
		
	}
}
