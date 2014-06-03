package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.DeviceUnit;

/**
 * 
 * @author marcel, yves
 * 
 */
public class MinMaxReducer extends
		Reducer<Text, DeviceUnit, Text, Mutation> {

	public void reduce(Text key, Iterable<DeviceUnit> values,
			Context c) throws IOException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		String outputTableName = conf.getStrings("outputTableName","MinMaxTable")[0];
		int selector = conf.getInt("selectModus", 0);
		long reportTimestamp = conf.getLong("reportTimestamp", 0);
		String qualifierName = "";
		
		
		float min = Float.MAX_VALUE;
		String minResID = "";
		float max = Float.MIN_VALUE;
		String maxResID = "";


		for(DeviceUnit value:values)
		{
			if(min > value.getDeviceAmount())
			{
				min = value.getDeviceAmount();
				minResID = value.getResidentialID();
			}
			if(max < value.getDeviceAmount())
			{
				max = value.getDeviceAmount();
				maxResID = value.getResidentialID();
			}
		}
		
		if(selector != 0)
		{
			if(selector == 5)
				qualifierName = "residentialUnit";
			if(selector == 4)
				qualifierName = "building";
			
			Mutation m1 = new Mutation(Helpers.toByteArray(key.toString()+"_"+qualifierName));
			
			// write minimum/maximum
			if(reportTimestamp != 0)
			{
				m1.put(Helpers.toByteArray("min"), Helpers.toByteArray(qualifierName),reportTimestamp,Helpers.toByteArray(minResID));
				m1.put(Helpers.toByteArray("min"), Helpers.toByteArray("amount"),reportTimestamp,Helpers.toByteArray(min));
				m1.put(Helpers.toByteArray("max"), Helpers.toByteArray(qualifierName),reportTimestamp,Helpers.toByteArray(maxResID));
				m1.put(Helpers.toByteArray("max"), Helpers.toByteArray("amount"),reportTimestamp,Helpers.toByteArray(max));
			}
			else
			{
				m1.put(Helpers.toByteArray("min"), Helpers.toByteArray(qualifierName),Helpers.toByteArray(minResID.getBytes()));
				m1.put(Helpers.toByteArray("min"), Helpers.toByteArray("amount"),Helpers.toByteArray(min));
				m1.put(Helpers.toByteArray("max"), Helpers.toByteArray(qualifierName),Helpers.toByteArray(maxResID.getBytes()));
				m1.put(Helpers.toByteArray("max"), Helpers.toByteArray("amount"),Helpers.toByteArray(max));
			}
				
				
		
			c.write(new Text(outputTableName), m1);
		}
	}
}
