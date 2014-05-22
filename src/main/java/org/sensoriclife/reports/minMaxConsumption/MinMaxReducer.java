package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.ResidentialUnit;

/**
 * 
 * @author marcel, yves
 * 
 */
public class MinMaxReducer extends
		Reducer<Text, ResidentialUnit, Text, Mutation> {

	public void reduce(Text key, Iterable<ResidentialUnit> values,
			Context c) throws IOException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		String outputTableName = conf.getStrings("outputTableName","MinMaxTable")[0];
		int selector = conf.getInt("selectModus", 0);
		String qualifierName = "";
		
		
		float min = Float.MAX_VALUE;
		String minResID = "";
		float max = Float.MIN_VALUE;
		String maxResID = "";


		for(ResidentialUnit value:values)
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
			
			Mutation m1 = new Mutation(key+"_"+qualifierName);
			
			// write minimum/maximum
			m1.put("min", qualifierName,new Value(minResID.getBytes()));
			m1.put("min", "amount",new Value(Helpers.toByteArray(min)));
			m1.put("max", qualifierName,new Value(maxResID.getBytes()));
			m1.put("max", "amount",new Value(Helpers.toByteArray(max)));
		
			c.write(new Text(outputTableName), m1);
		}
	}
}
