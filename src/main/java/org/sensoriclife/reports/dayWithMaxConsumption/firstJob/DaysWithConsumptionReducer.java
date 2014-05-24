package org.sensoriclife.reports.dayWithMaxConsumption.firstJob;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.Config;
import org.sensoriclife.world.ResidentialUnit;

/**
 * 
 * @author marcel
 * 
 */
public class DaysWithConsumptionReducer extends
		Reducer<LongWritable, ResidentialUnit, Text, Mutation> {

	public void reduce(LongWritable key, Iterable<ResidentialUnit> values,
			Context c) throws IOException, InterruptedException {

		double overallElecConsumption = 0;
		double overallWaterColdConsumption = 0;
		double overallWaterHotConsumption = 0;
		double overallHeatingConsumption = 0;
		
		Iterator<ResidentialUnit> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {

			ResidentialUnit flat = valuesIt.next();
			if (flat == null)
				continue;
			double overallConsumption = flat.getDeviceAmount();
			String counterType = flat.getCounterType();
			if(counterType.equals("el")){
				overallElecConsumption += overallConsumption;
			} else if(counterType.equals("wc")){
				overallWaterColdConsumption += overallConsumption;
			} else if(counterType.equals("wh")){
				overallWaterHotConsumption += overallConsumption;
			} else if(counterType.equals("he")){
				overallHeatingConsumption += overallConsumption;
			}		
		}

		Mutation m = new Mutation(String.valueOf(key));
		long timestamp = Long.parseLong(String.valueOf(key));
		
		m.put("el", "amount", timestamp,
				String.valueOf(overallElecConsumption));
		m.put("wc", "amount", timestamp,
				String.valueOf(overallWaterColdConsumption));
		m.put("wh", "amount", timestamp,
				String.valueOf(overallWaterHotConsumption));
		m.put("he", "amount", timestamp,
				String.valueOf(overallHeatingConsumption));
		
		String outputTableName = Config.getProperty("outputTableName");
		c.write(new Text(outputTableName), m);
	}
}
