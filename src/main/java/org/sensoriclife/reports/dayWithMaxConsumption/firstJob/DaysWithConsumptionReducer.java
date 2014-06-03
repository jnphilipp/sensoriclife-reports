package org.sensoriclife.reports.dayWithMaxConsumption.firstJob;

import java.io.IOException;
import java.util.Iterator;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.Config;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.ResidentialUnit;

/**
 * 
 * @author marcel
 * 
 */
public class DaysWithConsumptionReducer extends
		Reducer<IntWritable, ResidentialUnit, Text, Mutation> {
	@Override
	public void reduce(IntWritable key, Iterable<ResidentialUnit> values,
			Context c) throws IOException, InterruptedException {
		double overallElecConsumption = 0;
		double overallWaterColdConsumption = 0;
		double overallWaterHotConsumption = 0;
		double overallHeatingConsumption = 0;

		ResidentialUnit elecFlat = new ResidentialUnit();
		ResidentialUnit wcFlat = new ResidentialUnit();
		ResidentialUnit whFlat = new ResidentialUnit();
		ResidentialUnit heFlat = new ResidentialUnit();

		Iterator<ResidentialUnit> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {
			ResidentialUnit flat = valuesIt.next();
			if (flat == null)
				continue;
			double overallConsumption = flat.getDeviceAmount();
			String counterType = flat.getCounterType();
			try {
				if (counterType.equals("el")) {
					overallElecConsumption += overallConsumption;
					elecFlat = (ResidentialUnit) Helpers.deepCopy(flat);
				} else if (counterType.equals("wc")) {
					overallWaterColdConsumption += overallConsumption;
					wcFlat = (ResidentialUnit) Helpers.deepCopy(flat);
				} else if (counterType.equals("wh")) {
					overallWaterHotConsumption += overallConsumption;
					whFlat = (ResidentialUnit) Helpers.deepCopy(flat);
				} else if (counterType.equals("he")) {
					overallHeatingConsumption += overallConsumption;
					heFlat = (ResidentialUnit) Helpers.deepCopy(flat);
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		Mutation m = new Mutation(String.valueOf(key));
		m.put("el", "amount", elecFlat.getTimeStamp(),
				String.valueOf(overallElecConsumption));
		m.put("wc", "amount", wcFlat.getTimeStamp(),
				String.valueOf(overallWaterColdConsumption));
		m.put("wh", "amount", whFlat.getTimeStamp(),
				String.valueOf(overallWaterHotConsumption));
		m.put("he", "amount", heFlat.getTimeStamp(),
				String.valueOf(overallHeatingConsumption));
		
//		Mutation m = new Mutation(Helpers.toByteArray(String.valueOf(key)));
//		m.put(Helpers.toByteArray("el"), Helpers.toByteArray("amount"), elecFlat.getTimeStamp(),
//				Helpers.toByteArray(String.valueOf(overallElecConsumption)));
//		m.put(Helpers.toByteArray("wc"), Helpers.toByteArray("amount"), wcFlat.getTimeStamp(),
//				Helpers.toByteArray(String.valueOf(overallWaterColdConsumption)));
//		m.put(Helpers.toByteArray("wh"), Helpers.toByteArray("amount"), whFlat.getTimeStamp(),
//				Helpers.toByteArray(String.valueOf(overallWaterHotConsumption)));
//		m.put(Helpers.toByteArray("he"), Helpers.toByteArray("amount"), heFlat.getTimeStamp(),
//				Helpers.toByteArray(String.valueOf(overallHeatingConsumption)));

		String outputTableName = Config
				.getProperty("report.daysWithConsumption.outputTableName");
		c.write(new Text(outputTableName), m);
	}
}
