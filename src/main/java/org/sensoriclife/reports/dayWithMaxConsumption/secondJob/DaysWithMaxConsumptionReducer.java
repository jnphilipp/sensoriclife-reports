package org.sensoriclife.reports.dayWithMaxConsumption.secondJob;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.world.Consumption;

/**
 * 
 * @author marcel
 * 
 */
public class DaysWithMaxConsumptionReducer extends
		Reducer<IntWritable, Consumption, Text, Mutation> {

	public void reduce(IntWritable key, Iterable<Consumption> values, Context c)
			throws IOException, InterruptedException {

		// TO DO: list of days with maximum consumption

		Consumption maxElecConsumption = null;
		Consumption maxWaterColdConsumption = null;
		Consumption maxWaterHotConsumption = null;
		Consumption maxHeatingConsumption = null;

		Iterator<Consumption> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {

			Consumption consumption = (Consumption) valuesIt.next();
			if (consumption == null)
				continue;

			double overallAmount = consumption.getAmount();
			try {
				String counterType = consumption.getCounterType();
				if (counterType.equals("el")) {
					if (maxElecConsumption == null
							|| (overallAmount >= maxElecConsumption.getAmount())) {
						maxElecConsumption = (Consumption) consumption
								.deepCopy(consumption);
					}
				} else if (counterType.equals("wc")) {
					if (maxWaterColdConsumption == null
							|| (overallAmount >= maxWaterColdConsumption
									.getAmount())) {
						maxWaterColdConsumption = (Consumption) consumption
								.deepCopy(consumption);
					}
				} else if (counterType.equals("wh")) {
					if (maxWaterHotConsumption == null
							|| (overallAmount >= maxWaterHotConsumption
									.getAmount())) {
						maxWaterHotConsumption = (Consumption) consumption
								.deepCopy(consumption);
					}
				} else if (counterType.equals("he")) {
					if (maxHeatingConsumption == null
							|| (overallAmount >= maxHeatingConsumption
									.getAmount())) {
						maxHeatingConsumption = (Consumption) consumption
								.deepCopy(consumption);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		Mutation m = new Mutation("1");
		m.put("el", "DayOfYear", maxElecConsumption.getTimestamp(),
				String.valueOf(maxElecConsumption.getDayOfYear()));
		m.put("el", "MaxAmount", maxElecConsumption.getTimestamp(), String.valueOf(maxElecConsumption.getAmount()));

		m.put("wc", "DayOfYear", maxWaterColdConsumption.getTimestamp(),
				String.valueOf(maxWaterColdConsumption.getDayOfYear()));
		m.put("wc", "MaxAmount", maxWaterColdConsumption.getTimestamp(),
				String.valueOf(maxWaterColdConsumption.getAmount()));

		m.put("wh", "DayOfYear", maxWaterHotConsumption.getTimestamp(),
				String.valueOf(maxWaterHotConsumption.getDayOfYear()));
		m.put("wh", "MaxAmount", maxWaterHotConsumption.getTimestamp(),
				String.valueOf(maxWaterHotConsumption.getAmount()));

		m.put("he", "DayOfYear", maxHeatingConsumption.getTimestamp(),
				String.valueOf(maxHeatingConsumption.getDayOfYear()));
		m.put("he", "MaxAmount", maxHeatingConsumption.getTimestamp(),
				String.valueOf(maxHeatingConsumption.getAmount()));

		c.write(new Text("DaysWithMaxConsumption"), m);
	}
}
