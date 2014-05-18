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

		Consumption maxConsumption = null;

		Iterator<Consumption> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {

			Consumption consumption = (Consumption) valuesIt.next();
			if (consumption == null)
				continue;

			double overallAmount = consumption.getAmount();

			if ((maxConsumption != null && overallAmount >= maxConsumption
					.getAmount()) || maxConsumption == null) {
				try {
					maxConsumption = (Consumption) consumption
							.deepCopy(consumption);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		Mutation m = new Mutation("1");
		m.put("DaysWithMaxConsumption", "DayOfYearWithMaxAmount",
				String.valueOf(maxConsumption.getDayOfYear()));
		m.put("DaysWithMaxConsumption", "OverallAmount",
				String.valueOf(maxConsumption.getAmount()));
		c.write(new Text("DaysWithMaxConsumption"), m);
	}
}
