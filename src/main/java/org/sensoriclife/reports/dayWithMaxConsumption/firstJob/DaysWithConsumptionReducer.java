package org.sensoriclife.reports.dayWithMaxConsumption.firstJob;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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

		double overallConsumption = 0;

		Iterator<ResidentialUnit> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {

			ResidentialUnit flat = valuesIt.next();
			if (flat == null)
				continue;
			overallConsumption += flat.getElecConsumption().getAmount();
		}

		Mutation m = new Mutation(String.valueOf(key));
		long timestamp = Long.parseLong(String.valueOf(key));
		// write minimum
		m.put("day", "amount", timestamp,
				String.valueOf(overallConsumption));
		c.write(new Text("DaysWithConsumption"), m);
	}
}
