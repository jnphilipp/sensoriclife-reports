package org.sensoriclife.reports.dayWithMaxConsumption.secondJob;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.world.Consumption;

public class DaysWithMaxConsumptionMapper extends
		Mapper<Key, Value, IntWritable, Consumption> {

	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {

		double amount = Double.parseDouble(v.toString());
		int dayOfYear = Integer.parseInt(String.valueOf(k.getTimestamp()));
		Consumption cons = new Consumption();
		cons.setDayOfYear(dayOfYear);
		cons.setAmount(amount);
		c.write(new IntWritable(1), cons);
	}

}
