package org.sensoriclife.reports.dayWithMaxConsumption.secondJob;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.world.Consumption;

public class DayWithMaxConsumptionMapper extends
		Mapper<Key, Value, IntWritable, Consumption> {

	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {

		String family = k.getColumnFamily().toString();

		Consumption cons = new Consumption();
		if (family.equals("el")) {
			cons.setCounterType("el");
		} else if (family.equals("wc")) {
			cons.setCounterType("wc");
		} else if (family.equals("wh")) {
			cons.setCounterType("wh");
		} else if (family.equals("he")) {
			cons.setCounterType("he");
		}

		double amount = Double.parseDouble(v.toString());
		long timestamp = k.getTimestamp();

		cons.setTimestamp(timestamp);
		cons.setAmount(amount);
		c.write(new IntWritable(1), cons);
	}

}
