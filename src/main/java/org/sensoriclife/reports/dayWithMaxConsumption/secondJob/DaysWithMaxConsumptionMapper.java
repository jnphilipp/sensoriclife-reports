package org.sensoriclife.reports.dayWithMaxConsumption.secondJob;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.Consumption;

public class DaysWithMaxConsumptionMapper extends
		Mapper<Key, Value, IntWritable, Consumption> {

	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {

		String family = null;
		try {
			family = (String) Helpers.toObject(k.getColumnFamily().getBytes());
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}

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

		float amount = 0;
		try {
			amount = (float) Helpers.toObject(v.get());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		long timestamp = k.getTimestamp();

		cons.setTimestamp(timestamp);
		cons.setAmount(amount);
		c.write(new IntWritable(1), cons);
	}

}
