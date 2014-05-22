package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.util.Helpers;

public class ConsumptionGeneralizeToBuildingReducer extends Reducer<Text, FloatWritable, Text, Mutation> {

	public void reduce(Text key, Iterable<FloatWritable> values,Context c) 
			throws IOException, InterruptedException {
		
		float amount = 0;
		
		for(FloatWritable value:values)
		{
			amount += value.get();
		}
		
		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		String outputTableName = conf.getStrings("outputTableName","yearConsumption")[0];
		
		Mutation m1 = new Mutation(key);
		m1.put("building", "amount",new Value( Helpers.toByteArray(amount)));
		
		c.write(new Text(outputTableName), m1);
		
	}
}
