package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
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
		long reportTimestamp = conf.getLong("reportTimestamp", 0);
		String outputTableName = conf.getStrings("outputTableName","yearConsumption")[0];
		
		Mutation m1 = new Mutation(Helpers.toByteArray(key.toString()));
		if(reportTimestamp != 0)
			m1.put(Helpers.toByteArray("building"), Helpers.toByteArray("amount"),new ColumnVisibility(),reportTimestamp,Helpers.toByteArray(amount));
		else
			m1.put(Helpers.toByteArray("building"), Helpers.toByteArray("amount"),Helpers.toByteArray(amount));
		
			
		c.write(new Text(outputTableName), m1);
		
	}
}
