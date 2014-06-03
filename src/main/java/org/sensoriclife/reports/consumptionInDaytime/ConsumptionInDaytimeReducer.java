package org.sensoriclife.reports.consumptionInDaytime;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConsumptionInDaytimeReducer extends Reducer<Text, FloatWritable, Text, Mutation> {

	public void reduce(Text key, Iterable<FloatWritable> values, Context c) throws IOException, InterruptedException {

		long reportTimeStamp = c.getConfiguration().getLong("reportTimestamp", 0);
		String outputTable = c.getConfiguration().get("outputTableName");
		
		float result = 0;		
		for (FloatWritable value : values) {
			result += value.get();
		}		
		
		Mutation mutation = new Mutation(String.valueOf(key).getBytes());			
		mutation.put("result".getBytes(), "value".getBytes(), reportTimeStamp, String.valueOf(result).getBytes());		
		c.write(new Text(outputTable), mutation);		
	}
}
