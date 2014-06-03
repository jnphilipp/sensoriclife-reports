package org.sensoriclife.reports.yearInvoiceResidentialUnit;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.util.Helpers;

public class YearInvoiceComputeReducer extends Reducer<Text, FloatWritable, Text, Mutation> {
	
	public void reduce(Text key, Iterable<FloatWritable> values,Context c) 
			throws IOException, InterruptedException {
		
		String counterType = key.toString().split("_")[1];
		
		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		long reportTimestamp = conf.getLong("reportTimestamp", 0);
		
		float price = conf.getFloat(counterType, -1);
		
		if(price > -1)
		{
			float amount = 0;
			
			for(FloatWritable value:values)
			{
				amount += value.get();
			}
			
			price *= amount;
			
			String outputTableName = conf.getStrings("outputTableName","yearConsumption")[0];
			
			Mutation m1 = new Mutation(Helpers.toByteArray(key.toString()));
			if(reportTimestamp != 0)
				m1.put(Helpers.toByteArray("residentialUnit"), Helpers.toByteArray("invoice"),reportTimestamp,Helpers.toByteArray(price));
			else
				m1.put(Helpers.toByteArray("residentialUnit"), Helpers.toByteArray("invoice"),Helpers.toByteArray(price));
			
			c.write(new Text(outputTableName), m1);
		}
	}
}
