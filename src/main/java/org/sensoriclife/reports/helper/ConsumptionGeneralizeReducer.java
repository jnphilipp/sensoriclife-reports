package org.sensoriclife.reports.helper;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.util.Helpers;

public class ConsumptionGeneralizeReducer extends Reducer<Text, FloatWritable, Text, Mutation> {

	public void reduce(Text key, Iterable<FloatWritable> values,Context c) 
			throws IOException, InterruptedException {
		
		int identifier = key.toString().split("_")[0].split("-").length;
		float amount = 0;
		
		for(FloatWritable value:values)
		{
			amount += value.get();
		}
		
		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		long reportTimestamp = conf.getLong("reportTimestamp", 0);
		String outputTableName = conf.getStrings("outputTableName","yearConsumption")[0];
		
		String summaryObjectName = "";
		
		switch (identifier){
			case 0:
				summaryObjectName = "world";
				break;
			case 1:
				summaryObjectName = "city";
				break;
			case 2:
				summaryObjectName = "district";
				break;
			case 3:
				summaryObjectName = "street";
				break;
			case 4:
				summaryObjectName = "building";
				break;
			case 5:
				summaryObjectName = "residentialUnit";
				break;
			default:
				summaryObjectName = "";
				break;
		}
		
		Mutation m1 = new Mutation(Helpers.toByteArray(key.toString()));
		if(reportTimestamp != 0)
			m1.put(Helpers.toByteArray(summaryObjectName), Helpers.toByteArray("amount"),reportTimestamp,Helpers.toByteArray(amount));
		else
			m1.put(Helpers.toByteArray(summaryObjectName), Helpers.toByteArray("amount"),Helpers.toByteArray(amount));
		
		c.write(new Text(outputTableName), m1);
		
	}
}
