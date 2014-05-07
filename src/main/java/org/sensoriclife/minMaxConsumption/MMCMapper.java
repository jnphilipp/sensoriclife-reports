package org.sensoriclife.minMaxConsumption;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;


public class MMCMapper extends Mapper<Text, Text, NullWritable, ResidentialUnit>{
	
	/**
	 * default database configuration file
	 */
	
	public void map(Text key, Text value,OutputCollector<NullWritable, ResidentialUnit> output,
			Reporter reporter) throws IOException {
		
	    
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			tokenizer.nextToken();
			//split?
			//filter timestamp

			ResidentialUnit flat = new ResidentialUnit();
			//flat.set();
			output.collect(NullWritable.get(), flat);
		}

	}
}
