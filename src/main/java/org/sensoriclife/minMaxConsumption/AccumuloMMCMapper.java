package org.sensoriclife.minMaxConsumption;

import java.util.StringTokenizer;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.reports.world.ResidentialUnit;

public class AccumuloMMCMapper extends Mapper<Key,Value,NullWritable, ResidentialUnit> {
	
    public void map(Key k, Value v, Context c) {
    	String line = v.toString();
    	
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			tokenizer.nextToken();
			//split?
			//filter timestamp

			ResidentialUnit flat = new ResidentialUnit();
			//flat.set();
			//output.collect(NullWritable.get(), flat);
		}
    }

}
