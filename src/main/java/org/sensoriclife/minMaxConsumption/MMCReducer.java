package org.sensoriclife.minMaxConsumption;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MMCReducer extends Reducer<NullWritable, ResidentialUnit, Text, DoubleWritable> {

	
	public void reduce(NullWritable key, Iterator<ResidentialUnit> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		
		ResidentialUnit minFlat = null;
		ResidentialUnit maxFlat = null;
		
		
		while (values.hasNext()) {
			ResidentialUnit flat = null;
			flat = values.next();
			double consumption = flat.getElectricityConsumption();
			if ((minFlat != null && consumption < minFlat
					.getElectricityConsumption()) || minFlat == null)
				minFlat = flat;
			if ((maxFlat != null && consumption > maxFlat
					.getElectricityConsumption()) || maxFlat == null)
				maxFlat = flat;
		}
		// write minimum
		output.collect(new Text(minFlat.getAddress()), new DoubleWritable(
				minFlat.getElectricityConsumption()));
		// write maximum
		output.collect(new Text(maxFlat.getAddress()), new DoubleWritable(
				minFlat.getElectricityConsumption()));
	}
}
