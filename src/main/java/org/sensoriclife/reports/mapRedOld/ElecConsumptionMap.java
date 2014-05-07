package org.sensoriclife.reports.mapRedOld;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sensoriclife.reports.world.ResidentialUnit;

public class ElecConsumptionMap extends MapReduceBase implements
		Mapper<Text, Text, NullWritable, ResidentialUnit> {

	@Override
	public void map(Text key, Text value,
			OutputCollector<NullWritable, ResidentialUnit> output,
			Reporter reporter) throws IOException {
		String line = value.toString();

		String[] split = line.split(";");

		// filter timestamp

		ResidentialUnit flat = new ResidentialUnit();
		// flat.set();
		output.collect(NullWritable.get(), flat);

	}
}
