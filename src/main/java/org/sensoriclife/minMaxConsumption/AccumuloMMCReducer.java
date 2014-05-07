package org.sensoriclife.minMaxConsumption;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.reports.world.ResidentialUnit;

public class AccumuloMMCReducer extends
		Reducer<NullWritable, Writable, Text, Mutation> {

	public void reduce(NullWritable key, Iterable<ResidentialUnit> values, Context c)
			throws IOException, InterruptedException {

		ResidentialUnit minFlat = null;
		ResidentialUnit maxFlat = null;
		
		Iterator<ResidentialUnit> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {
			ResidentialUnit flat = null;
			flat = (ResidentialUnit) valuesIt.next();
			double consumption = flat.getElectricityConsumption();
			if ((minFlat != null && consumption < minFlat
					.getElectricityConsumption()) || minFlat == null)
				minFlat = flat;
			if ((maxFlat != null && consumption > maxFlat
					.getElectricityConsumption()) || maxFlat == null)
				maxFlat = flat;
		}
		// write minimum
		//output.collect(new Text(minFlat.getAddress()), new DoubleWritable(
			//	minFlat.getElectricityConsumption()));
		// write maximum
		//output.collect(new Text(maxFlat.getAddress()), new DoubleWritable(
			///	minFlat.getElectricityConsumption()));
		
		Mutation m = null;
		//m.put(columnFamily, columnQualifier, value);
		
		// create the mutation based on input key and value
		c.write(new Text("minMaxConsumption"), m);
		
		//report as console - output
		System.out.println(minFlat.getAddress() + " " + minFlat.getElectricityConsumption());
		System.out.println(maxFlat.getAddress() + " " + maxFlat.getElectricityConsumption());
	}
}
