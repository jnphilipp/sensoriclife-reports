package org.sensoriclife.minMaxConsumption;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.reports.world.ResidentialUnit;

/**
 * 
 * @author marcel
 *
 */
public class AccumuloMMCReducer extends
		Reducer<NullWritable, ResidentialUnit, Text, Mutation> {

	public void reduce(NullWritable key, Iterable<ResidentialUnit> values,
			Context c) throws IOException, InterruptedException {

		ResidentialUnit minFlat = null;
		ResidentialUnit maxFlat = null;

		Iterator<ResidentialUnit> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {
			ResidentialUnit flat = null;
			flat = (ResidentialUnit) valuesIt.next();
			double consumption = flat.getElecConsumption().getAmount();
			if ((minFlat != null && consumption < minFlat.getElecConsumption()
					.getAmount()) || minFlat == null)
				minFlat = flat;
			if ((maxFlat != null && consumption > maxFlat.getElecConsumption()
					.getAmount()) || maxFlat == null)
				maxFlat = flat;
		}

		Mutation m1 = new Mutation();
		// write minimum
		m1.put("consumptionId",
				"min",
				new Value(String.valueOf(
						minFlat.getElecConsumption().getConsumptionId())
						.getBytes()));
		m1.put("amount",
				"minAmount",
				new Value(String.valueOf(
						minFlat.getElecConsumption().getAmount()).getBytes()));
		
		Mutation m2 = new Mutation();
		// write maximum
		
		m2.put("consumptionId",
				"max",
				new Value(String.valueOf(
						maxFlat.getElecConsumption().getConsumptionId())
						.getBytes()));
		m2.put("amount",
				"maxAmount",
				new Value(String.valueOf(
						maxFlat.getElecConsumption().getAmount()).getBytes()));

		// create the mutation based on input key and value
		// report in hdfs
		c.write(new Text("minMaxConsumption"), m1);
		c.write(new Text("minMaxConsumption"), m2);

		// report as console - output
		System.out.println(minFlat.getAddress() + " "
				+ minFlat.getElecConsumption().getAmount());
		System.out.println(maxFlat.getAddress() + " "
				+ maxFlat.getElecConsumption().getAmount());
	}
}
