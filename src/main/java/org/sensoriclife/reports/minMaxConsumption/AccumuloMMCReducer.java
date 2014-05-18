package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.reports.world.ResidentialUnit;

/**
 * 
 * @author marcel
 * 
 */
public class AccumuloMMCReducer extends
		Reducer<IntWritable, ResidentialUnit, Text, Mutation> {

	public void reduce(IntWritable key, Iterable<ResidentialUnit> values,
			Context c) throws IOException, InterruptedException {

		// TO DO: list of minFlats / maxFlats!!!
		ArrayList<ResidentialUnit> minFlats = new ArrayList<ResidentialUnit>();
		ArrayList<ResidentialUnit> maxFlats = new ArrayList<ResidentialUnit>();

		ResidentialUnit minFlat = null;
		ResidentialUnit maxFlat = null;

		Iterator<ResidentialUnit> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {

			ResidentialUnit flat = valuesIt.next();
			if (flat == null)
				continue;

			double consumption = flat.getElecConsumption().getAmount();

			if ((minFlat != null
					&& consumption < minFlat.getElecConsumption().getAmount())
					|| (minFlat == null)) {
				try {
					minFlat = (ResidentialUnit) flat.deepCopy(flat);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if ((maxFlat != null && consumption > maxFlat.getElecConsumption()
					.getAmount()) || (maxFlat == null)) {
				try {
					maxFlat = (ResidentialUnit) flat.deepCopy(flat);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		if (minFlat == null || maxFlat == null)
			return;

		Mutation m1 = new Mutation(String.valueOf(minFlat.getElectricMeterId()));
		// write minimum
		m1.put("consumptionId", "min",
				new Value(String.valueOf(minFlat.getElectricMeterId())
						.getBytes()));
		m1.put("amount",
				"minAmount",
				new Value(String.valueOf(
						minFlat.getElecConsumption().getAmount()).getBytes()));
		c.write(new Text("MinMax"), m1);

		Mutation m2 = new Mutation(String.valueOf(maxFlat.getElectricMeterId()));
		// write maximum
		m2.put("consumptionId", "max",
				new Value(String.valueOf(maxFlat.getElectricMeterId())
						.getBytes()));
		m2.put("amount",
				"maxAmount",
				new Value(String.valueOf(
						maxFlat.getElecConsumption().getAmount()).getBytes()));

		// create the mutation based on input key and value
		// report in hdfs
		c.write(new Text("MinMax"), m2);
	}
}
