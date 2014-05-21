package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.world.ResidentialUnit;

/**
 * 
 * @author marcel, yves
 * 
 */
public class MinMaxReducer extends
		Reducer<Text, ResidentialUnit, Text, Mutation> {

	public void reduce(Text key, Iterable<ResidentialUnit> values, Context c)
			throws IOException, InterruptedException {

		ArrayList<ResidentialUnit> residentials = new ArrayList<ResidentialUnit>();

		float min = Float.MAX_VALUE;
		String minConID = "";
		String minResID = "";
		long minTimeStamp = 0;
		float max = Float.MIN_VALUE;
		String maxConID = "";
		String maxResID = "";
		long maxTimeStamp = 0;

		for (ResidentialUnit value : values) {
			if (value.isSetDeviceAmount()) {
				if (value.getDeviceAmount() < min) {
					minConID = value.getConsumptionID();
					min = value.getDeviceAmount();
					minTimeStamp = value.getTimeStamp();
				}

				if (value.getDeviceAmount() > max) {
					maxConID = value.getConsumptionID();
					max = value.getDeviceAmount();
					maxTimeStamp = value.getTimeStamp();
				}
			}
			if (value.isSetResidentialID()) {
				ResidentialUnit res = new ResidentialUnit();
				res.setConsumptionID(value.getConsumptionID());
				res.setResidentialID(value.getResidentialID());
				residentials.add(res);
			}
		}

		for (ResidentialUnit value : residentials) {
			if (value.getConsumptionID().equals(minConID))
				minResID = value.getResidentialID();

			if (value.getConsumptionID().equals(maxConID))
				maxResID = value.getResidentialID();
		}

		Mutation m1 = new Mutation(key);

		// write minimum/maximum
		m1.put("min", "residentialID", minTimeStamp,
				new Value(minResID.getBytes()));
		m1.put("min", "amount", minTimeStamp, new Value(new Float(min)
				.toString().getBytes()));
		m1.put("max", "residentialID", maxTimeStamp,
				new Value(maxResID.getBytes()));
		m1.put("max", "amount", maxTimeStamp, new Value(new Float(max)
				.toString().getBytes()));

		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		String outputTableName = conf.getStrings("outputTableName",
				"MinMaxTable")[0];

		c.write(new Text(outputTableName), m1);

	}
}
