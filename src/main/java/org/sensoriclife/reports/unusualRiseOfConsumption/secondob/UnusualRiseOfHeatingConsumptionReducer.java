package org.sensoriclife.reports.unusualRiseOfConsumption.secondob;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.world.Consumption;
import org.sensoriclife.world.ResidentialUnit;

/**
 * 
 * @author marcel
 * 
 */
public class UnusualRiseOfHeatingConsumptionReducer extends
		Reducer<Text, ResidentialUnit, Text, Mutation> {

	public void reduce(Text key, Iterable<ResidentialUnit> values, Context c)
			throws IOException, InterruptedException {

		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		long maxTs = conf.getLong("maxTimestamp", Long.MAX_VALUE);
		
		double currentConsumption = 0;
		double overallAmountHeatingsCurrent = 0;

		ResidentialUnit flat = null;

		Iterator<ResidentialUnit> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {

			flat = valuesIt.next();
			if (flat == null)
				continue;
			
			long timestamp = flat.getTimeStamp();
			if (timestamp == maxTs) {
				if (flat.getDeviceAmount() != -1) {
					currentConsumption = flat.getDeviceAmount();
				}
			} 
			
			overallAmountHeatingsCurrent += currentConsumption;			
		}

		Mutation m = new Mutation(String.valueOf(flat.getResidentialID()));
		m.put(flat.getCounterType(), "consumptionCurrentWeek", maxTs,
				String.valueOf(overallAmountHeatingsCurrent));
		
		String counterType = flat.getCounterType();
		if (counterType.equals("he")) {
			if(overallAmountHeatingsCurrent > 500){
				c.write(new Text("UnusualRiseOfConsumption"), m);
			}			
		}
	}
}
