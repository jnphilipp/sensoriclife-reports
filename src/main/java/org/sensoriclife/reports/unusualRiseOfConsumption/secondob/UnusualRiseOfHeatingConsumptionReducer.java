package org.sensoriclife.reports.unusualRiseOfConsumption.secondob;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.Config;
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

		long maxTs = Long.parseLong(Config.getProperty("maxTimestamp"));
		
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
		
		String outputTableName = Config.getProperty("report.unusualRiseOfHeatingConsumption.outputTableName");
		
		String counterType = flat.getCounterType();
		if (counterType.equals("he")) {
			if(overallAmountHeatingsCurrent > 500){
				c.write(new Text(outputTableName), m);
			}			
		}
	}
}
