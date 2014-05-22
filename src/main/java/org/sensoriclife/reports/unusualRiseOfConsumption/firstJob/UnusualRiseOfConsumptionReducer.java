package org.sensoriclife.reports.unusualRiseOfConsumption.firstJob;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.world.ResidentialUnit;

/**
 * 
 * @author marcel
 * 
 */
public class UnusualRiseOfConsumptionReducer extends
		Reducer<Text, ResidentialUnit, Text, Mutation> {

	public void reduce(Text key, Iterable<ResidentialUnit> values, Context c)
			throws IOException, InterruptedException {

		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		long maxTs = conf.getLong("maxTimestamp", Long.MAX_VALUE);
		
		double oldAmount = 0;
		double currentAmount = 0;

		ResidentialUnit flat = null;
		ResidentialUnit helperFlat = null;

		Iterator<ResidentialUnit> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {

			flat = valuesIt.next();
			if (flat == null)
				continue;

			long timestamp = flat.getTimeStamp();
			if (timestamp == maxTs) {
				if (flat.getDeviceAmount() != -1) {
					currentAmount = flat.getDeviceAmount();
				}
			} else { // timestamp == minTs
				if (flat.getDeviceAmount() != -1) {
					oldAmount = flat.getDeviceAmount();
				}
			}
			if(helperFlat == null && flat.getConsumptionID() != null && !flat.getResidentialID().equals("")){
				try {
					helperFlat = (ResidentialUnit) flat.deepCopy(flat);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		double currentConsumption = currentAmount - oldAmount;

		// start individual assumptions for "unusual rise"
		double riseOfConsumption = 0;
		if (oldAmount != 0) {
			riseOfConsumption = currentConsumption / oldAmount - 1;
		}
		

		Mutation m = null;
		String counterType = flat.getCounterType();

		if(!counterType.equals("he")){
			m = new Mutation(helperFlat.getResidentialID());
			m.put(helperFlat.getCounterType(), "consumptionCurrentWeek", maxTs,
					String.valueOf(currentConsumption));
		}
		
		if (counterType.equals("el")) {
			// more than 200% rise when consumption of last week is at least 200
			// kw/h or current consumption is higher than 2500 kw/h
			if ((oldAmount > 200 && riseOfConsumption >= 2)
					|| currentConsumption > 2500) {
				c.write(new Text("UnusualRiseOfConsumption"), m);		
			}
		} else if (counterType.equals("wc")) {
			// more than 300% rise when consumption of last week is at least 350
			// l/week or current consumption is higher than 7000 l/week
			if ((oldAmount > 350 && riseOfConsumption >= 3)
					|| currentConsumption > 7000) {
				c.write(new Text("UnusualRiseOfConsumption"), m);
			}
		} else if (counterType.equals("wh")) {
			if ((oldAmount > 150 && riseOfConsumption >= 3)
					|| currentConsumption > 3000) {
				c.write(new Text("UnusualRiseOfConsumption"), m);
			}
		} else if (counterType.equals("he")) {
			//second map reduce job is necessary
			m = new Mutation(helperFlat.getResidentialID() + ";" + key);
			m.put(helperFlat.getCounterType(), "consumptionCurrentWeek", maxTs,
					String.valueOf(currentConsumption));
			c.write(new Text("HeatingConsumption"), m);
		}
	}
}
