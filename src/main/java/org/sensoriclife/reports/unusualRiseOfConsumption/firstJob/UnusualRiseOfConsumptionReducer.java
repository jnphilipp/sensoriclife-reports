package org.sensoriclife.reports.unusualRiseOfConsumption.firstJob;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.Config;
import org.sensoriclife.util.Helpers;
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

		long maxTs = Long.parseLong(Config.getProperty("maxTimestamp"));
		long minTs = Long.parseLong(Config.getProperty("minTimestamp"));

		//contains the minimum device amount
		ResidentialUnit minFlat = new ResidentialUnit();
		//contains the maximum device amount
		ResidentialUnit maxFlat = new ResidentialUnit();
		
		ResidentialUnit flat = null;
		ResidentialUnit helperFlat = null;

		Iterator<ResidentialUnit> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {

			flat = valuesIt.next();
			if (flat == null)
				continue;

			try{	
				long timestamp = flat.getTimeStamp();
				if(minFlat.getDeviceAmount() == -1 && flat.getDeviceAmount() != -1 || (timestamp < minFlat.getTimeStamp() && minFlat.getDeviceAmount() != -1)){
					minFlat = (ResidentialUnit) Helpers.deepCopy(flat);
				}
				if((maxFlat.getDeviceAmount() == -1 && flat.getDeviceAmount() != -1) || (timestamp > maxFlat.getTimeStamp() && maxFlat.getDeviceAmount() != -1)){
					maxFlat = (ResidentialUnit) Helpers.deepCopy(flat);
				}
			}
			catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			
			if(helperFlat == null && flat.getConsumptionID() != null && !flat.getResidentialID().equals("")){
				try {
					helperFlat = (ResidentialUnit) Helpers.deepCopy(flat);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		double currentConsumption = maxFlat.getDeviceAmount() - minFlat.getDeviceAmount();

		// start individual assumptions for "unusual rise"
		double riseOfConsumption = 0;
		if (minFlat.getDeviceAmount() != 0) {
			riseOfConsumption = currentConsumption / minFlat.getDeviceAmount() - 1;
		}

		Mutation m = null;
		String counterType = flat.getCounterType();

		if(!counterType.equals("he")){
			m = new Mutation(helperFlat.getResidentialID());
			m.put(helperFlat.getCounterType(), "consumptionCurrentWeek", maxTs,
					String.valueOf(currentConsumption));
		}
		
		String outputTableName = Config.getProperty("outputTableName");
		String helperOutputTableName = Config.getProperty("helperOutputTableName");
		
		if (counterType.equals("el")) {
			// more than 200% rise when consumption of last week is at least 200
			// kw/h or current consumption is higher than 2500 kw/h
			if ((minFlat.getDeviceAmount() > 200 && riseOfConsumption >= 2)
					|| currentConsumption > 2500) {
				c.write(new Text(outputTableName), m);		
			}
		} else if (counterType.equals("wc")) {
			// more than 300% rise when consumption of last week is at least 350
			// l/week or current consumption is higher than 7000 l/week
			if ((minFlat.getDeviceAmount() > 350 && riseOfConsumption >= 3)
					|| currentConsumption > 7000) {
				c.write(new Text(outputTableName), m);
			}
		} else if (counterType.equals("wh")) {
			if ((minFlat.getDeviceAmount() > 150 && riseOfConsumption >= 3)
					|| currentConsumption > 3000) {
				c.write(new Text(outputTableName), m);
			}
		} else if (counterType.equals("he")) {
			//second map reduce job is necessary
			m = new Mutation(helperFlat.getResidentialID() + ";" + key);
			m.put(helperFlat.getCounterType(), "consumptionCurrentWeek", maxTs,
					String.valueOf(currentConsumption));
			c.write(new Text(helperOutputTableName), m);
		}
	}
}
