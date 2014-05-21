package org.sensoriclife.reports.dayWithMaxConsumption.firstJob;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.world.ResidentialUnit;

public class DaysWithConsumptionMapper extends
		Mapper<Key, Value, LongWritable, ResidentialUnit> {

	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {

		//new year has begun
		//filter all records with year - 1 and emit their amount.
		
		String consumptionID = k.getRow().toString();
		String family = k.getColumnFamily().toString();
		String qualifier = k.getColumnQualifier().toString();
		Long timestamp = k.getTimestamp();
			
		if(family.equals("device") && qualifier.equals("amount"))
		{
			Configuration conf = new Configuration();
			conf = c.getConfiguration();
			long maxTs = conf.getLong("maxTimestamp", Long.MAX_VALUE);
			long minTs = conf.getLong("minTimestamp", 0);
			
			if (timestamp >= minTs && timestamp <= maxTs) {
				String counterType = consumptionID.split("_")[1];
				ResidentialUnit flat = new ResidentialUnit();
				flat.setConsumptionID(consumptionID);
				flat.setTimeStamp(k.getTimestamp());
				flat.setDeviceAmount(Float.parseFloat(v.toString()));
				flat.setCounterType(counterType);
				
				/*
				Timestamp ts = new Timestamp(timestamp);
				GregorianCalendar cal = (GregorianCalendar) Calendar.getInstance();
				cal.setTime(ts);
				//emit day of year
				c.write(new LongWritable(cal.get(Calendar.DAY_OF_YEAR)), flat);
				*/
				
				c.write(new LongWritable(timestamp), flat);
			}
			
		}
	}

}
