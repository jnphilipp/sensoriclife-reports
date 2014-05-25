package org.sensoriclife.reports.dayWithMaxConsumption.firstJob;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.Config;
import org.sensoriclife.world.ResidentialUnit;

public class DaysWithConsumptionMapper extends
		Mapper<Key, Value, LongWritable, ResidentialUnit> {

	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {
		
		String consumptionID = k.getRow().toString();
		String family = k.getColumnFamily().toString();
		String qualifier = k.getColumnQualifier().toString();
		Long timestamp = k.getTimestamp();
			
		if(family.equals("device") && qualifier.equals("amount"))
		{
			long minTs = Long.parseLong(Config.getProperty("minTimestamp"));
			long maxTs = Long.parseLong(Config.getProperty("maxTimestamp"));
			
			if (timestamp >= minTs && timestamp <= maxTs) {
				String counterType = consumptionID.split("_")[1];
				ResidentialUnit flat = new ResidentialUnit();
				flat.setConsumptionID(consumptionID);
				flat.setTimeStamp(k.getTimestamp());
				flat.setDeviceAmount(Float.parseFloat(v.toString()));
				flat.setCounterType(counterType);
				
				Timestamp ts = new Timestamp(timestamp);
				GregorianCalendar cal = (GregorianCalendar) Calendar.getInstance();
				cal.setTime(ts);
				//emit day of year
				c.write(new LongWritable(cal.get(Calendar.DAY_OF_YEAR)), flat);	
			}
			
		}
	}

}
