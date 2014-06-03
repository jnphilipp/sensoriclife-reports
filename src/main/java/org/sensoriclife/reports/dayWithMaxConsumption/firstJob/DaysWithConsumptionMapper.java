package org.sensoriclife.reports.dayWithMaxConsumption.firstJob;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.Config;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.ResidentialUnit;

public class DaysWithConsumptionMapper extends Mapper<Key, Value, IntWritable, ResidentialUnit> {
	@Override
	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {
		
		String consumptionID = null;
		String family = null;
		String qualifier = null;
	//try {
			consumptionID = k.getRow().toString();
			family = k.getColumnFamily().toString();
			qualifier = k.getColumnQualifier().toString();
//			consumptionID = (String) Helpers.toObject(k.getRow().toString().getBytes());
//			family = (String) Helpers.toObject(k.getColumnFamily().toString().getBytes());
//			qualifier = (String) Helpers.toObject(k.getColumnQualifier().toString().getBytes());
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}
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
//				try {
//					flat.setDeviceAmount((float) Helpers.toObject(v.toString().getBytes()));
//				} catch (ClassNotFoundException e) {
//					e.printStackTrace();
//				}
				flat.setCounterType(counterType);
				
				Timestamp ts = new Timestamp(timestamp);
				GregorianCalendar cal = (GregorianCalendar) Calendar.getInstance();
				cal.setTime(ts);
				//emit day of year
				c.write(new IntWritable(cal.get(Calendar.DAY_OF_YEAR)), flat);	
			}
			
		}
	}
}
