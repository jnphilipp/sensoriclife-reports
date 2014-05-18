package org.sensoriclife.reports.dayWithMaxConsumption.firstJob;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.world.ResidentialUnit;

public class DaysWithConsumptionMapper extends
		Mapper<Key, Value, LongWritable, ResidentialUnit> {

	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {

		double amount = Double.parseDouble(v.toString());
		long timestamp = k.getTimestamp();
		// filter period for minMaxConsumption of electricity
		if (timestamp >= Long.parseLong(/*App.getProperty("minTimeStamp")*/"1")
				&& timestamp <= Long.parseLong(/*App.getProperty("maxTimeStamp")*/"40")) {
			String electricMeterId = k.getRow().toString();
			ResidentialUnit flat = new ResidentialUnit();
			flat.getElecConsumption().setAmount(amount);
			flat.getElecConsumption().setTimestamp(timestamp);
			flat.setElectricMeterId(electricMeterId);
			
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
