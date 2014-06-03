package org.sensoriclife.reports.consumptionInDaytime;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.util.Helpers;

public class ConsumptionInDaytimeMapper extends Mapper<Key, Value, Text, FloatWritable> {

	private long minTimeStamp;
	private long maxTimeStamp;
	private String district;

	private float[] currentConsumptionTotal = new float[4];
	private float lastRelevantValue;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		minTimeStamp = conf.getLong("minTimestamp", 0);
		maxTimeStamp = conf.getLong("maxTimestamp", 0);
		district = conf.get("district");
	}

	public void map(Key key, Value value, Context context) throws IOException, InterruptedException {

		long timestamp = key.getTimestamp();
		String consumptionId = "";
		String family = "";
		String qualifier = "";
		
		try {
			consumptionId = (String) Helpers.toObject(key.getRow().getBytes());
			family = (String) Helpers.toObject(key.getColumnFamily().getBytes());
			qualifier = (String) Helpers.toObject(key.getColumnQualifier().getBytes());
		} catch (ClassNotFoundException e1) {}
		

		// last row of each consumptionId
		if (family.equals("residential") && qualifier.equals("id")) {
			String val;
			try {
				val = (String) Helpers.toObject(value.get());
				if (val.startsWith(district))
					writeConsumption(context, consumptionId.split("_")[1]);
				currentConsumptionTotal = new float[4];
				return;
			} catch (ClassNotFoundException e) {}
		}

		if (minTimeStamp <= timestamp && timestamp <= maxTimeStamp) {
			if (family.equals("device") && qualifier.equals("amount")) {
				
				Calendar cal = GregorianCalendar.getInstance();
				cal.setTime(new Date(timestamp));				
				if (isNewDayTime(cal)){
					try {
						float currentValue = (Float) Helpers.toObject(value.get());						
						int index = cal.get(Calendar.HOUR_OF_DAY) / 6;
						float dif = lastRelevantValue - currentValue;
						if (dif > 0)
							currentConsumptionTotal[index] = dif;
						lastRelevantValue = currentValue;						
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}								
				}
			}
		}
	}

	private boolean isNewDayTime(Calendar cal) {
		if (cal.get(Calendar.MINUTE) == 0 && cal.get(Calendar.HOUR_OF_DAY) % 6 == 0){
			return true;
		}
		return false;
	}

	private void writeConsumption(Context context, String type) throws IOException, InterruptedException {
		context.write(new Text(type + "_0-6"), new FloatWritable(currentConsumptionTotal[0]));
		context.write(new Text(type + "_6-12"), new FloatWritable(currentConsumptionTotal[1]));
		context.write(new Text(type + "_12-18"), new FloatWritable(currentConsumptionTotal[2]));
		context.write(new Text(type + "_18-24"), new FloatWritable(currentConsumptionTotal[3]));
	}
}
