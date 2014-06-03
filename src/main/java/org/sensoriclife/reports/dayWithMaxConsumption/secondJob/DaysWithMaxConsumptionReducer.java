package org.sensoriclife.reports.dayWithMaxConsumption.secondJob;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Locale;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.Config;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.Consumption;

/**
 * 
 * @author marcel
 * 
 */
public class DaysWithMaxConsumptionReducer extends
		Reducer<IntWritable, Consumption, Text, Mutation> {

	public void reduce(IntWritable key, Iterable<Consumption> values, Context c)
			throws IOException, InterruptedException {

		ArrayList<Consumption> maxElecConsumptions = new ArrayList<Consumption>();
		ArrayList<Consumption> maxWaterColdConsumptions = new ArrayList<Consumption>();
		ArrayList<Consumption> maxWaterHotConsumptions = new ArrayList<Consumption>();
		ArrayList<Consumption> maxHeatingConsumptions = new ArrayList<Consumption>();

		Iterator<Consumption> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {

			Consumption consumption = (Consumption) valuesIt.next();
			if (consumption == null)
				continue;

			double overallAmount = consumption.getAmount();
			try {
				String counterType = consumption.getCounterType();
				if (counterType.equals("el")) {
					if (maxElecConsumptions.isEmpty()
							|| (maxElecConsumptions.isEmpty() == false && overallAmount >= maxElecConsumptions
									.get(0).getAmount())) {
						if (maxElecConsumptions.isEmpty() == false
								&& overallAmount > maxElecConsumptions.get(0)
										.getAmount()) {
							maxElecConsumptions.clear();
						}
						maxElecConsumptions.add((Consumption) Helpers
								.deepCopy(consumption));
					}
				} else if (counterType.equals("wc")) {
					if (maxWaterColdConsumptions.isEmpty()
							|| (maxWaterColdConsumptions.isEmpty() == false && overallAmount >= maxWaterColdConsumptions
									.get(0).getAmount())) {
						if (maxWaterColdConsumptions.isEmpty() == false
								&& overallAmount > maxWaterColdConsumptions
										.get(0).getAmount()) {
							maxWaterColdConsumptions.clear();
						}
						maxWaterColdConsumptions.add((Consumption) Helpers
								.deepCopy(consumption));
					}
				} else if (counterType.equals("wh")) {
					if (maxWaterHotConsumptions.isEmpty()
							|| (maxWaterHotConsumptions.isEmpty() == false && overallAmount >= maxWaterHotConsumptions
									.get(0).getAmount())) {
						if (maxWaterHotConsumptions.isEmpty() == false
								&& overallAmount > maxWaterHotConsumptions.get(
										0).getAmount()) {
							maxWaterHotConsumptions.clear();
						}
						maxWaterHotConsumptions.add((Consumption) Helpers
								.deepCopy(consumption));
					}
				} else if (counterType.equals("he")) {
					if (maxHeatingConsumptions.isEmpty()
							|| (maxHeatingConsumptions.isEmpty() == false && overallAmount >= maxHeatingConsumptions
									.get(0).getAmount())) {
						if (maxHeatingConsumptions.isEmpty() == false
								&& overallAmount > maxHeatingConsumptions
										.get(0).getAmount()) {
							maxHeatingConsumptions.clear();
						}
						maxHeatingConsumptions.add((Consumption) Helpers
								.deepCopy(consumption));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		Mutation m = new Mutation("1");
		for (Consumption cons : maxElecConsumptions) {
			long ts = cons.getTimestamp();
			m.put("el", "date", cons.getTimestamp(), calculateDate(ts));
			m.put("el", "maxAmount", cons.getTimestamp(),
					String.valueOf(cons.getAmount()));
		}

		for (Consumption cons : maxWaterColdConsumptions) {
			long ts = cons.getTimestamp();
			m.put("wc", "date", cons.getTimestamp(), calculateDate(ts));
			m.put("wc", "maxAmount", cons.getTimestamp(),
					String.valueOf(cons.getAmount()));
		}

		for (Consumption cons : maxWaterHotConsumptions) {
			long ts = cons.getTimestamp();
			m.put("wh", "date", cons.getTimestamp(), calculateDate(ts));
			m.put("wh", "maxAmount", cons.getTimestamp(),
					String.valueOf(cons.getAmount()));
		}

		for (Consumption cons : maxHeatingConsumptions) {
			long ts = cons.getTimestamp();
			m.put("he", "date", cons.getTimestamp(), calculateDate(ts));
			m.put("he", "maxAmount", cons.getTimestamp(),
					String.valueOf(cons.getAmount()));
		}

		String outputTableName = Config
				.getProperty("report.dayWithMaxConsumption.outputTableName");
		c.write(new Text(outputTableName), m);
	}

	/**
	 * calculates the date from the timestamp
	 * 
	 * @param dayOfYear
	 * @return String
	 */
	public String calculateDate(long timestamp) {
		Timestamp ts = new Timestamp(timestamp);
		GregorianCalendar cal = (GregorianCalendar) Calendar
				.getInstance(Locale.ENGLISH);
		cal.setTime(ts);

		SimpleDateFormat monthDate = new SimpleDateFormat("MMMMMMMMM");
		String monthName = monthDate.format(cal.getTime());

		return String.valueOf(cal.get(Calendar.DAY_OF_MONTH) + ". " + monthName
				+ " " + cal.get(Calendar.YEAR));

	}
}
