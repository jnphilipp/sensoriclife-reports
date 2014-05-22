package org.sensoriclife.reports.unusualRiseOfConsumption.secondob;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.world.ResidentialUnit;

public class UnusualRiseOfHeatingConsumptionMapper extends
		Mapper<Key, Value, Text, ResidentialUnit> {

	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {

		String rowId = k.getRow().toString();
		Long timestamp = k.getTimestamp();

		String family = k.getColumnFamily().toString();
		String qualifier = k.getColumnQualifier().toString();

		if (family.equals("he") && qualifier.equals("consumptionCurrentWeek")) {
			String[] split = rowId.split(";");
			ResidentialUnit flat = new ResidentialUnit();
			flat.setCounterType(family);
			flat.setResidentialID(split[0]);
			flat.setConsumptionID(split[1]);
			flat.setTimeStamp(timestamp);
			//deviceAmount ~~ consumption of last week
			flat.setDeviceAmount(Float.parseFloat(v.toString()));

			c.write(new Text(split[0]), flat);
		}
	}

}
