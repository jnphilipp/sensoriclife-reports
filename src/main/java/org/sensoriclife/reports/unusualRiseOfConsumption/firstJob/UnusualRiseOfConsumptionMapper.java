package org.sensoriclife.reports.unusualRiseOfConsumption.firstJob;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.world.ResidentialUnit;

public class UnusualRiseOfConsumptionMapper extends
		Mapper<Key, Value, Text, ResidentialUnit> {

	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {
		
		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		
		//intervall should be a week
		long minTs = conf.getLong("minTimestamp", 0);
		long maxTs = conf.getLong("maxTimestamp", Long.MAX_VALUE);
		
		String rowId = k.getRow().toString();
		Long timestamp = k.getTimestamp();
		
		if (timestamp == minTs || timestamp == maxTs) {
			String family = k.getColumnFamily().toString();
			String qualifier = k.getColumnQualifier().toString();

			if (family.equals("device") && qualifier.equals("amount")) {
				String counterType = rowId.split("_")[1];
				ResidentialUnit flat = new ResidentialUnit();
				flat.setConsumptionID(rowId);
				flat.setTimeStamp(timestamp);
				flat.setDeviceAmount(Float.parseFloat(v.toString()));
				flat.setCounterType(counterType);

				c.write(new Text(rowId), flat);
			}
			else if(family.equals("residential") && qualifier.equals("id"))
			{
				String counterType = rowId.split("_")[1];
				ResidentialUnit flat = new ResidentialUnit();
				flat.setTimeStamp(timestamp);
				flat.setConsumptionID(rowId);
				flat.setCounterType(counterType);
				flat.setResidentialID(v.toString());
				c.write(new Text(rowId), flat);
			}	
		}
	}

}
