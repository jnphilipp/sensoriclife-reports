package org.sensoriclife.reports.convert;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.ResidentialUnit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ConvertMapper extends Mapper<Key, Value, Text, ResidentialUnit> {

	public void map(Key k, Value v, Context c) throws IOException, InterruptedException {
		
		String consumptionID = k.getRow().toString();
		String family = k.getColumnFamily().toString();
		String qualifier = k.getColumnQualifier().toString();
			
		if(family.equals("device") && qualifier.equals("amount"))
		{
			Configuration conf = new Configuration();
			conf = c.getConfiguration();
			long maxTs = conf.getLong("maxTimestamp", Long.MAX_VALUE);
			long minTs = conf.getLong("minTimestamp", 0);
			
			Long timestamp = k.getTimestamp();
			
			if (timestamp >= minTs && timestamp <= maxTs) {
				ResidentialUnit flat = new ResidentialUnit();
				flat.setConsumptionID(consumptionID);
				flat.setTimeStamp(k.getTimestamp());
				try {
					flat.setDeviceAmount((Float)Helpers.toObject(v.get()));
					c.write(new Text(consumptionID),flat);
				} catch (ClassNotFoundException e) {}
				
				
			}
		}
		
		else if(family.equals("residential") && qualifier.equals("id"))
		{
			ResidentialUnit flat = new ResidentialUnit();
			flat.setConsumptionID(consumptionID);
			flat.setResidentialID(v.toString());
			c.write(new Text(consumptionID),flat);
		}	
	}
}
