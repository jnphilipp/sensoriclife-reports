package org.sensoriclife.reports.emptyResidentialUnitsConsumption;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.DeviceUnit;

public class ConvertWithoutUserMapper extends Mapper<Key, Value, Text, DeviceUnit> {

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
			boolean onlyYear = conf.getBoolean("onlyYear", false);
			long pastLimitation = conf.getLong("pastLimitation",Long.MIN_VALUE);
			
			Long timestamp = k.getTimestamp();
			
			if (timestamp >= minTs && timestamp < maxTs) {
				DeviceUnit flat = new DeviceUnit();
				flat.setConsumptionID(consumptionID);
				//flat.setTimeStamp(k.getTimestamp());
				
				
				try {
					flat.setDeviceAmount((Float)Helpers.toObject(v.get()));
					c.write(new Text(consumptionID),flat);
				} catch (ClassNotFoundException e) {}
				
				
			}
			else if(!onlyYear)
			{
				if(timestamp < minTs && timestamp >= pastLimitation)
				{
					DeviceUnit flat = new DeviceUnit();
					flat.setConsumptionID(consumptionID);
					//flat.setTimeStamp(k.getTimestamp());
					
					try {
						flat.setDeviceSecontAmount((Float)Helpers.toObject(v.get()));
						c.write(new Text(consumptionID),flat);
					} catch (ClassNotFoundException e) {}
				}
			}
		}
		
		else if(family.equals("residential") && qualifier.equals("id"))
		{
			DeviceUnit flat = new DeviceUnit();
			flat.setConsumptionID(consumptionID);
			flat.setResidentialID(v.toString());
			c.write(new Text(consumptionID),flat);
		}
		else if(family.equals("user") && qualifier.equals("id"))
		{
			DeviceUnit flat = new DeviceUnit();
			flat.setConsumptionID(consumptionID);
			flat.setUserID(1); //User exist (the ID is not importent)
			c.write(new Text(consumptionID),flat);
		}
	}
}
