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
		
		String consumptionID = "";
		String family = "";
		String qualifier = "";
		try {
			consumptionID = (String) Helpers.toObject(k.getRow().getBytes());
			family = (String) Helpers.toObject(k.getColumnFamily().getBytes());
			qualifier = (String) Helpers.toObject(k.getColumnQualifier().getBytes());
		} catch (ClassNotFoundException e1) {}
		
		
		if(family.equals("device") && qualifier.equals("amount"))
		{
			Configuration conf = new Configuration();
			conf = c.getConfiguration();
			long maxTs = conf.getLong("maxTimestamp", Long.MAX_VALUE);
			long minTs = conf.getLong("minTimestamp", 0);
			boolean onlyInTimeRange = conf.getBoolean("onlyInTimeRange", false);
			
			Long timestamp = k.getTimestamp();
			
			if (timestamp > minTs && timestamp <= maxTs) {
				DeviceUnit flat = new DeviceUnit();
				flat.setConsumptionID(consumptionID);
				
				try {
					flat.setDeviceAmount((Float)Helpers.toObject(v.get()));
					c.write(new Text(consumptionID),flat);
				} catch (ClassNotFoundException e) {}
				
				
			}
			else if(!onlyInTimeRange)
			{
				if(timestamp > maxTs)
				{
					DeviceUnit flat = new DeviceUnit();
					flat.setConsumptionID(consumptionID);
					
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
			try {
				flat.setResidentialID((String) Helpers.toObject(v.get()));
				c.write(new Text(consumptionID),flat);
			} catch (ClassNotFoundException e) {}
			
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
