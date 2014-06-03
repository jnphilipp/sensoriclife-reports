package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.DeviceUnit;

/**
 * 
 * @author marcel, yves
 *
 */
public class MinMaxMapper extends
		Mapper<Key, Value, Text, DeviceUnit> {

	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {
		
		String rowID = "";
		try {
			rowID = (String) Helpers.toObject(k.getRow().getBytes());
		} catch (ClassNotFoundException e1) {}
		
		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		int selector = conf.getInt("selectModus", 0);
		long timestamp = k.getTimestamp();
		long reportTimestamp = conf.getLong("reportTimestamp", 0);
		
		if((timestamp == reportTimestamp)|| (reportTimestamp == 0))
		{
			int rowIDindicator = rowID.split("_").length;
			
			if(rowIDindicator == 2)
			{
				int resIDindicator = rowID.split("_")[0].split("-").length;
				
				if(resIDindicator == selector)
				{
					String counterType = rowID.split("_")[1];
					String resID = rowID.split("_")[0];
					
					DeviceUnit rU = new DeviceUnit();
					try {
						rU.setDeviceAmount((float) Helpers.toObject(v.get()));
						rU.setResidentialID(resID);
						c.write(new Text(counterType),rU);
					} catch (ClassNotFoundException e) {}
				}
			}
		}
	}
}
