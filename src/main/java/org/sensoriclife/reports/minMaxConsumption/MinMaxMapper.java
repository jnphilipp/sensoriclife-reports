package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.ResidentialUnit;

public class MinMaxMapper extends
		Mapper<Key, Value, Text, ResidentialUnit> {

	public void map(Key k, Value v, Context c) throws IOException,
			InterruptedException {
		
		Configuration conf = new Configuration();
		conf = c.getConfiguration();
		int selector = conf.getInt("selectModus", 0);
		
		int rowIDindicator = k.getRow().toString().split("_").length;
		
		if(rowIDindicator == 2)
		{
			int resIDindicator = k.getRow().toString().split("_")[0].split("-").length;
			
			if(resIDindicator == selector)
			{
				String counterType = k.getRow().toString().split("_")[1];
				String resID = k.getRow().toString().split("_")[0];
				
				ResidentialUnit rU = new ResidentialUnit();
				try {
					rU.setDeviceAmount((float) Helpers.toObject(v.get()));
					rU.setResidentialID(resID);
					c.write(new Text(counterType),rU);
				} catch (ClassNotFoundException e) {}
			}
		}
		
	}

}
