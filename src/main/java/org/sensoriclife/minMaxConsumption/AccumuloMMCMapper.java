package org.sensoriclife.minMaxConsumption;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.reports.world.ResidentialUnit;

public class AccumuloMMCMapper extends Mapper<Key,Value,NullWritable, ResidentialUnit> {
	
    public void map(Key k, Value v, Context c) throws IOException, InterruptedException {
    	
    	double amount = Double.parseDouble(v.toString());
		long timestamp = k.getTimestamp();
		int electricMeterId = Integer.parseInt(k.getRow().toString());
		ResidentialUnit flat = new ResidentialUnit();
		flat.getElecConsumption().setAmount(amount);
		flat.getElecConsumption().setTimestamp(timestamp);
		flat.setElectricMeterId(electricMeterId);
		c.write(NullWritable.get(), flat);
    }

}
