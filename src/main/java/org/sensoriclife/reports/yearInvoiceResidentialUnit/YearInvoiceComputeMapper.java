package org.sensoriclife.reports.yearInvoiceResidentialUnit;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.util.Helpers;

public class YearInvoiceComputeMapper extends Mapper<Key, Value, Text, FloatWritable> {
	
public void map(Key k, Value v, Context c) throws IOException, InterruptedException {
		
		String[] splitRowID = k.getRow().toString().split("_");
		String reduceKey = "";
		
		if(splitRowID.length == 3)
		{
			reduceKey = splitRowID[0]+"_"+splitRowID[2];
			
			try {
				c.write(new Text(reduceKey),new FloatWritable((Float) Helpers.toObject(v.get())));
			} catch (ClassNotFoundException e) {}
		}
	}

}
