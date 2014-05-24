package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.Logger;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumptionMapper2 extends Mapper <Key, Value, Text, Mutation>
{
	private static ArrayList<String> list = new ArrayList<String>(); 
	@Override
	public void map(Key k, Value v, Context c) throws IOException,InterruptedException 
	{
		String consumptionID = k.getRow().toString();
		String family = k.getColumnFamily().toString();
		String qualifier = k.getColumnQualifier().toString();
		String value = v.toString();
		
		Mutation m = new Mutation(k.toString());
		m.put(family, qualifier, v);
		c.write(k.getRow(), m);
		
		Logger.info(EmptyUnitConsumptionMapper2.class, k.toString());
	}	
}