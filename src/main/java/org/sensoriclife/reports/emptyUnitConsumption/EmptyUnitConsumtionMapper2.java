package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.Logger;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumtionMapper2 extends Mapper <Key,Value,Text,Text>
{
	@Override
	public void map(Key k, Value v, Mapper.Context c) throws IOException,InterruptedException 
	{
		String consumptionID = k.getRow().toString();
		String family = k.getColumnFamily().toString();
		String qualifier = k.getColumnQualifier().toString();
		Long timestamp = k.getTimestamp();
		
		if(family.equals("user") && qualifier.equals("residential"))
		{
			try 
			{
				String address = (String)Helpers.toObject(v.get());
				String addresses[] = address.split(";");
				for(int i=0;i<addresses.length;i++)
				{
					c.write(new Text(consumptionID), new Text(addresses[i]));
				}
			} 
			catch (ClassNotFoundException ex) 
			{
				Logger.error(EmptyUnitConsumtionMapper1.class, ex.toString());
			}
		}
	}
}