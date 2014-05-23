package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumptionMapper1 extends Mapper <Key,Value,Text,Mutation>
{
	@Override
	public void map(Key k, Value v, Context c) throws IOException,InterruptedException 
	{
		String consumptionID = k.getRow().toString();
		String family = k.getColumnFamily().toString();
		String qualifier = k.getColumnQualifier().toString();
		Long timestamp = k.getTimestamp();
		Mutation m = new Mutation(k.toString());
		//get all amounts 
		if(family.equals("device") && qualifier.equals("amount"))
		{
			m.put(family, qualifier, v);
			c.write(k.getRow(), m);
		}
		//get all residential units with user
		if(family.equals("residential") && qualifier.equals("id"))
		{
			m.put(family, qualifier, v);
			c.write(k.getRow(), m);
		}
		//get all residential units
		if(family.equals("user") && qualifier.equals("residential"))
		{
			m.put(family, qualifier, v);
			c.write(k.getRow(), m);
		}
	}
}