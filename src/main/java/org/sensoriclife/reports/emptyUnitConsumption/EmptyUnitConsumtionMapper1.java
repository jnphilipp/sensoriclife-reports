package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import java.util.logging.Level;
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
public class EmptyUnitConsumtionMapper1 extends Mapper <Key,Value,Text,Text>
{
	@Override
	public void map(Key k, Value v, Context c) throws IOException,InterruptedException 
	{
		String consumptionID = k.getRow().toString();
		String family = k.getColumnFamily().toString();
		String qualifier = k.getColumnQualifier().toString();
		Long timestamp = k.getTimestamp();
		
		//get all residential units with user
		if(family.equals("residential") && qualifier.equals("id"))
		{
			try 
			{
				String address = (String)Helpers.toObject(v.get());
				c.write(new Text(consumptionID), new Text(address));
			} 
			catch (ClassNotFoundException ex) 
			{
				Logger.error(EmptyUnitConsumtionMapper1.class, ex.toString());
			}
		}
		//get all residential units
		if(family.equals("user") && qualifier.equals("residential"))
		{
			try 
			{
				String addresses = (String)Helpers.toObject(v.get());
				c.write(new Text(consumptionID), new Text(addresses));
			} 
			catch (ClassNotFoundException ex) 
			{
				Logger.error(EmptyUnitConsumtionMapper1.class, ex.toString());
			}
		}
		//get all amounts 
		if(family.equals("device") && qualifier.equals("amount"))
		{
			try 
			{
				c.write(new Text(consumptionID), new Text(family+"|"+qualifier+"|"+timestamp+"|"+(String)Helpers.toObject(v.get())));
			} 
			catch (ClassNotFoundException ex) 
			{
				java.util.logging.Logger.getLogger(EmptyUnitConsumtionMapper1.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}
}