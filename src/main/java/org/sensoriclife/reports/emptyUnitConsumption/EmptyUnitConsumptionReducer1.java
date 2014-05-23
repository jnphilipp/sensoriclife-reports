package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import java.util.LinkedHashSet;
import org.apache.accumulo.core.data.Mutation;import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.Config;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumptionReducer1 extends Reducer<Text, Mutation, Text, Mutation>
{
	private static LinkedHashSet<Mutation> list = new LinkedHashSet<Mutation>();
	
	public void reduce(Text key, Mutation values, Context c) throws IOException, InterruptedException 
	{
			Text text = new Text(values.getRow().toString());
			//amounts
			if( text.toString().contains("device") && text.toString().contains("amount") )
			{
				c.write(new Text(Config.getProperty("reports.empty_consumption_report.table_name")), values);
			}
			else if( text.toString().contains("residential") && text.toString().contains("id") )//all residential units with user
			{
				list.add(values);
			}
			else if( text.toString().contains("user") && text.toString().contains("residential") )//all residential units
			{
				if( !list.contains(values) )
				{
					c.write(new Text(Config.getProperty("reports.empty_consumption_report.table_name")), values);
				}
			}
	}
}