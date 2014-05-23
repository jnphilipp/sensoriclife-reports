package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumptionReducer1 extends Reducer<Text, Mutation, Text, Mutation>
{
	private static LinkedHashSet<String> list = new LinkedHashSet<String>();
	
	public void reduce(Text key, Mutation values, Context c) throws IOException, InterruptedException 
	{
			Text rowId = new Text(values.getRow().toString());
			List<ColumnUpdate> data = values.getUpdates();
			String all ="";
			for ( ColumnUpdate data1 : data )
				all += data1.toString();

			Logger.info(EmptyUnitConsumptionReducer1.class, values.toString());
			//amounts
			/*if( all.contains("device") && all.contains("amount") )
			{
				c.write(new Text(Config.getProperty("reports.empty_consumption_report.table_name")), values);
			}
			else if( all.contains("residential") && all.contains("id") )//all residential units with user
			{
				list.add(rowId.toString());
			}
			else if( all.contains("user") && all.contains("residential") )//all residential units
			{
				if( !list.contains(rowId.toString()) )
				{
					c.write(new Text(Config.getProperty("reports.empty_consumption_report.table_name")), values);
				}
			}*/
			c.write(new Text(Config.getProperty("reports.empty_consumption_report.table_name")), values);
	}
}