package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import java.util.Iterator;
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
	private static LinkedHashSet<String> list = new LinkedHashSet<>();
	
	@Override
	public void reduce(Text key, Iterable<Mutation> values, Context c) throws IOException, InterruptedException 
	{
		Logger.info(EmptyUnitConsumptionReducer1.class, key.toString());
		Iterator<Mutation> mutations = values.iterator();
		while ( mutations.hasNext() ) {
			Mutation m = mutations.next();
			Logger.info(EmptyUnitConsumptionReducer1.class, m.toString());

			Text rowId = key;//new Text(Arrays.toString(m.getRow()));
			List<ColumnUpdate> data = m.getUpdates();
			String all ="";
			for ( ColumnUpdate data1 : data ) {
				//try {
					all += new String(data1.getColumnFamily());//Helpers.toObject(data1.getColumnFamily()).toString();
					all += new String(data1.getColumnQualifier());//Helpers.toObject(data1.getColumnQualifier()).toString();
				/*}
				catch ( IOException | ClassNotFoundException e ) {
					Logger.error(EmptyUnitConsumptionReducer1.class, e.toString());
				}*/
		}

			Logger.info(EmptyUnitConsumptionReducer1.class, all);

			//amounts
			if( all.contains("device") && all.contains("amount") )
			{
				c.write(new Text(Config.getProperty("reports.empty_consumption_report.table_name")), m);
			}
			else if( all.contains("residential") && all.contains("id") )//all residential units with user
			{
				list.add(rowId.toString());
			}
			else if( all.contains("user") && all.contains("residential") )//all residential units
			{
				if( !list.contains(rowId.toString()) )
				{
					c.write(new Text(Config.getProperty("reports.empty_consumption_report.table_name")), m);
				}
			}
		}
	}
}