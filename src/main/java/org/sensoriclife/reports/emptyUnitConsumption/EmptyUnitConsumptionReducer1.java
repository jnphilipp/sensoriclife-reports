package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import java.util.Iterator;
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
	//private static LinkedHashSet<String> list = new LinkedHashSet<>();
	

	@Override
	public void reduce(Text key, Iterable<Mutation> values, Context c) throws IOException, InterruptedException 
	{
		Logger.info(EmptyUnitConsumptionReducer1.class, key.toString());
		Iterator<Mutation> mutations = values.iterator();
		while ( mutations.hasNext() ) 
		{
			Mutation m = mutations.next();
			Logger.info(EmptyUnitConsumptionReducer1.class, m.toString());

			/*Text rowId = key;
			List<ColumnUpdate> data = m.getUpdates();
			String all ="";
			for ( ColumnUpdate data1 : data ) 
			{
					all += new String(data1.getColumnFamily());
					all += new String(data1.getColumnQualifier());
					//all += data1.getTimestamp();
			}

			Logger.info(EmptyUnitConsumptionReducer1.class, all);

			//amounts
			if( all.contains("device") && all.contains("amount") )
			{
				c.write(new Text(Config.getProperty("reports.empty_consumption_report.table_name")), m);
			}
			else//residential units
			{*/
				c.write(new Text(Config.getProperty("reports.empty_consumption_report1.table_name")), m);
			//}
		}
	}
}