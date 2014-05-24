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
public class EmptyUnitConsumptionReducer2 extends Reducer<Text, Mutation, Text, Mutation>
{
	@Override
	public void reduce(Text key, Iterable<Mutation> values, Context c) throws IOException, InterruptedException 
	{
		Logger.info(EmptyUnitConsumptionReducer2.class, key.toString());
		Iterator<Mutation> mutations = values.iterator();
		while ( mutations.hasNext() ) 
		{
			Mutation m = mutations.next();
			Logger.info(EmptyUnitConsumptionReducer2.class, m.toString());

			c.write(new Text(Config.getProperty("reports.empty_consumption_report.table_name")), m);
		}
	}
}