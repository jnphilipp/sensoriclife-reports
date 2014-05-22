package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumtionReducer2 extends Reducer<Text, Text, Text, Mutation>
{
	public void reduce(Text key, Text values, Context c) throws IOException, InterruptedException 
	{
		//delete all incoming adresses from accumulo database
	}
}