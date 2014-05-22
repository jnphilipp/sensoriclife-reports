package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumtionReducer1 extends Reducer<Text, Text, Text, Mutation>
{
	public void reduce(Text key, Text values, Context c) throws IOException, InterruptedException 
	{
		//amounts
		if(values.toString().contains("|"))
		{
			Mutation m = new Mutation(String.valueOf(key));
			//parser
			String amounts[] = values.toString().split("|");//[0]=family | [1]=qualifier | [2]=timestamp | [3]=value
			for(int i=0;i<amounts.length;i++)
			{
				m.put(amounts[0], amounts[1], Long.parseLong(amounts[2]), amounts[3]);
				c.write(new Text("AllAmounts"), m);
			}
		}
		else//residential units
		{
			Mutation m = new Mutation(String.valueOf(key));
			m.put("residential", "id", values.toString());
			c.write(new Text("AllResidentialUnits"), m);
		}
	}
}