package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import java.util.LinkedHashSet;
import org.apache.accumulo.core.data.Mutation;import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumtionReducer1 extends Reducer<Text, Text, Text, Mutation>
{
	private static LinkedHashSet<String> list = new LinkedHashSet<String>();
	
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
		else if(values.toString().contains(";"))//all residential units with user
		{
				String addresses[] = values.toString().split(";");
				for(int i=0;i<addresses.length;i++)
				{
					list.add(addresses[i]);
				}
		}
		else//all residential units
		{
			if( !list.contains(values.toString()) )
			{
				Mutation m = new Mutation(String.valueOf(key));
				m.put("residential", "id", values.toString());
				c.write(new Text("AllEmptyResidentialUnits"), m);
			}
		}
	}
}