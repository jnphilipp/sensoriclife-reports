package org.sensoriclife.reports.emptyUnitConsumption;

import java.io.IOException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.sensoriclife.Logger;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumptionMapper2 extends Mapper <Key, Value, Text, Mutation>
{
	//private static final ArrayList<Mutation> list = new ArrayList<Mutation>();
	//private static final ArrayList<Key> list2 = new ArrayList<Key>();
	@Override
	public void map(Key k, Value v, Context c) throws IOException,InterruptedException 
	{
		String family = k.getColumnFamily().toString();
		String qualifier = k.getColumnQualifier().toString();
		
		Logger.info(EmptyUnitConsumptionMapper2.class, k.toString());
		
		if(qualifier.contains("inhabited"))
		{
			//list.add(family);
			Mutation m = new Mutation(k.toString());
			m.putDelete(family, "existed");
			//list.add(m);
			//list2.add(k);
			c.write(k.getRow(), m);
			Mutation m2 = new Mutation(k.toString());
			m2.putDelete(family, "amount");
			//list.add(m);
			//list2.add(k);
			c.write(k.getRow(), m2);
		}
		/*else
		{
			Mutation m3 = new Mutation(k.toString());
			m3.put(family, qualifier, v);
			c.write(k.getRow(), m3);
			for(int i=0;i<list.size();i++)
				c.write(list2.get(i).getRow(),list.get(i));
			c.write(null, m3);
		}*/
	}	
}