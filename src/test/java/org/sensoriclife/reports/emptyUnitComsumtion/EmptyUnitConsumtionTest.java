package org.sensoriclife.reports.emptyUnitComsumtion;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Test;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.emptyUnitConsumption.EmptyUnitConsumptionReport;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumtionTest 
{
	@Test
	public void mainTest() throws IOException, AccumuloException, AccumuloSecurityException, TableExistsException, MutationsRejectedException, TableNotFoundException, InterruptedException, Exception
	{
		Logger.getInstance();

		//create mock accumulo 
		Config.getInstance().getProperties().setProperty("accumulo.name", "mockInstance");
		Config.getInstance().getProperties().setProperty("accumulo.table_name", "table");
		//Config.getInstance().getProperties().setProperty("accumulo.user", "");
		Config.getInstance().getProperties().setProperty("reports.empty_consumption_report.table_name", "report_empty_report");
		Accumulo.getInstance().connect(Config.getProperty("accumulo.name"));
		Accumulo.getInstance().createTable(Config.getProperty("accumulo.table_name"), false);
		Accumulo.getInstance().createTable(Config.getProperty("reports.empty_consumption_report.table_name"), false);

		//unit with user
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo.table_name"), "0_el", "device", "amount", Helpers.toByteArray(0.0f));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo.table_name"), "0_el", "residential", "id", Helpers.toByteArray("1-1-1-1-1"));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo.table_name"), "0_el", "user", "id", ArrayUtils.addAll(Helpers.toByteArray(5L), Helpers.toByteArray("Nati")));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo.table_name"), "0_el", "user", "residential", Helpers.toByteArray("1-1-1-1-1"));
		//empty unit wit comsumtion
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo.table_name"), "1_el", "device", "amount", Helpers.toByteArray(0.1f));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo.table_name"), "1_el", "residential", "id", Helpers.toByteArray("1-1-1-1-2"));
		//empty unit without comsumtion
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo.table_name"), "2_el", "device", "amount", Helpers.toByteArray(0.0f));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo.table_name"), "2_el", "residential", "id", Helpers.toByteArray("1-1-1-1-3"));
		Accumulo.getInstance().flushBashWriter(Config.getProperty("accumulo.table_name"));
		
		//read input table
		Iterator<Map.Entry<Key, Value>> entries = Accumulo.getInstance().scanAll(Config.getProperty("accumulo.table_name"));
		int i = 0;
		for ( ; entries.hasNext(); ++i ) {
			Entry<Key, Value> entry = entries.next();
			Logger.info(entry.getKey().getRow().toString(), entry.getKey().getColumnFamily().toString(), entry.getKey().getColumnQualifier().toString(), entry.getKey().getColumnVisibility().toString(), "" + entry.getKey().getTimestamp(), entry.getValue().toString());
		}
		assertNotEquals(i, 0);	

		int res = ToolRunner.run(new Configuration(), new EmptyUnitConsumptionReport(), new String[0]);
		assertEquals(0, res);

		//read output table
		Iterator<Entry<Key, Value>> entriesOut = Accumulo.getInstance().scanAll(Config.getProperty("reports.empty_consumption_report.table_name"));
		int k = 0;
		for ( ; entriesOut.hasNext(); ++k ) {
			Entry<Key, Value> entry = entriesOut.next();
			Logger.info(entry.getKey().getRow().toString(), entry.getKey().getColumnFamily().toString(), entry.getKey().getColumnQualifier().toString(), entry.getKey().getColumnVisibility().toString(), "" + entry.getKey().getTimestamp(), entry.getValue().toString());
			
		}
		assertNotEquals(k, 0);	
		
		//delete table
		Accumulo.getInstance().deleteTable(Config.getProperty("accumulo.table_name"));
		Accumulo.getInstance().disconnect();
	}
}