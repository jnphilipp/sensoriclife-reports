package org.sensoriclife.reports.emptyUnitComsumtion;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import org.sensoriclife.Config;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;
import org.apache.commons.lang.ArrayUtils;
import static org.junit.Assert.assertNotEquals;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumtionTest 
{
	@Test
	public void mainTest() throws IOException, AccumuloException, AccumuloSecurityException, TableExistsException, MutationsRejectedException, TableNotFoundException, InterruptedException
	{
		//create mock accumulo 
		Config.getInstance().getProperties().setProperty("accumulo.name", "test");
		Config.getInstance().getProperties().setProperty("accumulo_table.name", "table");
		Accumulo.getInstance().connect(Config.getProperty("accumulo.name"));
		Accumulo.getInstance().createTable(Config.getProperty("accumulo_table.name"));
		//unit with user
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo_table.name"), "0_el", "device", "amount", Helpers.toByteArray(0.0f));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo_table.name"), "0_el", "residential", "id", Helpers.toByteArray("1-1-1-1-1"));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo_table.name"), "0_el", "user", "id", ArrayUtils.addAll(Helpers.toByteArray(5L), Helpers.toByteArray("Nati")));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo_table.name"), "0_el", "user", "residential", Helpers.toByteArray("1-1-1-1-1"));
		//empty unit wit comsumtion
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo_table.name"), "1_el", "device", "amount", Helpers.toByteArray(0.1f));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo_table.name"), "0_el", "residential", "id", Helpers.toByteArray("1-1-1-1-2"));
		//empty unit without comsumtion
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo_table.name"), "2_el", "device", "amount", Helpers.toByteArray(0.0f));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo_table.name"), "0_el", "residential", "id", Helpers.toByteArray("1-1-1-1-3"));
		Accumulo.getInstance().flushBashWriter(Config.getProperty("accumulo_table.name"));
		
		//read table
		Iterator<Map.Entry<Key, Value>> entries = Accumulo.getInstance().scanAll(Config.getProperty("accumulo_table.name"));
		int i = 0;
		for ( ; entries.hasNext(); ++i ) {entries.next();}
		assertNotEquals(i, 0);		
		
		//delete table
		Accumulo.getInstance().deleteTable(Config.getProperty("accumulo_table.name"));
		Accumulo.getInstance().disconnect();
	}
}


