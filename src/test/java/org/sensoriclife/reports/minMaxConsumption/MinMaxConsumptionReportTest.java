package org.sensoriclife.reports.minMaxConsumption;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.helper.ConsumptionGeneralizeReport;
import org.sensoriclife.reports.helper.ConvertMinMaxTimeStampReport;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class MinMaxConsumptionReportTest {
	@BeforeClass
	public static void setUpClass() throws AccumuloException, AccumuloSecurityException, TableExistsException, ParseException, IOException, MutationsRejectedException, TableNotFoundException {
		Logger.getInstance();

		Config.getInstance().getProperties().setProperty("accumulo.name", "test");
		Config.getInstance().getProperties().setProperty("accumulo.table", "consumptions");

		Accumulo.getInstance().connect(Config.getProperty("accumulo.name"));
		Accumulo.getInstance().createTable(Config.getProperty("accumulo.table"), false);
		Accumulo.getInstance().createTable(Config.getProperty("reports.min_max_consumption.output_table.name"), false);

		int consumptionId = 1;
		DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
	
		Text colQual = new Text("amount");
		Text colFam2 = new Text("residential");
		Text colQual2 = new Text("id");

		Date d = formatter.parse( "12.03.2012");
		Value value = new Value(Helpers.toByteArray(1.0f));
		Mutation mutation = Accumulo.getInstance().newMutation(consumptionId + "_el", "device", "amount", d.getTime(), value);
		mutation.put(colFam2, colQual2, 2, new Value("1-1-1-1-1".getBytes()));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo.table"), mutation);

		d = formatter.parse( "13.03.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(2.0f)));
		
		d = formatter.parse( "14.03.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(3.0f)));
		
		d = formatter.parse( "13.04.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(4.0f)));
		
		d = formatter.parse( "13.10.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(5.0f)));
		
		d = formatter.parse( "12.03.2013");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(6.0f)));
		
		d = formatter.parse( "13.03.2013");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(7.0f)));
		
		
		
		d = formatter.parse( "12.03.2012");
		consumptionId++;
		mutation = Accumulo.getInstance().newMutation(consumptionId + "_el", "residential", "id", 2, new Value("1-1-1-1-1".getBytes()));
		mutation.put("device", "amount", d.getTime(), new Value(Helpers.toByteArray(1.0f)));
		Accumulo.getInstance().addMutation(Config.getProperty("accumulo.table"), mutation);
		
		d = formatter.parse( "13.03.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(2.0f)));
		
		d = formatter.parse( "14.03.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(3.0f)));
		
		d = formatter.parse( "13.04.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(4.0f)));
		
		d = formatter.parse( "13.10.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(10.0f)));
		
		d = formatter.parse( "12.03.2013");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(13.0f)));
		
		d = formatter.parse( "13.03.2013");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_el", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(15.0f)));
		
		
		
		d = formatter.parse( "12.03.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_wc", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(1.0f)));
		
		d = formatter.parse( "13.03.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_wc", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(2.0f)));
		
		d = formatter.parse( "14.03.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_wc", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(3.0f)));
		
		d = formatter.parse( "13.04.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_wc", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(4.0f)));
		
		d = formatter.parse( "13.10.2012");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_wc", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(5.0f)));
		
		d = formatter.parse( "12.03.2013");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_wc", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(6.0f)));
		
		d = formatter.parse( "13.03.2013");
		Accumulo.getInstance().newMutation(Config.getProperty("accumulo.table"), consumptionId + "_wc", "device", "amount", d.getTime(), new Value(Helpers.toByteArray(7.0f)));

		Accumulo.getInstance().flushBashWriter(Config.getProperty("accumulo.table"));
	}

	@AfterClass
	public static void tearDownClass() throws IOException, InterruptedException, MutationsRejectedException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
		Accumulo.getInstance().deleteTable(Config.getProperty("accumulo.table"));
		Accumulo.getInstance().deleteTable(Config.getProperty("reports.min_max_consumption.output_table.name"));
		Accumulo.getInstance().disconnect();
	}

	@Test
	public void testMinMaxConsumptionReport() throws Exception {
		String[] args = new String[15];
		args[0] = "Report: MinMaxConsumption"; //Name :)
		args[1] = Config.getProperty("accumulo.name");//inputInstanceName
		args[2] = Config.getProperty("accumulo.zooServers");//inputTableName
		args[3] = Config.getProperty("accumulo.user");//inputUserName
		args[4] = Config.getProperty("accumulo.password");//inputPassword
		args[5] = Config.getProperty("accumulo.table_name");//inputPassword
		  
		args[6] = Config.getProperty("reports.min_max_consumption.output_table.name");//outputInstanceName
		args[7] = Config.getProperty("reports.min_max_consumption.output_table.zooServers");//outputTableName
		args[8] = Config.getProperty("reports.min_max_consumption.output_table.user");//outputUserName
		args[9] = Config.getProperty("reports.min_max_consumption.output_table.password");//outputPassword
		   
		args[10] = Config.getProperty("accumulo.minMaxConsumption.dateRange.min");//(time) minTimestamp
		args[11] = Config.getProperty("accumulo.minMaxConsumption.dateRange.max");//(long) maxTimestamp
		args[12] = Config.getProperty("accumulo.minMaxConsumption.dataRange.onlyYear");//(boolean) onlyYear -> is false, when the compute inside the year
		args[13] = Config.getProperty("accumulo.minMaxConsumption.outPutTable.pastLimitation");//(time) pastLimitation
		 
		args[14] = Config.getProperty("accumulo.minMaxConsumption.run.residentialUnit");//(boolean) compute min max consumption for residentialunit
		args[15] = Config.getProperty("accumulo.minMaxConsumption.run.builiding");//(boolean) compute min max consumption for builiding
		 		
		GregorianCalendar now = new GregorianCalendar(); 
		long reportTimestamp = now.getTimeInMillis();
		
		String[] filterArgs = new String[14];
		for(int i = 0; (i < args.length) && (i < 13);i++)
			filterArgs[i] = args[i];
		filterArgs[13] = new Long(reportTimestamp).toString();
	
		//first report: merge residentialID,consumptionID and Consumption -> filter timestamp
		ConvertMinMaxTimeStampReport.runConvert(filterArgs);
		
		//second report: consumption for a residentialUnits
		String[] summeryArgs = new String[7];
		summeryArgs[0] = args[0];
		summeryArgs[1] = args[5];
		summeryArgs[2] = args[6];
		summeryArgs[3] = args[7];
		summeryArgs[4] = args[8];
		summeryArgs[5] = "";
		summeryArgs[6] = new Long(reportTimestamp).toString();
		
		if(args[13].equals("true"))
		{
			summeryArgs[5] = "6";
			ConsumptionGeneralizeReport.runYearConsumptionGeneralize(summeryArgs);
			
			summeryArgs[5] = "5";
			MinMaxReport.runMinMax(summeryArgs);
		}
			
		if(args[14].equals("true"))
		{
			ConsumptionGeneralizeToBuildingReport.runConsumptionGeneralize(summeryArgs);
			
			summeryArgs[5] = "4";
			MinMaxReport.runMinMax(summeryArgs);
		}
		
		Accumulo.getInstance().connect(args[5]);
		Accumulo.getInstance().addMutation(args[6], "MinMaxConsumption", "report", "version", Helpers.toByteArray(reportTimestamp));
		Accumulo.getInstance().flushBashWriter(args[6]);
		Accumulo.getInstance().disconnect();

		Iterator<Entry<Key, Value>> iterator = Accumulo.getInstance().scanAll(Config.getProperty("reports.min_max_consumption.output_table.name"));

		while ( iterator.hasNext() ) {
			Entry<Key, Value> entry = iterator.next();

			assertTrue(entry.getKey().getRow().equals("MinMaxConsuption"));
		}
	}
}