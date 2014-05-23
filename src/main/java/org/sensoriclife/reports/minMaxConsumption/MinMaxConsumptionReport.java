package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.sensoriclife.db.Accumulo;
import org.apache.hadoop.io.Text;
import org.sensoriclife.reports.convert.ConvertMinMaxTimeStampReport;
import org.sensoriclife.reports.yearConsumption.ConsumptionGeneralizeReport;
import org.sensoriclife.util.Helpers;

public class MinMaxConsumptionReport {
	
	public static boolean test = true;
	
	public static void runMinMaxConsumption(String[] args) throws Exception{/*
		 * Todo: Versionierung muss noch rein 
		 */
		
		/*
		 * 
		 * args[0] = reportName
		 * 
		 * args[1] = inputInstanceName
		 * args[2] = inputTableName
		 * args[3] = inputUserName
		 * args[4] = inputPassword
		 * 
		 * args[5] = outputInstanceName
		 * args[6] = outputTableName
		 * args[7] = outputUserName
		 * args[8] = outputPassword
		 *  
		 * args[9] = (time) minTimestamp
		 * args[10] = (long) maxTimestamp
		 * args[11] = (boolean) onlyYear -> is true, when the compute inside the year
		 * args[12] = (time) pastLimitation
		 * args[13] = (boolean) compute min max consumption for residentialunit
		 * args[14] = (boolean) compute min max consumption for builiding
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		if(test)
		{
			MockInstance inputMockInstance = new MockInstance(args[1]);
			MockInstance outputMockInstance = new MockInstance(args[5]);
			
			Connector inputConnector = inputMockInstance.getConnector(args[3], new PasswordToken(args[4]));
			Connector outputConnector = outputMockInstance.getConnector(args[7], new PasswordToken(args[8]));
			
			inputConnector.tableOperations().create(args[2], false);
			outputConnector.tableOperations().create(args[6], false);

			// insert some test data
			insertData(inputConnector, args[2]);
		}
		
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
		/*
		 * Test
		 */
		if(test)
		{
			MockInstance inputMockInstance = new MockInstance(args[1]);
			MockInstance outputMockInstance = new MockInstance(args[5]);
			
			Connector inputConnector = inputMockInstance.getConnector(args[3], new PasswordToken(args[4]));
			Connector outputConnector = outputMockInstance.getConnector(args[7], new PasswordToken(args[8]));
			
			// print the inputtable
			printTable(inputConnector, args[2]);
			System.out.println("############################################");
			// print the results of mapreduce
			printTable(outputConnector, args[6]);
		}
	}
	
	public static void insertData(Connector conn, String tableName)
			throws IOException, AccumuloException, AccumuloSecurityException,
			TableExistsException, TableNotFoundException, ParseException {
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter wr = conn.createBatchWriter(tableName, config);

		// ######################################################################
		int consumptionId = 1;
		DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
		
		Text colFam = new Text("device");
		Text colQual = new Text("amount");
		Text colFam2 = new Text("residential");
		Text colQual2 = new Text("id");
		ColumnVisibility colVis = new ColumnVisibility();// "public");
		// long timestamp = System.currentTimeMillis();
		
		Float floatValue = new Float(1);
		Date d = formatter.parse( "12.03.2012");
		Value value = new Value(Helpers.toByteArray(floatValue));
		//mutation with consumptionId as rowId
		Mutation mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), value);
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-1".getBytes()));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(2))));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-1".getBytes()));
		wr.addMutation(mutation);
		
		d = formatter.parse( "14.03.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-3".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(3))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.04.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-4".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(4))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.10.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-5".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(5))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "12.03.2013");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-5".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),new Value(Helpers.toByteArray(new Float(6))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2013");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-7".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),new Value(Helpers.toByteArray(new Float(7))));
		wr.addMutation(mutation);
		
		
		
		d = formatter.parse( "12.03.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId+1) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-2".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(1))));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-1".getBytes()));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId+1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(2))));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-1".getBytes()));
		wr.addMutation(mutation);
		
		d = formatter.parse( "14.03.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId+1) + "_el"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-3".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(3))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.04.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId+1) + "_el"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-4".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(4))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.10.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId+1) + "_el"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-5".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(10))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "12.03.2013");
		mutation = new Mutation(new Text(String.valueOf(consumptionId+1) + "_el"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-5".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),new Value(Helpers.toByteArray(new Float(13))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2013");
		mutation = new Mutation(new Text(String.valueOf(consumptionId+1) + "_el"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-7".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),new Value(Helpers.toByteArray(new Float(15))));
		wr.addMutation(mutation);
		
		
		
		d = formatter.parse( "12.03.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wu"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-3".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(1))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wu"));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(2))));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-1".getBytes()));
		wr.addMutation(mutation);
		
		d = formatter.parse( "14.03.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wu"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-3".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(3))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.04.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wu"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-4".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(4))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.10.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wu"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-5".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(5))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "12.03.2013");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wu"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-5".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),new Value(Helpers.toByteArray(new Float(6))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2013");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wu"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-7".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),new Value(Helpers.toByteArray(new Float(7))));
		wr.addMutation(mutation);
		
		wr.close();	
}

	public static void printTable(Connector conn, String tableName)
			throws TableNotFoundException {

		System.out.println("\n Tabelle:" + tableName);
		Scanner scanner = conn.createScanner(tableName, new Authorizations());
		Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			Map.Entry<Key, Value> entry = iterator.next();
			Key key = entry.getKey();
			Value v = entry.getValue();
			if(key.getColumnQualifier().toString().equals("amount"))//getColumnFamily().toString().equals("device"))
			{
				try {
					System.out.println("Row: " + key.getRow() + " Fam: " + key.getColumnFamily() + " Qual: " + key.getColumnQualifier() +" Ts:" + 
							key.getTimestamp() +  " Value: " + (Float) Helpers.toObject(v.get()));
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("Row: " + key.getRow() + " Fam: " + key.getColumnFamily() + " Qual: " + key.getColumnQualifier() +" Ts:" + 
						key.getTimestamp() +  " Value: " + v);
			}
		}
	}	
}
