package org.sensoriclife.reports.yearConsumption;

import java.io.IOException;
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
import org.apache.hadoop.io.Text;
import org.sensoriclife.reports.convert.ConvertForAYearReport;
import org.sensoriclife.util.Helpers;

public class YearConsumptionReport {
	
	public static boolean test = true;
	
	public static void runYearConsumption(String[] args) throws Exception
	{
		/*
		 * Todo: Versionierung muss noch rein 
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
		
		
		//first report: merge residentialID,consumptionID and Consumption -> filter timestamp
		ConvertForAYearReport.runConvert(args);
		
		//second report: consumption for a residentialUnits
		String[] summeryArgs = new String[6];
		summeryArgs[0] = args[0];
		summeryArgs[1] = args[5];
		summeryArgs[2] = args[6];
		summeryArgs[3] = args[7];
		summeryArgs[4] = args[8];
		summeryArgs[5] = "6";
		
		ConsumptionGeneralizeReport.runYearConsumptionGeneralize(summeryArgs);
		
		//buildings
		summeryArgs[5] = "5";
		ConsumptionGeneralizeReport.runYearConsumptionGeneralize(summeryArgs);
		
		//streets
		summeryArgs[5] = "4";
		ConsumptionGeneralizeReport.runYearConsumptionGeneralize(summeryArgs);
		
		//districts
		summeryArgs[5] = "3";
		ConsumptionGeneralizeReport.runYearConsumptionGeneralize(summeryArgs);
		
		//cities
		summeryArgs[5] = "2";
		ConsumptionGeneralizeReport.runYearConsumptionGeneralize(summeryArgs);
				
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
			TableExistsException, TableNotFoundException {
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter wr = conn.createBatchWriter(tableName, config);

		// ######################################################################
		int consumptionId = 1;
		Text colFam = new Text("device");
		Text colQual = new Text("amount");
		Text colFam2 = new Text("residential");
		Text colQual2 = new Text("id");
		ColumnVisibility colVis = new ColumnVisibility();// "public");
		// long timestamp = System.currentTimeMillis();
		
		Float floatValue = new Float(4.0);
		Value value = new Value(Helpers.toByteArray(floatValue));
		//mutation with consumptionId as rowId
		Mutation mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wu"));
		mutation.put(colFam, colQual, colVis, 1, value);
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-1".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1) + "_wu"));
		mutation.put(colFam, colQual, colVis, 2,new Value(Helpers.toByteArray(new Float(8))));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-2".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 2) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-3".getBytes()));
		mutation.put(colFam, colQual, colVis, 3,new Value(Helpers.toByteArray(new Float(111))));
		wr.addMutation(mutation);
		
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 3) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-4".getBytes()));
		mutation.put(colFam, colQual, colVis, 4,new Value(Helpers.toByteArray(new Float(7))));
		wr.addMutation(mutation);
		
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 5) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-5".getBytes()));
		mutation.put(colFam, colQual, colVis, 10,new Value(Helpers.toByteArray(new Float(42))));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 5) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-5".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 2,new Value(Helpers.toByteArray(new Float(2))));
		wr.addMutation(mutation);
		
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 6) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-7".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 3,new Value(Helpers.toByteArray(new Float(2))));
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
