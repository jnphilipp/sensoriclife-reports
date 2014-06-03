package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;
import java.text.DateFormat;
import java.text.Format;
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
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.apache.hadoop.io.Text;
import org.sensoriclife.reports.helper.ConsumptionGeneralizeReport;
import org.sensoriclife.reports.helper.ConvertMinMaxTimeStampReport;
import org.sensoriclife.util.Helpers;

public class MinMaxConsumptionReport {
	
	public static boolean test = true;
	
	public static boolean runMinMaxConsumption() throws Exception{
			
		Map<String, String> confs = Config.toMap();
		
		if(test)
		{
			
			MockInstance inputMockInstance = new MockInstance(Config.getProperty("accumulo.name"));
			MockInstance outputMockInstance = new MockInstance(Config.getProperty("accumulo.name"));
			
			Connector inputConnector = inputMockInstance.getConnector("", new PasswordToken(""));
			Connector outputConnector = outputMockInstance.getConnector("", new PasswordToken(""));
			
			inputConnector.tableOperations().create(Config.getProperty("accumulo.tableName"),false);
			//outputConnector.tableOperations().create(Config.getProperty("reports.minMaxConsumption.outputTable.tableName"), false);
			
			// insert some test data
			insertData(inputConnector, Config.getProperty("accumulo.tableName"));
			
		}
		
		GregorianCalendar now = new GregorianCalendar(); 
		long reportTimestamp = now.getTimeInMillis();
		
		String[] jobArgs = new String[6];
		if(confs.containsKey("accumulo.tableName")){
			jobArgs[0] = confs.get("accumulo.tableName");}
		else{Logger.error("ConfigError: accumulo.tableName doesn't exist");return false;}
		if(confs.containsKey("reports.minMaxConsumption.outputTable.tableName")){
			jobArgs[1] = confs.get("reports.minMaxConsumption.outputTable.tableName");}
		else{Logger.error("ConfigError: reports.minMaxConsumption.outputTable.tableName doesn't exist");return false;}
		if(confs.containsKey("reports.minMaxConsumption.dateRange.min")){
			jobArgs[2] = confs.get("reports.minMaxConsumption.dateRange.min");}
		else{jobArgs[2] = "";}
		if(confs.containsKey("reports.minMaxConsumption.dateRange.max")){
			jobArgs[3] = confs.get("reports.minMaxConsumption.dateRange.max");}
		else{jobArgs[3] = "";}
		if(confs.containsKey("reports.minMaxConsumption.dataRange.onlyInTimeRange")){
			jobArgs[4] = confs.get("reports.minMaxConsumption.dataRange.onlyInTimeRange");}
		else{jobArgs[4] = "false";}
		jobArgs[5] = new Long(reportTimestamp).toString();
		
		//first report: merge residentialID,consumptionID and Consumption -> filter timestamp
		ConvertMinMaxTimeStampReport.test = test;
		ConvertMinMaxTimeStampReport.runConvert(jobArgs);
		
		//second report: consumption for a residentialUnits
		/*if(Config.getProperty("reports.minMaxConsumption.run.residentialUnit").equals("true"))
		{
			jobArgs = new String[7];
			jobArgs[0] = Config.getProperty("reports.minMaxConsumption.outputTable.name");//OutputInstanceName
			jobArgs[1] = Config.getProperty("reports.minMaxConsumption.outputTable.zooServers");//Server
			jobArgs[2] = Config.getProperty("reports.minMaxConsumption.outputTable.tableName");//outputTableName	
			jobArgs[3] = Config.getProperty("reports.minMaxConsumption.outputTable.user");//outputUserName
			jobArgs[4] = Config.getProperty("reports.minMaxConsumption.outputTable.password");//outputPassword
			jobArgs[5] = "6";
			jobArgs[6] = new Long(reportTimestamp).toString();
			ConsumptionGeneralizeReport.test=test;
			ConsumptionGeneralizeReport.runConsumptionGeneralize(jobArgs);
			
			jobArgs = new String[2];
			jobArgs[0] = "5";
			jobArgs[1] = new Long(reportTimestamp).toString();
			MinMaxReport.test = test;
			MinMaxReport.runMinMax(jobArgs);
		}
			
		if(Config.getProperty("reports.minMaxConsumption.run.builiding").equals("true"))
		{
			jobArgs = new String[6];
			jobArgs[0] = Config.getProperty("reports.minMaxConsumption.outputTable.name");//OutputInstanceName
			jobArgs[1] = Config.getProperty("reports.minMaxConsumption.outputTable.zooServers");//Server
			jobArgs[2] = Config.getProperty("reports.minMaxConsumption.outputTable.tableName");//outputTableName	
			jobArgs[3] = Config.getProperty("reports.minMaxConsumption.outputTable.user");//outputUserName
			jobArgs[4] = Config.getProperty("reports.minMaxConsumption.outputTable.password");//outputPassword
			jobArgs[5] = new Long(reportTimestamp).toString();
			ConsumptionGeneralizeToBuildingReport.test=test;
			ConsumptionGeneralizeToBuildingReport.runConsumptionGeneralize(jobArgs);
			
			jobArgs = new String[2];
			jobArgs[0] = "4";
			jobArgs[1] = new Long(reportTimestamp).toString();
			MinMaxReport.test=test;
			MinMaxReport.runMinMax(jobArgs);
		}*/
		
		if(test){
			Accumulo.getInstance().connect(Config.getProperty("accumulo.name"));
		}
		else{
			Accumulo.getInstance().connect(confs.get("accumulo.name"),
					confs.get("accumulo.zooServers"),
					confs.get("accumulo.user"),
					confs.get("accumulo.password"));
		}
		
		Accumulo.getInstance().addMutation(confs.get("reports.minMaxConsumption.outputTable.tableName"), Helpers.toByteArray("MinMaxConsumption"), Helpers.toByteArray("report"), Helpers.toByteArray("version"), Helpers.toByteArray(reportTimestamp));
		Accumulo.getInstance().flushBashWriter(confs.get("reports.minMaxConsumption.outputTable.tableName"));
		Accumulo.getInstance().disconnect();
		/*
		 * Test
		 */
		if(test)
		{
			MockInstance inputMockInstance = new MockInstance(Config.getProperty("accumulo.name"));
			MockInstance outputMockInstance = new MockInstance(Config.getProperty("accumulo.name"));
			
			Connector inputConnector = inputMockInstance.getConnector("", new PasswordToken(""));
			Connector outputConnector = outputMockInstance.getConnector("", new PasswordToken(""));
			
			// print the inputtable
			printTable(inputConnector, Config.getProperty("accumulo.tableName"));
			System.out.println("############################################");
			// print the results of mapreduce
			printTable(outputConnector, Config.getProperty("reports.minMaxConsumption.outputTable.tableName"));
		}
		
		return true;
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
		
		byte[] colFam = Helpers.toByteArray("device");
		byte[] colQual = Helpers.toByteArray("amount");
		byte[] colFam2 = Helpers.toByteArray("residential");
		byte[] colQual2 = Helpers.toByteArray("id");
		ColumnVisibility colVis = new ColumnVisibility();// "public");
		// long timestamp = System.currentTimeMillis();
		
		Float floatValue = new Float(1);
		Date d = formatter.parse( "12.03.2012");
		//mutation with consumptionId as rowId
		Mutation mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(), Helpers.toByteArray(floatValue));
		mutation.put(colFam2, colQual2, colVis, 2, Helpers.toByteArray("1-1-1-1-1"));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(2)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "14.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(3)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.04.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(4)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.10.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(5)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "12.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),Helpers.toByteArray(new Float(6)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),Helpers.toByteArray(new Float(7)));
		wr.addMutation(mutation);
		
		
		
		d = formatter.parse( "12.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId+1) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,Helpers.toByteArray("1-1-1-1-2"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(1)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId+1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(2)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "14.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId+1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(3)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.04.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId+1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(4)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.10.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId+1) + "_el"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(10)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "12.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId+1) + "_el"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),Helpers.toByteArray(new Float(13)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId+1) + "_el"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),Helpers.toByteArray(new Float(15)));
		wr.addMutation(mutation);
		
		
		
		d = formatter.parse( "12.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(1)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(2)));
		mutation.put(colFam2, colQual2, colVis, 2,Helpers.toByteArray("1-1-1-1-1"));
		wr.addMutation(mutation);
		
		d = formatter.parse( "14.03.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(3)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.04.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(4)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.10.2012");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, d.getTime(),Helpers.toByteArray(new Float(5)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "12.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),Helpers.toByteArray(new Float(6)));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2013");
		mutation = new Mutation(Helpers.toByteArray(String.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),Helpers.toByteArray(new Float(7)));
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
			Date date = new Date(key.getTimestamp());
			Format format = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
			
			String timestamp = format.format(date).toString();
			
			if(key.getColumnQualifier().toString().equals("amount"))//getColumnFamily().toString().equals("device"))
			{
				try {
					
					System.out.println("Row: " + (String) Helpers.toObject(key.getRow().getBytes()) + " Fam: " + (String) Helpers.toObject(key.getColumnFamily().getBytes()) + " Qual: " + (String) Helpers.toObject(key.getColumnQualifier().getBytes()) +" Ts:" + 
							timestamp +  " Value: " + (Float) Helpers.toObject(v.get()));
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
				try {
					System.out.println("Row: " + (String) Helpers.toObject(key.getRow().getBytes()) + " Fam: " + (String) Helpers.toObject(key.getColumnFamily().getBytes()) + " Qual: " + (String) Helpers.toObject(key.getColumnQualifier().getBytes()) +" Ts:" + 
							timestamp +  " Value: " +  Helpers.toObject(v.get()));
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}	
}
