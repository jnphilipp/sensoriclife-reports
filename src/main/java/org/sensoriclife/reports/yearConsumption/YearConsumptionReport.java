package org.sensoriclife.reports.yearConsumption;

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
import org.apache.hadoop.io.Text;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.helper.ConsumptionGeneralizeReport;
import org.sensoriclife.reports.helper.ConvertForAYearReport;
import org.sensoriclife.util.Helpers;

public class YearConsumptionReport {
	
	public static boolean test = true;
	
	public static boolean runYearConsumption() throws Exception{
		
		Map<String, String> confs = Config.toMap();
		
		GregorianCalendar now = new GregorianCalendar(); 
		long reportTimestamp = now.getTimeInMillis();
		
		String instanceName = "";
		String zooServer = "";
		String userName = "";
		String password = "";
		String outputTableName = "";
		String inputTableName = "";
		
		if(confs.containsKey("accumulo.name")){
			instanceName = confs.get("accumulo.name");}
		else{Logger.error("ConfigError: accumulo.name doesn't exist");return false;}
		if(confs.containsKey("accumulo.zooServers")){
			zooServer = confs.get("accumulo.zooServers");}
		else{Logger.error("ConfigError: accumulo.zooServers doesn't exist");return false;}
		if(confs.containsKey("accumulo.user")){
			userName = confs.get("accumulo.user");}
		else{Logger.error("ConfigError: accumulo.user doesn't exist");return false;}
		if(confs.containsKey("accumulo.password")){
			password = confs.get("accumulo.password");}
		else{Logger.error("ConfigError: accumulo.password doesn't exist");return false;}
		if(confs.containsKey("reports.yearConsumption.outputTable.tableName")){
			password = confs.get("reports.yearConsumption.outputTable.tableName");}
		else{Logger.error("ConfigError: reports.yearConsumption.outputTable.tableName doesn't exist");return false;}
		if(confs.containsKey("accumulo.tableName")){
			inputTableName = confs.get("accumulo.tableName");}
		else{Logger.error("ConfigError: accumulo.tableName doesn't exist");return false;}
		
		
		if(test)
		{
			
			MockInstance inputMockInstance = new MockInstance(instanceName);
			MockInstance outputMockInstance = new MockInstance(instanceName);
			
			Connector inputConnector = inputMockInstance.getConnector("", new PasswordToken(""));
			Connector outputConnector = outputMockInstance.getConnector("", new PasswordToken(""));
			
			inputConnector.tableOperations().create(inputTableName,false);
			outputConnector.tableOperations().create(outputTableName, false);
			
			// insert some test data
			insertData(inputConnector, inputTableName);
			
		}
		
		//JOB1
		String[] jobArgs = new String[6];
		if(confs.containsKey("accumulo.tableName")){
			jobArgs[0] = confs.get("accumulo.tableName");}
		else{Logger.error("ConfigError: accumulo.tableName doesn't exist");return false;}
		if(confs.containsKey("reports.minMaxConsumption.outputTable.tableName")){
			outputTableName = jobArgs[1] = confs.get("reports.minMaxConsumption.outputTable.tableName");}
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
		/*String[] jobArgs = new String[13];
		jobArgs[0] = Config.getProperty("accumulo.name");//inputInstanceName
		jobArgs[1] = Config.getProperty("accumulo.zooServers");//Server
		jobArgs[2] = Config.getProperty("accumulo.tableName");//inputTableName	
		jobArgs[3] = Config.getProperty("accumulo.user");//inputUserName
		jobArgs[4] = Config.getProperty("accumulo.password");//inputPassword
		
		jobArgs[5] = Config.getProperty("reports.yearConsumption.outputTable.name");//OutputInstanceName
		jobArgs[6] = Config.getProperty("reports.yearConsumption.outputTable.zooServers");//Server
		jobArgs[7] = Config.getProperty("reports.yearConsumption.outputTable.tableName");//outputTableName	
		jobArgs[8] = Config.getProperty("reports.yearConsumption.outputTable.user");//outputUserName
		jobArgs[9] = Config.getProperty("reports.yearConsumption.outputTable.password");//outputPassword
						
		jobArgs[10] = Config.getProperty("reports.yearConsumption.dateRange.untilDate");
		jobArgs[11] = Config.getProperty("reports.yearConsumption.dataRange.onlyInTimeRange");//in the TimeRange
		
		jobArgs[12] = new Long(reportTimestamp).toString();*/
		
		
		//first report: merge residentialID,consumptionID and Consumption -> filter timestamp
		ConvertForAYearReport.test=test;
		ConvertForAYearReport.runConvert(jobArgs);
		
		//second report: consumption for a residentialUnits
		/*jobArgs = new String[7];
		jobArgs[0] = Config.getProperty("reports.yearConsumption.outputTable.name");//OutputInstanceName
		jobArgs[1] = Config.getProperty("reports.yearConsumption.outputTable.zooServers");//Server
		jobArgs[2] = Config.getProperty("reports.yearConsumption.outputTable.tableName");//outputTableName	
		jobArgs[3] = Config.getProperty("reports.yearConsumption.outputTable.user");//outputUserName
		jobArgs[4] = Config.getProperty("reports.yearConsumption.outputTable.password");//outputPassword
		jobArgs[5] = "6";
		jobArgs[6] = new Long(reportTimestamp).toString();
		
		ConsumptionGeneralizeReport.test=test;
		ConsumptionGeneralizeReport.runConsumptionGeneralize(jobArgs);
		
		//buildings
		jobArgs[5] = "5";
		ConsumptionGeneralizeReport.test=test;
		ConsumptionGeneralizeReport.runConsumptionGeneralize(jobArgs);
		
		//streets
		jobArgs[5] = "4";
		ConsumptionGeneralizeReport.test=test;
		ConsumptionGeneralizeReport.runConsumptionGeneralize(jobArgs);
		
		//districts
		jobArgs[5] = "3";
		ConsumptionGeneralizeReport.test=test;
		ConsumptionGeneralizeReport.runConsumptionGeneralize(jobArgs);
		
		//cities
		jobArgs[5] = "2";
		ConsumptionGeneralizeReport.test=test;
		ConsumptionGeneralizeReport.runConsumptionGeneralize(jobArgs);*/
		
		if(test){
			Accumulo.getInstance().connect(instanceName);
		}
		else{
			Accumulo.getInstance().connect(instanceName,
					zooServer,
					userName,
					password);
		}
		Accumulo.getInstance().addMutation(outputTableName, Helpers.toByteArray("YearConsumption"), Helpers.toByteArray("report"), Helpers.toByteArray("version"), Helpers.toByteArray(reportTimestamp));
		Accumulo.getInstance().flushBashWriter(outputTableName);
		Accumulo.getInstance().disconnect();
		/*
		 * Test
		 */
		if(test)
		{
			MockInstance inputMockInstance = new MockInstance(instanceName);
			MockInstance outputMockInstance = new MockInstance(instanceName);
			
			Connector inputConnector = inputMockInstance.getConnector("", new PasswordToken(""));
			Connector outputConnector = outputMockInstance.getConnector("", new PasswordToken(""));
			
			// print the inputtable
			printTable(inputConnector, inputTableName);
			System.out.println("############################################");
			// print the results of mapreduce
			printTable(outputConnector, outputTableName);
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
		
		/*Text colFam = new Text("device");
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
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wk"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-3".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(1))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wk"));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(2))));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-1".getBytes()));
		wr.addMutation(mutation);
		
		d = formatter.parse( "14.03.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wk"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-3".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(3))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.04.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wk"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-4".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(4))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.10.2012");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wk"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-5".getBytes()));
		mutation.put(colFam, colQual, colVis, d.getTime(),new Value(Helpers.toByteArray(new Float(5))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "12.03.2013");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wk"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-5".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),new Value(Helpers.toByteArray(new Float(6))));
		wr.addMutation(mutation);
		
		d = formatter.parse( "13.03.2013");
		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wk"));
		//mutation.put(colFam2, colQual2, colVis, 2,new Value("1-1-1-1-7".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), d.getTime(),new Value(Helpers.toByteArray(new Float(7))));
		wr.addMutation(mutation);
		
		wr.close();*/
		
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
			
			String timestamp = format.format(date).toString();;
			
			if(key.getColumnQualifier().toString().equals("amount"))//getColumnFamily().toString().equals("device"))
			{
				try {
					
					System.out.println("Row: " + key.getRow() + " Fam: " + key.getColumnFamily() + " Qual: " + key.getColumnQualifier() +" Ts:" + 
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
				System.out.println("Row: " + key.getRow() + " Fam: " + key.getColumnFamily() + " Qual: " + key.getColumnQualifier() +" Ts:" + 
						timestamp +  " Value: " + v);
			}
		}
	}	
}
