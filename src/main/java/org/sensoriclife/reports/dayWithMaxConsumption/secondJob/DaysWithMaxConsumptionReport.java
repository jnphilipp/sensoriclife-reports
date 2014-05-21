package org.sensoriclife.reports.dayWithMaxConsumption.secondJob;

import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.util.MockInstanceConfiguration;
import org.sensoriclife.world.Consumption;

public class DaysWithMaxConsumptionReport extends Configured implements Tool {
	
	public static void runSecondJob(MockInstance mockInstance) throws Exception {

		MockInstanceConfiguration mockConfig = MockInstanceConfiguration.getInstance();
		mockConfig.setMockInstanceName("mockInstance");
		mockConfig.setInputTableName("DaysWithConsumption");
		mockConfig.setOutputTableName("DaysWithMaxConsumption");
		mockConfig.setUserName("");
		mockConfig.setPassword("");
		
		Connector connector = mockInstance.getConnector(mockConfig.getUserName(), new PasswordToken(
				mockConfig.getPassword()));
		//input table should exist (output from first mapreduce job)
		connector.tableOperations().create(mockConfig.getOutputTableName(), false);
		
		/*
		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect();
		Connector connector = accumulo.getConnector();

		accumulo.createTable(args2[0]);
		accumulo.createTable(args2[4]);
		
		String colFam = "device";
		String colQual = "amount";
		long timestamp = System.currentTimeMillis();
		
		accumulo.write(args2[0], "1_el", colFam, colQual, timestamp, new Value("5".getBytes()));
		accumulo.write(args2[0], "2_el", colFam, colQual, timestamp, new Value("3".getBytes()));
		accumulo.write(args2[0], "3_el", colFam, colQual, timestamp, new Value("2".getBytes()));
		accumulo.write(args2[0], "4_el", colFam, colQual, timestamp, new Value("8".getBytes()));
		
		Iterator<Entry<Key,Value>> scanner = accumulo.scanAll(args2[0]);
		
		while(scanner.hasNext()){
			Entry<Key, Value> entry = scanner.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: " + entry.getValue().toString());
		}
		*/
		
		// run the map reduce job to read the edge table and populate the node
		// table
		int res = ToolRunner.run(new Configuration(), new DaysWithMaxConsumptionReport(),
				mockConfig.getConfigAsStringArray());

		/*
		Iterator<Entry<Key,Value>> scanner2 = accumulo.scanAll(args2[4]);
		
		while(scanner2.hasNext()){
			Entry<Key, Value> entry = scanner2.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: " + entry.getValue().toString());
		}
		*/
		
		// print the inputtable
		printTable(connector, mockConfig.getInputTableName());
		System.out.println("############################################");
		// print the results of mapreduce
		printTable(connector, mockConfig.getOutputTableName());
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName(DaysWithMaxConsumptionReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(DaysWithMaxConsumptionMapper.class);
		job.setReducerClass(DaysWithMaxConsumptionReducer.class);

		AccumuloInputFormat.setMockInstance(job, args[3]);
		AccumuloInputFormat.setConnectorInfo(job, args[2], new PasswordToken(
				args[1]));
		AccumuloInputFormat.setInputTableName(job, args[0]);
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());

		AccumuloOutputFormat.setConnectorInfo(job, args[2], new PasswordToken(
				args[1]));
		AccumuloOutputFormat.setDefaultTableName(job, args[4]);
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setMockInstance(job, args[3]);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Consumption.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
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
			System.out.println("Row: " + key.getRow() + " Fam: " + key.getColumnFamily() + " Qual: " + key.getColumnQualifier() +" Ts:" + key.getTimestamp() +  " Value: " + v);
		}
	}	
}
