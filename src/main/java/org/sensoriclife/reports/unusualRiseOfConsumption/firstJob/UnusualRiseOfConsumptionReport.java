package org.sensoriclife.reports.unusualRiseOfConsumption.firstJob;

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
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.util.MockInstanceConfiguration;
import org.sensoriclife.world.ResidentialUnit;

public class UnusualRiseOfConsumptionReport extends Configured implements Tool {

	public static MockInstance runFirstJob() throws Exception {

		MockInstanceConfiguration mockConfig = MockInstanceConfiguration
				.getInstance();
		mockConfig.setMockInstanceName("mockInstance");
		mockConfig.setInputTableName("Consumption");
		mockConfig.setOutputTableName("UnusualRiseOfConsumption");
		mockConfig.setUserName("");
		mockConfig.setPassword("");

		MockInstance mockInstance = new MockInstance(
				mockConfig.getMockInstanceName());
		Connector connector = mockInstance.getConnector(
				mockConfig.getUserName(),
				new PasswordToken(mockConfig.getPassword()));
		connector.tableOperations().create(mockConfig.getInputTableName(),
				false);
		connector.tableOperations().create(mockConfig.getOutputTableName(),
				false);
		connector.tableOperations().create("HeatingConsumption",
				false);

		// insert some test data
		insertData(connector, mockConfig.getInputTableName());

		/*
		 * Accumulo accumulo = Accumulo.getInstance(); accumulo.connect();
		 * Connector connector = accumulo.getConnector();
		 * 
		 * accumulo.createTable(args2[0]); accumulo.createTable(args2[4]);
		 * 
		 * String colFam = "device"; String colQual = "amount"; long timestamp =
		 * System.currentTimeMillis();
		 * 
		 * accumulo.write(args2[0], "1_el", colFam, colQual, timestamp, new
		 * Value("5".getBytes())); accumulo.write(args2[0], "2_el", colFam,
		 * colQual, timestamp, new Value("3".getBytes()));
		 * accumulo.write(args2[0], "3_el", colFam, colQual, timestamp, new
		 * Value("2".getBytes())); accumulo.write(args2[0], "4_el", colFam,
		 * colQual, timestamp, new Value("8".getBytes()));
		 * 
		 * Iterator<Entry<Key,Value>> scanner = accumulo.scanAll(args2[0]);
		 * 
		 * while(scanner.hasNext()){ Entry<Key, Value> entry = scanner.next();
		 * System.out.println("Key: " + entry.getKey().toString() + " Value: " +
		 * entry.getValue().toString()); }
		 */

		// run the map reduce job to read the edge table and populate the node
		// table
		int res = ToolRunner.run(new Configuration(),
				new UnusualRiseOfConsumptionReport(),
				mockConfig.getConfigAsStringArray());

		/*
		 * Iterator<Entry<Key,Value>> scanner2 = accumulo.scanAll(args2[4]);
		 * 
		 * while(scanner2.hasNext()){ Entry<Key, Value> entry = scanner2.next();
		 * System.out.println("Key: " + entry.getKey().toString() + " Value: " +
		 * entry.getValue().toString()); }
		 */

		// print the inputtable
		printTable(connector, mockConfig.getInputTableName());
		System.out.println("############################################");
		// print the results of mapreduce
		printTable(connector, mockConfig.getOutputTableName());

		// TO DO: remove old records from table

		return mockInstance;

	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.setLong("minTimestamp", 1);
		conf.setLong("maxTimestamp", 2);

		Job job = Job.getInstance(conf);
		job.setJobName(UnusualRiseOfConsumptionReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(UnusualRiseOfConsumptionMapper.class);
		job.setReducerClass(UnusualRiseOfConsumptionReducer.class);

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

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ResidentialUnit.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
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

		Value value = new Value("90".getBytes());

		// mutation with consumptionId as rowId
		Mutation mutation = new Mutation(new Text(String.valueOf(consumptionId)
				+ "_wc"));
		mutation.put(colFam, colQual, colVis, 1, value);
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("1-2-10".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, 2, new Value("440".getBytes()));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("1-2-10".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1)
				+ "_wc"));
		mutation.put(colFam, colQual, colVis, 1, new Value("80".getBytes()));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("1-2-3".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1)
				+ "_wc"));
		mutation.put(colFam, colQual, colVis, 2, new Value("125".getBytes()));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("1-2-3".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wh"));
		mutation.put(colFam, colQual, colVis, 1, new Value("35".getBytes()));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("1-2-10".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wh"));
		mutation.put(colFam, colQual, colVis, 2, new Value("175".getBytes()));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("1-2-10".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("1-2-2".getBytes()));
		mutation.put(colFam, colQual, colVis, 1, new Value("111".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("1-2-2".getBytes()));
		mutation.put(colFam, colQual, colVis, 2, new Value("222".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("1-2-1".getBytes()));
		mutation.put(colFam, colQual, colVis, 1, new Value("111".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("1-2-1".getBytes()));
		mutation.put(colFam, colQual, colVis, 2, new Value("600".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 2)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("1-1-3".getBytes()));
		mutation.put(colFam, colQual, colVis, 1, new Value("42".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 2)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("1-1-3".getBytes()));
		mutation.put(colFam, colQual, colVis, 2, new Value("50".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 3)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("2-2-3".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 1, new Value(
				"333".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 3)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("2-2-3".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 2, new Value(
				"1500".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 4)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("3-2-3".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 1,
				new Value("1".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 4)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("3-2-3".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 2,
				new Value("14".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 5)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("3-2-8".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 1, new Value(
				"2000".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 5)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("3-2-8".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 2, new Value(
				"5000".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_he"));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("1-2-1".getBytes()));
		mutation.put(colFam, colQual, colVis, 1, new Value("180".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_he"));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("1-2-1".getBytes()));
		mutation.put(colFam, colQual, colVis, 2, new Value("220".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1)
				+ "_he"));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("1-2-4".getBytes()));
		mutation.put(colFam, colQual, colVis, 1, new Value("310".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1)
				+ "_he"));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("1-2-4".getBytes()));
		mutation.put(colFam, colQual, colVis, 2, new Value("800".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 2)
				+ "_he"));
		mutation.put(colFam2, colQual2, colVis, 1,
				new Value("1-2-4".getBytes()));
		mutation.put(colFam, colQual, colVis, 1, new Value("50".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 2)
				+ "_he"));
		mutation.put(colFam2, colQual2, colVis, 2,
				new Value("1-2-4".getBytes()));
		mutation.put(colFam, colQual, colVis, 2, new Value("80".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 8)
				+ "_el"));
		mutation.put(colFam2, colQual2, colVis, 0,
				new Value("3-2-3".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 0,
				new Value("14".getBytes()));
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
			System.out.println("Row: " + key.getRow() + " Fam: "
					+ key.getColumnFamily() + " Qual: "
					+ key.getColumnQualifier() + " Ts:" + key.getTimestamp()
					+ " Value: " + v);
		}
	}
}
