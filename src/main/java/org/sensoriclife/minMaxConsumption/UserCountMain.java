package org.sensoriclife.minMaxConsumption;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.reports.world.ResidentialUnit;

public class UserCountMain extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {

		String mockInstanceName = "mockInstance";
		String inputTableName = "ElectricityConsumption";
		String outputTableName = "MinMax";
		String userName = "";
		String password = "";

		String[] args2 = new String[5];
		args2[0] = inputTableName;
		args2[1] = password;
		args2[2] = userName;
		args2[3] = mockInstanceName;
		args2[4] = outputTableName;

	
		MockInstance inputMi = new MockInstance(mockInstanceName);
		Connector connector = inputMi.getConnector(userName, new PasswordToken(
				password));
		connector.tableOperations().create(inputTableName, false);
		connector.tableOperations().create(outputTableName, false);
	

		// insert some test data
		insertData(connector, inputTableName);

		/*
		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect();
		Connector connector = accumulo.getConnector();

		accumulo.createTable(args2[0]);
		accumulo.createTable(args2[5]);
		
		String colFam = "electricity";
		String colQual = "amount";
		long timestamp = System.currentTimeMillis();
		
		accumulo.write(args2[0], "row1", colFam, colQual, timestamp, new Value("5".getBytes()));
		accumulo.write(args2[0], "row2", colFam, colQual, timestamp, new Value("3".getBytes()));
		accumulo.write(args2[0], "row3", colFam, colQual, timestamp, new Value("2".getBytes()));
		accumulo.write(args2[0], "row4", colFam, colQual, timestamp, new Value("8".getBytes()));
		
		Iterator<Entry<Key,Value>> scanner = accumulo.scanAll(args2[0]);
		
		while(scanner.hasNext()){
			Entry<Key, Value> entry = scanner.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: " + entry.getValue().toString());
		}
		*/
		
		// run the map reduce job to read the edge table and populate the node
		// table
		int res = ToolRunner.run(new Configuration(), new UserCountMain(),
				args2);

		/*
		Iterator<Entry<Key,Value>> scanner2 = accumulo.scanAll(args2[4]);
		
		while(scanner2.hasNext()){
			Entry<Key, Value> entry = scanner2.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: " + entry.getValue().toString());
		}
		*/
		
		// print the out the counts for each node
		printUserCount(connector, inputTableName);
		System.out.println("############################################");
		// print the out the counts for each node
		printUserCount(connector, outputTableName);

		System.exit(res);

	}

	public static class UserCountMap extends Mapper<Key, Value, IntWritable, Text> {
		public void map(Key k, Value v, Context c) throws IOException,
				InterruptedException {

			Text keyRow = k.getRow();
			String userValue = v.toString();

			System.out.println("MAPKey: " + keyRow.toString() + " Value "
					+ userValue);
			c.write(new IntWritable(1), new Text(userValue));

		}
	}

	public static class UserCountReducer extends
			Reducer<IntWritable, Text, Text, Mutation> {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context c)
				throws IOException, InterruptedException {

			int count = 0;
			for (Text value : values) {
				count += Integer.parseInt(value.toString());
			}
			String countString = new Integer(count).toString();
			System.out.println("REDUCEKey: " + key.toString() + " value "
					+ countString);
			Mutation mutation = new Mutation(new Text(String.valueOf(key)));
			mutation.put(new Text(""), new Text("Sum"), new ColumnVisibility(), 
					1, new Value(countString.getBytes()));
			c.write(new Text("MinMax"), mutation);
		}
	}

	// @Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJobName(UserCountMain.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(AccumuloMMCMapper.class);
		job.setReducerClass(AccumuloMMCReducer.class);
		
		//job.setMapperClass(UserCountMap.class);
		//job.setReducerClass(UserCountReducer.class);

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
		Text rowID = new Text("1");
		Text colFam = new Text("Log");
		Text colQual = new Text("5");
		ColumnVisibility colVis = new ColumnVisibility();// "public");
		// long timestamp = System.currentTimeMillis();

		Value value = new Value("3".getBytes());

		Mutation mutation = new Mutation(rowID);
		mutation.put(colFam, colQual, colVis, 1, value);
		wr.addMutation(mutation);
		mutation = new Mutation("2");
		mutation.put(colFam, new Text("5"), colVis, 2,
				new Value("1".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation("3");
		mutation.put(colFam, new Text("5"), colVis, 3,
				new Value("1".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation("4");
		mutation.put(colFam, new Text("5"), colVis, 4,
				new Value("7".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation("5");
		mutation.put(colFam, new Text("5"), colVis, 10,
				new Value("4".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation("6");
		mutation.put("Log", "5", new ColumnVisibility(), 2,
				new Value("2".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation("7");
		mutation.put("Log", "5", new ColumnVisibility(), 3,
				new Value("3".getBytes()));
		wr.addMutation(mutation);
		wr.close();
	}

	public static void printUserCount(Connector conn, String tableName)
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
