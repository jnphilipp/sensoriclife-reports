package org.sensoriclife.reports.dayWithMaxConsumption.firstJob;

import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.MockInstanceConfiguration;
import org.sensoriclife.world.DeviceUnit;

public class DaysWithConsumptionReport extends Configured implements Tool {
	
	/**
	 * runs the whole process of the first map reduce job, including configuration, inserting of test data and execution of the job.
	 * @return Accumulo
	 * @throws Exception
	 */
	public static Accumulo runFirstJob() throws Exception {

		MockInstanceConfiguration mockConfig = MockInstanceConfiguration.getInstance();
		mockConfig.setMockInstanceName("mockInstance");
		mockConfig.setInputTableName("Consumption");
		mockConfig.setOutputTableName("DaysWithConsumption");
		mockConfig.setUserName("");
		mockConfig.setPassword("");

		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect();

		accumulo.createTable(mockConfig.getInputTableName(), false);
		accumulo.createTable(mockConfig.getOutputTableName(), false);
		
		insertData(accumulo, mockConfig.getInputTableName());

		Iterator<Entry<Key,Value>> scanner = accumulo.scanAll(mockConfig.getInputTableName());
		while(scanner.hasNext()){
			Entry<Key, Value> entry = scanner.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: " + entry.getValue().toString());
		}
		
		ToolRunner.run(new Configuration(), new DaysWithConsumptionReport(),
				mockConfig.getConfigAsStringArray());
		
		Iterator<Entry<Key,Value>> scanner2 = accumulo.scanAll(mockConfig.getOutputTableName());
		while(scanner2.hasNext()){
			Entry<Key, Value> entry = scanner2.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: " + entry.getValue().toString());
		}
		return accumulo;
	}

	/**
	 * executes the accumulo map reduce job.
	 */
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//minTimestamp ~ 01.01.LastYear 0:00.00
		conf.setLong("minTimestamp", 1);
		//maxTimestamp ~ 31.12.LastYear 23:59.59
		conf.setLong("maxTimestamp", 10);
		
		Job job = Job.getInstance(conf);
		job.setJobName(DaysWithConsumptionReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(DaysWithConsumptionMapper.class);
		job.setReducerClass(DaysWithConsumptionReducer.class);

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

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DeviceUnit.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	/**
	 * inserts some test data, adapted for this report.
	 * @param accumulo
	 * @param tableName
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public static void insertData(Accumulo accumulo, String tableName) throws MutationsRejectedException, TableNotFoundException{
		int consumptionId = 1;
		String colFam = "device";
		String colQual = "amount";
		String colFam2 = "residential";
		String colQual2 = "id";

		Mutation mutation = accumulo.newMutation(String.valueOf(consumptionId) + "_wc", colFam, colQual, 1, new Value("4".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1, new Value("1-2-10".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(String.valueOf(consumptionId + 1) + "_wc", colFam, colQual, 2, new Value("125".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2, new Value("1-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.newMutation(String.valueOf(consumptionId) + "_wh", colFam, colQual, 1, new Value("12".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1, new Value("1-2-10".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(String.valueOf(consumptionId + 1) + "_wh", colFam, colQual, 2, new Value("13".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2, new Value("1-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(String.valueOf(consumptionId) + "_el", colFam, colQual, 3, new Value("111".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 3, new Value("1-2-1".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(String.valueOf(consumptionId + 1) + "_el", colFam, colQual, 2, new Value("7".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2, new Value("1-2-0".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(String.valueOf(consumptionId + 2) + "_el", colFam, colQual, 10, new Value("42".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 10, new Value("1-1-3".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(String.valueOf(consumptionId + 3) + "_el", colFam, colQual, 2, new Value("2".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2, new Value("2-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.newMutation(String.valueOf(consumptionId + 4) + "_el", colFam, colQual, 3, new Value("1".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 3, new Value("3-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(String.valueOf(consumptionId) + "_he", colFam, colQual, 3, new Value("18".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 3, new Value("1-2-1".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.newMutation(String.valueOf(consumptionId + 1) + "_he", colFam, colQual, 2, new Value("20".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2, new Value("1-2-0".getBytes()));
		accumulo.addMutation(tableName, mutation);
	}
}
