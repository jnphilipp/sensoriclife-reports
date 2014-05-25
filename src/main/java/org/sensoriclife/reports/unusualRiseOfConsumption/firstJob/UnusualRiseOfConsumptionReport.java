package org.sensoriclife.reports.unusualRiseOfConsumption.firstJob;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.Config;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.world.ResidentialUnit;

public class UnusualRiseOfConsumptionReport extends Configured implements Tool {

	/**
	 * runs the whole process of the first map reduce job, including configuration, inserting of test data and execution of the job.
	 * @return Accumulo
	 * @throws Exception
	 */
	public static Accumulo runFirstJob() throws Exception {
		
		Config conf = Config.getInstance();
		//config for mockinstance
		conf.getProperties().setProperty("mockInstanceName", "mockInstance");
		conf.getProperties().setProperty("inputTableName", "Consumption");
		conf.getProperties().setProperty("outputTableName", "UnusualRiseOfConsumption");
		conf.getProperties().setProperty("helperOutputTableName", "HeatingConsumption");
		conf.getProperties().setProperty("username", "");
		conf.getProperties().setProperty("password", "");
		
		//config for map reduce job
		//time interval: weekly for two weeks
		//week2 - week1 = oldConsumption
		//week1 - week0 = currentConsumption
		conf.getProperties().setProperty("minTimestamp", String.valueOf(System.currentTimeMillis() - 86400000*14));
		conf.getProperties().setProperty("maxTimestamp", String.valueOf(System.currentTimeMillis()));
		
		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect();

		accumulo.createTable(Config.getProperty("inputTableName"), false);
		accumulo.createTable(Config.getProperty("outputTableName"), false);
		// another output table ~ helpertable
		//--> needed for second map reduce job
		accumulo.createTable(Config.getProperty("helperOutputTableName"), false);

		insertData(accumulo, Config.getProperty("inputTableName"));
		Iterator<Entry<Key, Value>> scanner = accumulo.scanAll(Config.getProperty("inputTableName"), new Authorizations());

		while (scanner.hasNext()) {
			Entry<Key, Value> entry = scanner.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}

		String[] args = new String[0];
		ToolRunner.run(new Configuration(),
				new UnusualRiseOfConsumptionReport(),
				args);

		Iterator<Entry<Key, Value>> scanner2 = accumulo.scanAll(Config.getProperty("outputTableName"), new Authorizations());

		while (scanner2.hasNext()) {
			Entry<Key, Value> entry = scanner2.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}

		return accumulo;
	}

	/**
	 * executes the accumulo map reduce job.
	 */
	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJobName(UnusualRiseOfConsumptionReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(UnusualRiseOfConsumptionMapper.class);
		job.setReducerClass(UnusualRiseOfConsumptionReducer.class);

		AccumuloInputFormat.setMockInstance(job, Config.getProperty("mockInstanceName"));
		AccumuloInputFormat.setConnectorInfo(job, Config.getProperty("username"), new PasswordToken(
				Config.getProperty("password")));
		AccumuloInputFormat.setInputTableName(job, Config.getProperty("inputTableName"));
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());

		AccumuloOutputFormat.setConnectorInfo(job, Config.getProperty("username"), new PasswordToken(
				Config.getProperty("password")));
		AccumuloOutputFormat.setDefaultTableName(job, Config.getProperty("outputTableName"));
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setMockInstance(job, Config.getProperty("mockInstanceName"));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ResidentialUnit.class);
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
	public static void insertData(Accumulo accumulo, String tableName)
			throws MutationsRejectedException, TableNotFoundException {
		int consumptionId = 1;
		String colFam = "device";
		String colQual = "amount";
		String colFam2 = "residential";
		String colQual2 = "id";
		
		//current week data : one week = 86400000*7 ms
		long tsCurrent = System.currentTimeMillis() - 900000;
		long tsLastWeek = tsCurrent - 86400000*7 + 900000;
		long tsTwoWeeksAgo = tsCurrent - 86400000*14 + 900000;
		
		Mutation mutation = accumulo.newMutation(
				String.valueOf(consumptionId) + "_el", colFam, colQual, tsTwoWeeksAgo,
				new Value("90".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsTwoWeeksAgo,
				new Value("1-1-1-1-1".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId) + "_el", colFam, colQual, tsLastWeek - 900000,
				new Value("150".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek - 900000,
				new Value("1-1-1-1-1".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId) + "_el", colFam, colQual, tsLastWeek,
				new Value("200".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek,
				new Value("1-1-1-1-1".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId) + "_el", colFam, colQual, tsLastWeek + 900000,
				new Value("300".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek + 900000,
				new Value("1-1-1-1-1".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId) + "_el", colFam, colQual, tsCurrent,
				new Value("390".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsCurrent,
				new Value("1-1-1-1-1".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		//second device - consumption very high
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 1) + "_el", colFam, colQual, tsTwoWeeksAgo,
				new Value("40".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsTwoWeeksAgo,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 1) + "_el", colFam, colQual, tsLastWeek - 900000,
				new Value("150".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek - 900000,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 1) + "_el", colFam, colQual, tsLastWeek,
				new Value("260".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 1) + "_el", colFam, colQual, tsLastWeek + 900000,
				new Value("600".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek + 900000,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 1) + "_el", colFam, colQual, tsCurrent,
				new Value("1190".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsCurrent,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		//third device
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId) + "_he", colFam, colQual, tsTwoWeeksAgo,
				new Value("110".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsTwoWeeksAgo,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId) + "_he", colFam, colQual, tsLastWeek - 900000,
				new Value("160".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek - 900000,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId) + "_he", colFam, colQual, tsLastWeek,
				new Value("190".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId) + "_he", colFam, colQual, tsLastWeek + 900000,
				new Value("400".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek + 900000,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId) + "_he", colFam, colQual, tsCurrent,
				new Value("620".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsCurrent,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		//fourth device (+ third device) in combination too high
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 1) + "_he", colFam, colQual, tsTwoWeeksAgo,
				new Value("80".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsTwoWeeksAgo,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 1) + "_he", colFam, colQual, tsLastWeek - 900000,
				new Value("110".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek - 900000,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 1) + "_he", colFam, colQual, tsLastWeek,
				new Value("150".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 1) + "_he", colFam, colQual, tsLastWeek - 900000,
				new Value("220".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek - 900000,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 1) + "_he", colFam, colQual, tsCurrent,
				new Value("300".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsCurrent,
				new Value("1-1-1-1-2".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		//fifth device
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 2) + "_he", colFam, colQual, tsTwoWeeksAgo,
				new Value("110".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsTwoWeeksAgo,
				new Value("1-1-1-1-3".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 2) + "_he", colFam, colQual, tsLastWeek - 900000,
				new Value("140".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek - 900000,
				new Value("1-1-1-1-3".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 2) + "_he", colFam, colQual, tsLastWeek,
				new Value("220".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek,
				new Value("1-1-1-1-3".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 2) + "_he", colFam, colQual, tsLastWeek + 900000,
				new Value("444".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsLastWeek + 900000,
				new Value("1-1-1-1-3".getBytes()));
		accumulo.addMutation(tableName, mutation);
		
		mutation = accumulo.newMutation(
				String.valueOf(consumptionId + 2) + "_he", colFam, colQual, tsCurrent,
				new Value("640".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, tsCurrent,
				new Value("1-1-1-1-3".getBytes()));
		accumulo.addMutation(tableName, mutation);
	}
}
