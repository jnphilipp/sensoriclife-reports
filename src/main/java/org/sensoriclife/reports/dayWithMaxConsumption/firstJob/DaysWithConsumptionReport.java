package org.sensoriclife.reports.dayWithMaxConsumption.firstJob;

import java.util.Calendar;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
import org.sensoriclife.Config;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.world.ResidentialUnit;

public class DaysWithConsumptionReport extends Configured implements Tool {

	/**
	 * runs the whole process of the first map reduce job, including
	 * configuration, inserting of test data and execution of the job.
	 * 
	 * @return Accumulo
	 * @throws Exception
	 */
	public static Accumulo runFirstJob() throws Exception {

		// config for map reduce job
		// minTimestamp ~ 01.01.LastYear 0:00.00
		Calendar cal = Calendar.getInstance();
		cal.set(cal.get(Calendar.YEAR) - 1, 0, 1, 0, 0, 0);
		cal.getTimeInMillis();
		Config conf = Config.getInstance();
		conf.getProperties().setProperty("minTimestamp",
				String.valueOf(cal.getTimeInMillis()));
		// maxTimestamp ~ 31.12.LastYear 23:59.59
		cal.set(cal.get(Calendar.YEAR), 11, 31, 23, 59, 59);
		conf.getProperties().setProperty("maxTimestamp",
				String.valueOf(cal.getTimeInMillis()));

		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect();

		accumulo.createTable(
				Config.getProperty("report.daysWithConsumption.inputTableName"),
				false);
		accumulo.createTable(Config
				.getProperty("report.daysWithConsumption.outputTableName"),
				false);

		insertData(accumulo,
				Config.getProperty("report.daysWithConsumption.inputTableName"));

		/*
		Iterator<Entry<Key, Value>> scanner = accumulo
				.scanAll(
						Config.getProperty("report.daysWithConsumption.inputTableName"),
						new Authorizations());
		while (scanner.hasNext()) {
			Entry<Key, Value> entry = scanner.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}
		*/

		String[] args = new String[0];
		ToolRunner.run(new Configuration(), new DaysWithConsumptionReport(),
				args);

		/*
		Iterator<Entry<Key, Value>> scanner2 = accumulo.scanAll(Config
				.getProperty("report.daysWithConsumption.outputTableName"),
				new Authorizations());
		while (scanner2.hasNext()) {
			Entry<Key, Value> entry = scanner2.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}
		*/
		
		return accumulo;
	}

	/**
	 * executes the accumulo map reduce job.
	 */
	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJobName(DaysWithConsumptionReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(DaysWithConsumptionMapper.class);
		job.setReducerClass(DaysWithConsumptionReducer.class);

		AccumuloInputFormat.setMockInstance(job,
				Config.getProperty("mockInstanceName"));
		AccumuloInputFormat.setConnectorInfo(job, Config
				.getProperty("accumulo.username"),
				new PasswordToken(Config.getProperty("accumulo.password")));
		AccumuloInputFormat
				.setInputTableName(
						job,
						Config.getProperty("report.daysWithConsumption.inputTableName"));
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());

		AccumuloOutputFormat.setConnectorInfo(job, Config
				.getProperty("accumulo.username"),
				new PasswordToken(Config.getProperty("accumulo.password")));
		AccumuloOutputFormat.setDefaultTableName(job, Config
				.getProperty("report.daysWithConsumption.outputTableName"));
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setMockInstance(job,
				Config.getProperty("mockInstanceName"));

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ResidentialUnit.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * inserts some test data, adapted for this report.
	 * 
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

		// data for last year
		Calendar cal = Calendar.getInstance();
		// 20. may
		cal.set(cal.get(Calendar.YEAR) - 1, 4, 20, 0, 0, 0);
		cal.getTimeInMillis();
		long ts1 = cal.getTimeInMillis();

		Mutation mutation = accumulo.newMutation(String.valueOf(consumptionId)
				+ "_wc", colFam, colQual, ts1, new Value("4".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts1,
				new Value("1-2-10".getBytes()));
		accumulo.addMutation(tableName, mutation);

		// one day = 86400000 ms
		long ts2 = ts1 + 86400000;

		mutation = accumulo.newMutation(String.valueOf(consumptionId + 1)
				+ "_wc", colFam, colQual, ts2, new Value("125".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts2,
				new Value("1-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		ts1 += 900000;

		mutation = accumulo.newMutation(String.valueOf(consumptionId) + "_wh",
				colFam, colQual, ts1, new Value("12".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts1,
				new Value("1-2-10".getBytes()));
		accumulo.addMutation(tableName, mutation);

		ts2 += 86400000;

		mutation = accumulo.newMutation(String.valueOf(consumptionId + 1)
				+ "_wh", colFam, colQual, ts2, new Value("13".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts2,
				new Value("1-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		long ts3 = ts2 + 86400000;

		mutation = accumulo.newMutation(String.valueOf(consumptionId) + "_el",
				colFam, colQual, ts3, new Value("111".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts3,
				new Value("1-2-1".getBytes()));
		accumulo.addMutation(tableName, mutation);

		ts2 += 900000;

		mutation = accumulo.newMutation(String.valueOf(consumptionId + 1)
				+ "_el", colFam, colQual, ts2, new Value("7".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts2,
				new Value("1-2-0".getBytes()));
		accumulo.addMutation(tableName, mutation);

		long ts10 = ts1 + 9 * 86400000;

		mutation = accumulo.newMutation(String.valueOf(consumptionId + 2)
				+ "_el", colFam, colQual, ts10, new Value("42".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts10,
				new Value("1-1-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		ts2 += 900000;

		mutation = accumulo.newMutation(String.valueOf(consumptionId + 3)
				+ "_el", colFam, colQual, ts2, new Value("105".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts2,
				new Value("2-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		ts3 += 900000;

		mutation = accumulo.newMutation(String.valueOf(consumptionId + 4)
				+ "_el", colFam, colQual, ts3, new Value("1".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts3,
				new Value("3-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.newMutation(String.valueOf(consumptionId) + "_he",
				colFam, colQual, ts3, new Value("18".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts3,
				new Value("1-2-1".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.newMutation(String.valueOf(consumptionId + 1)
				+ "_he", colFam, colQual, ts2, new Value("20".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, ts2,
				new Value("1-2-0".getBytes()));

		accumulo.addMutation(tableName, mutation);
	}
}
