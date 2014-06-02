package org.sensoriclife.reports.dayWithMaxConsumption.secondJob;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
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
import org.sensoriclife.Config;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.world.Consumption;

public class DaysWithMaxConsumptionReport extends Configured implements Tool {

	/**
	 * runs the whole process of the second map reduce job, including
	 * configuration, inserting of test data and execution of the job.
	 * 
	 * @return Accumulo
	 * @throws Exception
	 */
	public static void runSecondJob(Accumulo accumulo) throws Exception {

		Connector connector = accumulo.getConnector();

		accumulo.createTable(Config
				.getProperty("report.dayWithMaxConsumption.outputTableName"),
				false);

		/*
		Iterator<Entry<Key, Value>> scanner = accumulo.scanAll(Config
				.getProperty("report.dayWithMaxConsumption.inputTableName"),
				new Authorizations());
		while (scanner.hasNext()) {
			Entry<Key, Value> entry = scanner.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}
		*/

		String[] args = new String[0];
		ToolRunner.run(new Configuration(), new DaysWithMaxConsumptionReport(),
				args);

		// print outputtable
		Iterator<Entry<Key, Value>> scanner2 = accumulo.scanAll(Config
				.getProperty("report.dayWithMaxConsumption.outputTableName"),
				new Authorizations());
		while (scanner2.hasNext()) {
			Entry<Key, Value> entry = scanner2.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}

		// this is for production mode - save storage
		connector
				.tableOperations()
				.delete(Config
						.getProperty("report.dayWithMaxConsumption.inputTableName"));
	}

	/**
	 * executes the accumulo map reduce job.
	 */
	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJobName(DaysWithMaxConsumptionReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(DaysWithMaxConsumptionMapper.class);
		job.setReducerClass(DaysWithMaxConsumptionReducer.class);

		AccumuloInputFormat.setMockInstance(job,
				Config.getProperty("mockInstanceName"));
		AccumuloInputFormat.setConnectorInfo(job, Config
				.getProperty("accumulo.username"),
				new PasswordToken(Config.getProperty("accumulo.password")));
		AccumuloInputFormat.setInputTableName(job, Config
				.getProperty("report.dayWithMaxConsumption.inputTableName"));
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());

		AccumuloOutputFormat.setConnectorInfo(job, Config
				.getProperty("accumulo.username"),
				new PasswordToken(Config.getProperty("accumulo.password")));
		AccumuloOutputFormat.setDefaultTableName(job, Config
				.getProperty("report.dayWithMaxConsumption.outputTableName"));
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setMockInstance(job,
				Config.getProperty("mockInstanceName"));

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Consumption.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
