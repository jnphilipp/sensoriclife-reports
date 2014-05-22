package org.sensoriclife.reports.unusualRiseOfConsumption.secondob;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.MockInstanceConfiguration;
import org.sensoriclife.world.ResidentialUnit;

public class UnusualRiseOfHeatingConsumptionReport extends Configured implements
		Tool {

	/**
	 * runs the whole process of the second map reduce job, including configuration, inserting of test data and execution of the job.
	 * @return Accumulo
	 * @throws Exception
	 */
	public static Accumulo runSecondJob(Accumulo accumulo) throws Exception {

		MockInstanceConfiguration mockConfig = MockInstanceConfiguration
				.getInstance();
		mockConfig.setMockInstanceName("mockInstance");
		mockConfig.setInputTableName("HeatingConsumption");
		mockConfig.setOutputTableName("UnusualRiseOfConsumption");
		mockConfig.setUserName("");
		mockConfig.setPassword("");

		Iterator<Entry<Key, Value>> scanner = accumulo.scanAll(mockConfig
				.getInputTableName());

		while (scanner.hasNext()) {
			Entry<Key, Value> entry = scanner.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}

		ToolRunner.run(new Configuration(),
				new UnusualRiseOfHeatingConsumptionReport(),
				mockConfig.getConfigAsStringArray());

		Iterator<Entry<Key, Value>> scanner2 = accumulo.scanAll(mockConfig
				.getOutputTableName());

		while (scanner2.hasNext()) {
			Entry<Key, Value> entry = scanner2.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}

		Connector connector = accumulo.getConnector();
		connector.tableOperations().delete(mockConfig.getInputTableName());

		return accumulo;

	}

	/**
	 * executes the accumulo map reduce job.
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		//time interval: weekly
		conf.setLong("minTimestamp", 1);
		conf.setLong("maxTimestamp", 2);

		Job job = Job.getInstance(conf);

		job.setJobName(UnusualRiseOfHeatingConsumptionReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(UnusualRiseOfHeatingConsumptionMapper.class);
		job.setReducerClass(UnusualRiseOfHeatingConsumptionReducer.class);

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
}
