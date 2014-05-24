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
import org.sensoriclife.Config;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.world.ResidentialUnit;

public class UnusualRiseOfHeatingConsumptionReport extends Configured implements
		Tool {

	/**
	 * runs the whole process of the second map reduce job, including configuration, inserting of test data and execution of the job.
	 * @return Accumulo
	 * @throws Exception
	 */
	public static Accumulo runSecondJob(Accumulo accumulo) throws Exception {
		
		Config conf = Config.getInstance();
		//config for mockinstance
		conf.getProperties().setProperty("mockInstanceName", "mockInstance");
		conf.getProperties().setProperty("inputTableName", "HeatingConsumption");
		conf.getProperties().setProperty("outputTableName", "UnusualRiseOfConsumption");
		conf.getProperties().setProperty("username", "");
		conf.getProperties().setProperty("password", "");
		
		//config for map reduce job
		//time interval: weekly
		conf.getProperties().setProperty("minTimestamp", "1");
		conf.getProperties().setProperty("maxTimestamp", "2");
		
		Iterator<Entry<Key, Value>> scanner = accumulo.scanAll(Config.getProperty("inputTableName"), new Authorizations());

		while (scanner.hasNext()) {
			Entry<Key, Value> entry = scanner.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}

		String[] args = new String[0];
		ToolRunner.run(new Configuration(),
				new UnusualRiseOfHeatingConsumptionReport(), 
				args);

		Iterator<Entry<Key, Value>> scanner2 = accumulo.scanAll(Config.getProperty("outputTableName"), new Authorizations());

		while (scanner2.hasNext()) {
			Entry<Key, Value> entry = scanner2.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}

		Connector connector = accumulo.getConnector();
		connector.tableOperations().delete(Config.getProperty("inputTableName"));

		return accumulo;

	}

	/**
	 * executes the accumulo map reduce job.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setJobName(UnusualRiseOfHeatingConsumptionReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(UnusualRiseOfHeatingConsumptionMapper.class);
		job.setReducerClass(UnusualRiseOfHeatingConsumptionReducer.class);

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
}
