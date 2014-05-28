package org.sensoriclife.reports.minMaxConsumption;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConsumptionGeneralizeToBuildingReport extends Configured implements Tool {
	/**
	 * 
	 * @param args reportName, InstanceName, TableName, UserName, Password, -, reportTimestamp
	 * @throws Exception 
	 */
	public static void runConsumptionGeneralize(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ConsumptionGeneralizeToBuildingReport(),args);
	}
	
	/**
	 * 
	 * @param args reportName, InstanceName, TableName, UserName, Password, -, reportTimestamp, zooServers
	 * @return 
	 * @throws Exception 
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[2]);
		conf.setLong("reportTimestamp", new Long(args[6]));
		
		Job job = Job.getInstance(conf);
		job.setJobName(ConsumptionGeneralizeToBuildingReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(ConsumptionGeneralizeToBuildingMapper.class);
		job.setReducerClass(ConsumptionGeneralizeToBuildingReducer.class);

		ClientConfiguration c = new ClientConfiguration();
		c.withInstance(args[1]);
		c.withZkHosts(args[7]);
		
		AccumuloInputFormat.setConnectorInfo(job, args[3], new PasswordToken(args[4])); //username,password
		AccumuloInputFormat.setInputTableName(job, args[2]);//tablename
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		AccumuloInputFormat.setZooKeeperInstance(job, c);
		
		AccumuloOutputFormat.setConnectorInfo(job, args[3], new PasswordToken(args[4]));
		AccumuloOutputFormat.setDefaultTableName(job, args[2]);
		AccumuloOutputFormat.setCreateTables(job, false);
		AccumuloOutputFormat.setZooKeeperInstance(job, c);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}