package org.sensoriclife.reports.minMaxConsumption;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.Config;
import org.sensoriclife.world.DeviceUnit;

/**
 * 
 * @author marcel, yves
 *
 */
public class MinMaxReport extends Configured implements Tool {
	public static void runMinMax(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MinMaxReport(),args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", Config.getProperty("reports.min_max_consumption.output_table.name"));
		conf.setInt("selectModus", Integer.parseInt(args[5]));
		conf.setLong("reportTimestamp", new Long(args[6]));
		
		Job job = Job.getInstance(conf);
		job.setJobName(MinMaxReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(MinMaxMapper.class);
		job.setReducerClass(MinMaxReducer.class);

		ClientConfiguration c = new ClientConfiguration();
		c.withInstance(Config.getProperty("accumulo.name"));
		c.withZkHosts(Config.getProperty("reports.min_max_consumption.output_table.name.zooServers"));
	
		AccumuloInputFormat.setConnectorInfo(job, Config.getProperty("reports.min_max_consumption.output_table.user"), new PasswordToken(Config.getProperty("reports.min_max_consumption.output_table.password")));
		AccumuloInputFormat.setInputTableName(job, Config.getProperty("reports.min_max_consumption.output_table.name"));
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		AccumuloInputFormat.setZooKeeperInstance(job, c);

		AccumuloOutputFormat.setConnectorInfo(job, Config.getProperty("reports.min_max_consumption.output_table.user"), new PasswordToken(Config.getProperty("reports.min_max_consumption.output_table.password")));
		AccumuloOutputFormat.setDefaultTableName(job, Config.getProperty("reports.min_max_consumption.output_table.name"));
		AccumuloOutputFormat.setCreateTables(job, false);
		AccumuloOutputFormat.setZooKeeperInstance(job, c);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DeviceUnit.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
