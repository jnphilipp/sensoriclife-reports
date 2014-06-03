package org.sensoriclife.reports.minMaxConsumption;

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
import org.sensoriclife.Config;

public class ConsumptionGeneralizeToBuildingReport extends Configured implements Tool{
	
	public static boolean test = true;
	
	public static void runConsumptionGeneralize(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ConsumptionGeneralizeToBuildingReport(),args);
	}
	@Override
	public int run(String[] args) throws Exception {
		
		/*
		 * args[0] = TableName
		 * args[1] = reportTimestamp	  
		 */
		
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[0]);
		conf.setLong("reportTimestamp", new Long(args[1]));
		
		Job job = Job.getInstance(conf);
		job.setJobName(ConsumptionGeneralizeToBuildingReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(ConsumptionGeneralizeToBuildingMapper.class);
		job.setReducerClass(ConsumptionGeneralizeToBuildingReducer.class);

		if(test)
		{
			AccumuloInputFormat.setMockInstance(job, Config.getProperty("accumulo.name")); // Instanzname
			AccumuloOutputFormat.setMockInstance(job, Config.getProperty("accumulo.name"));
		}
		else
		{
			AccumuloInputFormat.setZooKeeperInstance(job, Config.getProperty("accumulo.name"), Config.getProperty("accumulo.zooServers"));
			AccumuloOutputFormat.setZooKeeperInstance(job, Config.getProperty("accumulo.name"), Config.getProperty("accumulo.zooServers"));
		}
		
		AccumuloInputFormat.setConnectorInfo(job, Config.getProperty("accumulo.user"), new PasswordToken(Config.getProperty("accumulo.password"))); //username,password
		AccumuloInputFormat.setInputTableName(job, args[0]);//tablename
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		
		AccumuloOutputFormat.setConnectorInfo(job, Config.getProperty("accumulo.user"), new PasswordToken(Config.getProperty("accumulo.password")));
		AccumuloOutputFormat.setDefaultTableName(job, args[0]);
		AccumuloOutputFormat.setCreateTables(job, false);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
	
}