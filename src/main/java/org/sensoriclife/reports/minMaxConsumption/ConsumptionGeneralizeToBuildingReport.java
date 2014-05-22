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

public class ConsumptionGeneralizeToBuildingReport extends Configured implements Tool{
	
	public static boolean test = true;
	
	public static void runConsumptionGeneralize(String[] args) throws Exception {
		/*
		 * args[0] = reportName
		 * 
		 * args[1] = InstanceName
		 * args[2] = TableName
		 * args[3] = UserName
		 * args[4] = Password
		 * 
		 */
		
		// run the map reduce job to read the edge table and populate the node
		// table
		ToolRunner.run(new Configuration(), new ConsumptionGeneralizeToBuildingReport(),args);

	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		/*
		 * args[0] = reportName
		 * 
		 * args[1] = InstanceName
		 * args[2] = TableName
		 * args[3] = UserName
		 * args[4] = Password
		  		  
		 */
		
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[2]);
		
		Job job = Job.getInstance(conf);
		job.setJobName(ConsumptionGeneralizeToBuildingReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(ConsumptionGeneralizeToBuildingMapper.class);
		job.setReducerClass(ConsumptionGeneralizeToBuildingReducer.class);

		if(test)
		{
			AccumuloInputFormat.setMockInstance(job, args[1]); // Instanzname
			AccumuloOutputFormat.setMockInstance(job, args[1]);
		}
		else
		{
			AccumuloInputFormat.setZooKeeperInstance(job, args[1], "zooserver-one,zooserver-two");
			AccumuloOutputFormat.setZooKeeperInstance(job, args[1], "zooserver-one,zooserver-two");
		}
		
		AccumuloInputFormat.setConnectorInfo(job, args[3], new PasswordToken(args[4])); //username,password
		AccumuloInputFormat.setInputTableName(job, args[2]);//tablename
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		
		AccumuloOutputFormat.setConnectorInfo(job, args[3], new PasswordToken(args[4]));
		AccumuloOutputFormat.setDefaultTableName(job, args[2]);
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