package org.sensoriclife.reports.consumptionInDaytime;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConsumptionInDaytimeStarter extends Configured implements Tool {
	
	public static void runConvert(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ConsumptionInDaytimeStarter(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

		DateFormat formatter = new SimpleDateFormat("dd.MM.yyyy");

		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[6]);
		conf.setLong("minTimestamp", formatter.parse(args[9]).getTime());
		conf.setLong("maxTimestamp", formatter.parse(args[10]).getTime());
		conf.setStrings("reportTimestamp", args[12]);
		conf.setStrings("district", args[11]);


		Job job = Job.getInstance(conf);
		job.setJobName(ConsumptionInDaytimeStarter.class.getName());
		job.setJarByClass(this.getClass());
		job.setMapperClass(ConsumptionInDaytimeMapper.class);
		job.setReducerClass(ConsumptionInDaytimeReducer.class);

		if (ConsumptionInDaytimeReport.test) {
			AccumuloInputFormat.setMockInstance(job, args[1]); 
			AccumuloOutputFormat.setMockInstance(job, args[5]);
		} else {
			AccumuloInputFormat.setZooKeeperInstance(job, args[1], "zooserver-one,zooserver-two");
			AccumuloOutputFormat.setZooKeeperInstance(job, args[5], "zooserver-one,zooserver-two");
		}

		AccumuloInputFormat.setConnectorInfo(job, args[3], new PasswordToken(args[4])); 
		AccumuloInputFormat.setInputTableName(job, args[2]);
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());

		AccumuloOutputFormat.setConnectorInfo(job, args[7], new PasswordToken(args[8]));
		AccumuloOutputFormat.setDefaultTableName(job, args[6]);
		AccumuloOutputFormat.setCreateTables(job, true);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}

}
