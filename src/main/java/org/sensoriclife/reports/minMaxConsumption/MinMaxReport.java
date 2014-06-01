package org.sensoriclife.reports.minMaxConsumption;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
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
	
	public static boolean test = true;
	
	
	public static void runMinMax(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MinMaxReport(),args);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		/*
		 * args[0] = (4|5) select to get Max Min for residentialUnit(5) or building(4)
		 * args[1] = reportTimestamp
		 */
		Config.getProperty("reports.minMaxConsumption.outputTable.name");
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", Config.getProperty("reports.minMaxConsumption.outputTable.tableName"));
		conf.setInt("selectModus", Integer.parseInt(args[0]));
		conf.setLong("reportTimestamp", new Long(args[1]));
		
		Job job = Job.getInstance(conf);
		job.setJobName(MinMaxReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(MinMaxMapper.class);
		job.setReducerClass(MinMaxReducer.class);

		if(test)
		{
			AccumuloInputFormat.setMockInstance(job, Config.getProperty("reports.minMaxConsumption.outputTable.name")); // Instanzname
			AccumuloOutputFormat.setMockInstance(job, Config.getProperty("reports.minMaxConsumption.outputTable.name"));
		}
		else
		{
			AccumuloInputFormat.setZooKeeperInstance(job, Config.getProperty("reports.minMaxConsumption.outputTable.name"), Config.getProperty("reports.minMaxConsumption.outputTable.zooServers"));
			AccumuloOutputFormat.setZooKeeperInstance(job, Config.getProperty("reports.minMaxConsumption.outputTable.name"), Config.getProperty("reports.minMaxConsumption.outputTable.zooServers"));
		}
		
		AccumuloInputFormat.setConnectorInfo(job, Config.getProperty("reports.minMaxConsumption.outputTable.user"),
				new PasswordToken(Config.getProperty("reports.minMaxConsumption.outputTable.password"))); //username,password
		AccumuloInputFormat.setInputTableName(job, Config.getProperty("reports.minMaxConsumption.outputTable.tableName"));//tablename
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		//Filter
		Set cols = new HashSet();
		if(args[0].equals("5")){
			cols.add(new Pair(new Text("residentialUnit"), new Text("amount")));
		}
		else if(args[0].equals("4")){
			cols.add(new Pair(new Text("building"), new Text("amount")));
		}
		AccumuloInputFormat.fetchColumns(job, cols);
		
		AccumuloOutputFormat.setConnectorInfo(job, Config.getProperty("reports.minMaxConsumption.outputTable.user"),
				new PasswordToken(Config.getProperty("reports.minMaxConsumption.outputTable.password"))); //username,password
		AccumuloOutputFormat.setDefaultTableName(job, Config.getProperty("reports.minMaxConsumption.outputTable.tableName"));
		AccumuloOutputFormat.setCreateTables(job, false);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DeviceUnit.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
