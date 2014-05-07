package org.sensoriclife.minMaxConsumption;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.reports.world.ResidentialUnit;

import com.beust.jcommander.Parameter;
/**
* A simple map reduce job that computes the unique column families and column qualifiers in a table. This example shows one way to run against an offline
* table.
*/
public class MMCMain extends Configured implements Tool {
	
	private static final Text EMPTY = new Text();

	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--output", description = "output directory")
		String output;
		@Parameter(names = "--reducers", description = "number of reducers to use", required = true)
		int reducers;
		@Parameter(names = "--offline", description = "run against an offline table")
		boolean offline = false;
	}

	@Override
	public int run(String[] args) throws Exception {

		Opts opts = new Opts();
		opts.parseArgs(MMCMain.class.getName(), args);
		String jobName = this.getClass().getSimpleName() + "_" + System.currentTimeMillis();
		Job job = Job.getInstance(getConf(), jobName);
		job.setJarByClass(this.getClass());
		String clone = opts.tableName;
		Connector conn = null;
		opts.setAccumuloConfigs(job);
		if (opts.offline) {
			conn = opts.getConnector();
			clone = opts.tableName + "_" + jobName;
			conn.tableOperations().clone(opts.tableName, clone, true, new HashMap<String,String>(), new HashSet<String>());
			conn.tableOperations().offline(clone);
			AccumuloInputFormat.setOfflineTableScan(job, true);
			AccumuloInputFormat.setInputTableName(job, clone);
		}
		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setMapperClass(AccumuloMMCMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(ResidentialUnit.class);
		job.setCombinerClass(AccumuloMMCReducer.class);
		job.setReducerClass(AccumuloMMCReducer.class);
		job.setNumReduceTasks(opts.reducers);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(opts.output));
		job.waitForCompletion(true);

		if (opts.offline) {
			conn.tableOperations().delete(clone);
		}
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(), new MMCMain(), args);
		
		
		/*
		
		Job job = new Job(getConf());
		AccumuloInputFormat.setInputInfo(job,
		        "user",
		        "passwd".getBytes(),
		        "table",
		        new Authorizations());

		AccumuloInputFormat.setMockInstance(job, "mockInstance");
		//AccumuloInputFormat.setZooKeeperInstance(job, "myinstance",
		  //      "zooserver-one,zooserver-two");	
		
		boolean createTables = true;
		String defaultTable = "mytable";

		AccumuloOutputFormat.setOutputInfo(job,
		        "user",
		        "passwd".getBytes(),
		        createTables,
		        defaultTable);

		AccumuloOutputFormat.setMockInstance(job, "mockInstance");
		//AccumuloOutputFormat.setZooKeeperInstance(job, "myinstance",
		  //      "zooserver-one,zooserver-two");
		
		Accumulo accumulo = Accumulo.getInstance();
		//connect to accumulo
		accumulo.connect();
		//load data from accumulo - two tables needed (electricity_consumption and residentialUnit with resi_id and electric_id as parameters))
		accumulo.scanByKey("electricity_consumption", key, attribute, authentication);
		//start job (including map and reduce -> with report)
		//start bin/tool.sh script
		*/
		

		
		System.exit(res);
	}
}