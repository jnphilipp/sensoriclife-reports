package org.sensoriclife.minMaxConsumption;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.App;
import org.sensoriclife.reports.world.ResidentialUnit;

import com.beust.jcommander.Parameter;

/**
 * 
 * @author marcel
 *
 */
public class MMCMain extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(),
				new MMCMain(), args);
		System.exit(res);
	}

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
		
		Logger.info("Start writing Mockdata");
		// write mockdata -> two lines
		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect();

		String mockTablename = "electricityConsumption";
		accumulo.createTable(mockTablename);
		String colFam = "electricity";
		String colQual = "amount";
		long timestamp = System.currentTimeMillis();
		
		accumulo.write(mockTablename, "row1", colFam, colQual, timestamp, new Value("5".getBytes()));
		accumulo.write(mockTablename, "row2", colFam, colQual, timestamp, new Value("3".getBytes()));

		Iterator<Entry<Key,Value>> scanner = accumulo.scanAll(mockTablename);
		
		while(scanner.hasNext()){
			Entry<Key, Value> entry = scanner.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: " + entry.getValue().toString());
		}
		
		
		
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(this.getClass());
		AccumuloInputFormat.setMockInstance(job, "mockInstance");

		// AccumuloInputFormat.setZooKeeperInstance(job, "myinstance",
		// "zooserver-one,zooserver-two");

		// user must have a create table permission
		AccumuloInputFormat.setConnectorInfo(job, App
				.getProperty("accumulo.username"), new PasswordToken(App
				.getProperty("accumulo.password").getBytes()));
		AccumuloInputFormat.setInputTableName(job, mockTablename);
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		
		AccumuloOutputFormat.setConnectorInfo(job, App
				.getProperty("accumulo.username"), new PasswordToken(App
				.getProperty("accumulo.password").getBytes()));

		AccumuloOutputFormat.setMockInstance(job, "mockInstance");
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setDefaultTableName(job, "minMaxConsumption");

		// AccumuloOutputFormat.setZooKeeperInstance(job, "myinstance",
		// "zooserver-one,zooserver-two");

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setMapperClass(AccumuloMMCMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(ResidentialUnit.class);
		job.setCombinerClass(AccumuloMMCReducer.class);
		job.setReducerClass(AccumuloMMCReducer.class);

		// job.setNumReduceTasks(opts.reducers);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/output"));
		job.waitForCompletion(true);
		
		return job.isSuccessful() ? 0 : 1;
	}
}