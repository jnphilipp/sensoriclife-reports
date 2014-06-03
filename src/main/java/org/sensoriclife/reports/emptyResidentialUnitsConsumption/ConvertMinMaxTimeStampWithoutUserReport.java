package org.sensoriclife.reports.emptyResidentialUnitsConsumption;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.sensoriclife.util.Helpers;
import org.sensoriclife.world.DeviceUnit;

public class ConvertMinMaxTimeStampWithoutUserReport extends Configured implements Tool{
	
	public static boolean test = true;
	
	public static void runConvert(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ConvertMinMaxTimeStampWithoutUserReport(),args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		/* 
		 * args[0] = outputTableName
		 * 
		 * args[1] = (date) timeStamp -> min
		 * args[2] = (date) timeStamp -> max
		 * args[3] = (boolean) onlyInTimeRange -> is true, when the compute inside the timerange
		 * args[4] = (long) reportTimestamp
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[0]);
		conf.setBoolean("onlyInTimeRange", args[3].equals("true"));
		conf.setLong("reportTimestamp", Long.parseLong(args[4]));
		
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy HH:mm:ss" );
			Date d  = formatter.parse( args[1]);// day.month.year"
			conf.setLong("minTimestamp", d.getTime());
		}
		catch ( ParseException e )
		{
			try
			{
				DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				Date d  = formatter.parse( args[1]);// day.month.year hour:minut:second
				conf.setLong("minTimestamp", d.getTime());
			}
			catch ( ParseException ee ) {}
		}
		
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy HH:mm:ss" );
			Date d  = formatter.parse( args[2]);// day.month.year"
			conf.setLong("maxTimestamp", d.getTime());
		}
		catch ( ParseException e )
		{
			try
			{
				DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				Date d  = formatter.parse( args[2]);// day.month.year"
				conf.setLong("maxTimestamp", d.getTime());
			}
			catch ( ParseException ee ) {}
		}
			
		Job job = Job.getInstance(conf);
		job.setJobName(ConvertMinMaxTimeStampWithoutUserReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(ConvertWithoutUserMapper.class);
		job.setReducerClass(ConvertWithoutUserReducer.class);

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
		AccumuloInputFormat.setInputTableName(job, Config.getProperty("accumulo.tableName"));//tablename
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		Set cols = new HashSet();
		cols.add(new Pair(new Text(Helpers.toByteArray("device")), new Text(Helpers.toByteArray("amount"))));
		cols.add(new Pair(new Text(Helpers.toByteArray("residential")), new Text(Helpers.toByteArray("id"))));
		cols.add(new Pair(new Text(Helpers.toByteArray("user")), new Text(Helpers.toByteArray("id"))));
		AccumuloInputFormat.fetchColumns(job, cols);
		
		AccumuloOutputFormat.setConnectorInfo(job, Config.getProperty("accumulo.user"), new PasswordToken(Config.getProperty("accumulo.password")));
		AccumuloOutputFormat.setDefaultTableName(job, args[0]);
		AccumuloOutputFormat.setCreateTables(job, true);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DeviceUnit.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
