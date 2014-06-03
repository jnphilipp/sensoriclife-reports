package org.sensoriclife.reports.helper;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
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

public class ConvertForAYearReport extends Configured implements Tool{
	
	public static boolean test = true;
	
	public static void runConvert(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ConvertForAYearReport(),args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		/*
		 * args[0] = outputTableName
		 * 
		 * args[1] = (Times) timestamp (max)
		 * args[2] = (boolean) onlyInTimeRange -> is true, when the compute inside the timeRange
		 * args[3] = (long) reportTimestamp
		 */
		
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[0]);
		conf.setBoolean("onlyInTimeRange", args[2].equals("true"));
		conf.setLong("reportTimestamp", Long.parseLong(args[3]));
		
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy HH:mm:ss" );
			Date d  = formatter.parse( args[1]);// day.month.year"
			conf.setLong("maxTimestamp", d.getTime());
		}
		catch ( ParseException e )
		{
			try
			{
				DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				Date d  = formatter.parse( args[1]);// day.month.year hour:minut:second
				conf.setLong("maxTimestamp", d.getTime());
			}
			catch ( ParseException ee ) {}
		}
		
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy HH:mm:ss" );
			Date date  = formatter.parse( args[1]);// day.month.year"
			Calendar caIn = new GregorianCalendar();
			caIn.setTime(date);
			int year = caIn.get(Calendar.YEAR);
			int month = caIn.get(Calendar.MONTH);
			int day = caIn.get(Calendar.DAY_OF_MONTH);
			int hour = caIn.get(Calendar.HOUR_OF_DAY);  
			int minute = caIn.get(Calendar.MINUTE);
			int second = caIn.get(Calendar.SECOND);
			year -= 1;
			Calendar caOut = new GregorianCalendar(year,month,day,hour,minute,second);
			conf.setLong("minTimestamp", caOut.getTimeInMillis());
		}
		catch ( ParseException e )
		{
			try
			{
				DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				Date date  = formatter.parse( args[1]);// day.month.year hour:minut:second
				Calendar caIn = new GregorianCalendar();
				caIn.setTime(date);
				int year = caIn.get(Calendar.YEAR);
				int month = caIn.get(Calendar.MONTH);
				int day = caIn.get(Calendar.DAY_OF_MONTH);
				year -= 1;
				
				Calendar caOut = new GregorianCalendar(year,month,day);
				conf.setLong("minTimestamp", caOut.getTimeInMillis());
			}
			catch ( ParseException ee ) {}
		}
		
		Job job = Job.getInstance(conf);
		job.setJobName(ConvertForAYearReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(ConvertMapper.class);
		job.setReducerClass(ConvertReducer.class);

		
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
