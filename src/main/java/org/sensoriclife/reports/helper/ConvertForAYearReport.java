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
import org.sensoriclife.world.DeviceUnit;

public class ConvertForAYearReport extends Configured implements Tool{
	
	public static boolean test = true;
	
	public static void runConvert(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ConvertForAYearReport(),args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		/*
		 * args[0] = inputInstanceName
		 * args[1] = inputZooServers
		 * args[2] = inputTableName
		 * args[3] = inputUserName
		 * args[4] = inputPassword
		 * 
		 * args[5] = outputInstanceName
		 * args[6] = outputZooServers
		 * args[7] = outputTableName
		 * args[8] = outputUserName
		 * args[9] = outputPassword
		 * 
		 * args[10] = (Times) timestamp (max)
		 * args[11] = (boolean) onlyInTimeRange -> is true, when the compute inside the timeRange
		 * args[12] = (long) reportTimestamp
		 */
		
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[7]);
		conf.setBoolean("onlyInTimeRange", args[11].equals("true"));
		conf.setLong("reportTimestamp", Long.parseLong(args[12]));
		
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy HH:mm:ss" );
			Date d  = formatter.parse( args[10]);// day.month.year"
			conf.setLong("maxTimestamp", d.getTime());
		}
		catch ( ParseException e )
		{
			try
			{
				DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				Date d  = formatter.parse( args[10]);// day.month.year hour:minut:second
				conf.setLong("maxTimestamp", d.getTime());
			}
			catch ( ParseException ee ) {}
		}
		
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy HH:mm:ss" );
			Date date  = formatter.parse( args[10]);// day.month.year"
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
				Date date  = formatter.parse( args[10]);// day.month.year hour:minut:second
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
			AccumuloInputFormat.setMockInstance(job, args[0]); // Instanzname
			AccumuloOutputFormat.setMockInstance(job, args[5]);
		}
		else
		{
			AccumuloInputFormat.setZooKeeperInstance(job, args[0], args[1]);
			AccumuloOutputFormat.setZooKeeperInstance(job, args[5], args[6]);
		}
		
		AccumuloInputFormat.setConnectorInfo(job, args[3], new PasswordToken(args[4])); //username,password
		AccumuloInputFormat.setInputTableName(job, args[2]);//tablename
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		Set cols = new HashSet();
		cols.add(new Pair(new Text("device"), new Text("amount")));
		cols.add(new Pair(new Text("residential"), new Text("id")));
		AccumuloInputFormat.fetchColumns(job, cols);
		
		AccumuloOutputFormat.setConnectorInfo(job, args[8], new PasswordToken(args[9]));
		AccumuloOutputFormat.setDefaultTableName(job, args[7]);
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
