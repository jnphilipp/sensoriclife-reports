package org.sensoriclife.reports.emptyResidentialUnitsConsumption;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
import org.sensoriclife.world.DeviceUnit;

public class ConvertMinMaxTimeStampWithoutUserReport extends Configured implements Tool{
	
	public static boolean test = true;
	
	public static void runConvert(String[] args) throws Exception {
		/*
		 * args[0] = reportName
		 * 
		 * args[1] = inputInstanceName
		 * args[2] = inputTableName
		 * args[3] = inputUserName
		 * args[4] = inputPassword
		 * 
		 * args[5] = outputInstanceName
		 * args[6] = outputTableName
		 * args[7] = outputUserName
		 * args[8] = outputPassword
		 * 
		 * args[9] = (date) timeStamp -> min
		 * args[10] = (date) timeStamp -> max
		 * args[11] = (boolean) onlyYear -> is true, when the compute inside the year
		 * args[12] = (day;month;year) pastLimitation
		 * args[13] = (long) reportTimestamp
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		// run the map reduce job to read the edge table and populate the node
		// table
		ToolRunner.run(new Configuration(), new ConvertMinMaxTimeStampWithoutUserReport(),args);
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		/*
		 * args[0] = reportName
		 * 
		 * args[1] = inputInstanceName
		 * args[2] = inputTableName
		 * args[3] = inputUserName
		 * args[4] = inputPassword
		 * 
		 * args[5] = outputInstanceName
		 * args[6] = outputTableName
		 * args[7] = outputUserName
		 * args[8] = outputPassword
		 * 
		 * args[9] = (date) timeStamp -> min
		 * args[10] = (date) timeStamp -> max
		 * args[11] = (boolean) onlyYear -> is true, when the compute inside the year
		 * args[12] = (day;month;year) pastLimitation
		 * args[13] = (long) reportTimestamp
		 * 
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[6]);
		conf.setBoolean("onlyYear", args[11].equals("true"));
		conf.setLong("reportTimestamp", Long.parseLong(args[13]));
		
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy hh:mm:ss" );
			Date d  = formatter.parse( args[9]);// day.month.year"
			conf.setLong("minTimestamp", d.getTime());
		}
		catch ( ParseException e )
		{
			try
			{
				DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				Date d  = formatter.parse( args[9]);// day.month.year hour:minut:second
				conf.setLong("minTimestamp", d.getTime());
				System.out.println("minTimestamp: "+args[9]);
			}
			catch ( ParseException ee ) {}
		}
		
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy hh:mm:ss" );
			Date d  = formatter.parse( args[10]);// day.month.year"
			conf.setLong("maxTimestamp", d.getTime());
		}
		catch ( ParseException e )
		{
			try
			{
				DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				Date d  = formatter.parse( args[10]);// day.month.year"
				conf.setLong("maxTimestamp", d.getTime());
			}
			catch ( ParseException ee ) {}
		}
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
			Date d  = formatter.parse( args[12]);// day.month.year"
			conf.setLong("pastLimitation", d.getTime());
		}
		catch ( ParseException e ){}
			
		
		
		Job job = Job.getInstance(conf);
		job.setJobName(ConvertMinMaxTimeStampWithoutUserReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(ConvertWithoutUserMapper.class);
		job.setReducerClass(ConvertWithoutUserReducer.class);

		if(test)
		{
			AccumuloInputFormat.setMockInstance(job, args[1]); // Instanzname
			AccumuloOutputFormat.setMockInstance(job, args[5]);
		}
		else
		{
			//AccumuloInputFormat.setZooKeeperInstance(job, args[1], "zooserver-one,zooserver-two");
			//AccumuloOutputFormat.setZooKeeperInstance(job, args[5], "zooserver-one,zooserver-two");
		}
		
		AccumuloInputFormat.setConnectorInfo(job, args[3], new PasswordToken(args[4])); //username,password
		AccumuloInputFormat.setInputTableName(job, args[2]);//tablename
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
		
		AccumuloOutputFormat.setConnectorInfo(job, args[7], new PasswordToken(args[8]));
		AccumuloOutputFormat.setDefaultTableName(job, args[6]);
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
