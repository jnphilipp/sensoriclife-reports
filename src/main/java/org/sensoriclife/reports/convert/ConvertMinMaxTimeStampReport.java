package org.sensoriclife.reports.convert;

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
import org.sensoriclife.world.ResidentialUnit;

public class ConvertMinMaxTimeStampReport extends Configured implements Tool{
	
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
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		// run the map reduce job to read the edge table and populate the node
		// table
		ToolRunner.run(new Configuration(), new ConvertMinMaxTimeStampReport(),args);
		
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
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[6]);
		
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
				DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy hh:mm:ss" );
				Date d  = formatter.parse( args[10]);// day.month.year"
				conf.setLong("maxTimestamp", d.getTime());
			}
			catch ( ParseException ee ) {}
		}
			
		
		
		Job job = Job.getInstance(conf);
		job.setJobName(ConvertMinMaxTimeStampReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(ConvertMapper.class);
		job.setReducerClass(ConvertReducer.class);

		if(test)
		{
			AccumuloInputFormat.setMockInstance(job, args[1]); // Instanzname
			AccumuloInputFormat.setConnectorInfo(job, args[3], new PasswordToken(args[4])); //username,password
			AccumuloInputFormat.setInputTableName(job, args[2]);//tablename
			AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());

			AccumuloOutputFormat.setMockInstance(job, args[5]);
			AccumuloOutputFormat.setConnectorInfo(job, args[7], new PasswordToken(args[8]));
			AccumuloOutputFormat.setDefaultTableName(job, args[6]);
			AccumuloOutputFormat.setCreateTables(job, true);
		}
		
		

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ResidentialUnit.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
