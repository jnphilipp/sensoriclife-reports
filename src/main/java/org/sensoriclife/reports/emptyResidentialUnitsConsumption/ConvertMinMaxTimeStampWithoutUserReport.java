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
import org.sensoriclife.world.DeviceUnit;

public class ConvertMinMaxTimeStampWithoutUserReport extends Configured implements Tool{
	
	public static boolean test = true;
	
	public static void runConvert(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ConvertMinMaxTimeStampWithoutUserReport(),args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		/* 
		 * args[0] = inputInstanceName
		 * args[1] = inputZooServer
		 * args[2] = inputTableName
		 * args[3] = inputUserName
		 * args[4] = inputPassword
		 * 
		 * args[5] = outputInstanceName
		 * args[6] = outputZooServer
		 * args[7] = outputTableName
		 * args[8] = outputUserName
		 * args[9] = outputPassword
		 * 
		 * args[10] = (date) timeStamp -> min
		 * args[11] = (date) timeStamp -> max
		 * args[12] = (boolean) onlyInTimeRange -> is true, when the compute inside the timerange
		 * args[13] = (long) reportTimestamp
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[7]);
		conf.setBoolean("onlyInTimeRange", args[11].equals("true"));
		conf.setLong("reportTimestamp", Long.parseLong(args[13]));
		
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy HH:mm:ss" );
			Date d  = formatter.parse( args[10]);// day.month.year"
			conf.setLong("minTimestamp", d.getTime());
		}
		catch ( ParseException e )
		{
			try
			{
				DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				Date d  = formatter.parse( args[10]);// day.month.year hour:minut:second
				conf.setLong("minTimestamp", d.getTime());
			}
			catch ( ParseException ee ) {}
		}
		
		try
		{
			DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy HH:mm:ss" );
			Date d  = formatter.parse( args[11]);// day.month.year"
			conf.setLong("maxTimestamp", d.getTime());
		}
		catch ( ParseException e )
		{
			try
			{
				DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				Date d  = formatter.parse( args[11]);// day.month.year"
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
		cols.add(new Pair(new Text("user"), new Text("id")));
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
