package org.sensoriclife.reports.emptyUnitConsumption;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.sensoriclife.Config;

/**
 *
 * @author paul
 * @version 0.0.1
 */
public class EmptyUnitConsumptionReport extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName(EmptyUnitConsumptionReport.class.getName());
		job.setJarByClass(this.getClass());
		job.setMapperClass(EmptyUnitConsumptionMapper1.class);
		job.setReducerClass(EmptyUnitConsumptionReducer1.class);

		AccumuloInputFormat.setMockInstance(job, Config.getProperty("accumulo.name"));
		AccumuloInputFormat.setConnectorInfo(job, Config.getProperty("accumulo.user"), new PasswordToken(Config.getProperty("accumulo.password")));
		AccumuloInputFormat.setInputTableName(job, Config.getProperty("accumulo.table_name"));
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());

		AccumuloOutputFormat.setConnectorInfo(job, Config.getProperty("accumulo.user"), new PasswordToken(Config.getProperty("accumulo.password")));
		AccumuloOutputFormat.setDefaultTableName(job, Config.getProperty("reports.empty_consumption_report.table_name"));
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setMockInstance(job, Config.getProperty("accumulo.name"));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}