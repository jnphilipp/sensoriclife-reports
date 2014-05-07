package org.sensoriclife.reports.mapRedOld;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ElecConsumptionMain {
	public static void main(String[] args) throws Exception {
		
		JobConf conf = new JobConf(ElecConsumptionMain.class);
		conf.addResource(new Path(
				"/home/marcel/Programs/hadoop-2.4.0/etc/hadoop/core-site.xml"));
		conf.addResource(new Path(
				"/home/marcel/Programs/hadoop-2.4.0/etc/hadoop/hdfs-site.xml"));
		
		conf.setJobName("minMaxConsumption");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(ElecConsumptionMap.class);
		conf.setCombinerClass(ElecConsumptionReduce.class);
		conf.setReducerClass(ElecConsumptionReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		//input and output to hdfs
		FileInputFormat.setInputPaths(conf, new Path("hdfs://localhost:8020/input/minMaxConsumption/"));
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://localhost:8020/output/minMaxConsumption/"));

		JobClient.runJob(conf);
	}
}
