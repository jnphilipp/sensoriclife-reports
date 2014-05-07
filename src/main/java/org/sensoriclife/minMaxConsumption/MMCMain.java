package org.sensoriclife.minMaxConsumption;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.mapreduce.Job;
import org.sensoriclife.db.Accumulo;

public class MMCMain {
	public static void main(String[] args) {

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
		
		Accumulo accumulo = new Accumulo();
		//connect to accumulo
		accumulo.connect();
		//load data from accumulo - two tables needed (electricity_consumption and residentialUnit with resi_id and electric_id as parameters))
		accumulo.scanByKey("electricity_consumption", key, attribute, authentication);
		//start job (including map and reduce -> with report)
		//start bin/tool.sh script
		
	}
	
	public void getConf(){
		
	}
}
