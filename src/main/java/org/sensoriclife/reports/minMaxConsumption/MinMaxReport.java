package org.sensoriclife.reports.minMaxConsumption;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.world.ResidentialUnit;

/**
 * 
 * @author marcel, yves
 *
 */
public class MinMaxReport extends Configured implements Tool {
	
	public static boolean test = true;
	
	
	public static void runMinMax(String[] args) throws Exception {
		
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
		 * args[9] = (boolean) allTime 
		 * args[10] = (long) minTimestamp
		 * args[11] = (long) maxTimestamp
		 */
		
		
		/*TEST*/
		if(test)
		{
			MockInstance inputMockInstance = new MockInstance(args[1]);
			MockInstance outputMockInstance = new MockInstance(args[5]);
			
			Connector inputConnector = inputMockInstance.getConnector(args[3], new PasswordToken(args[4]));
			Connector outputConnector = outputMockInstance.getConnector(args[7], new PasswordToken(args[8]));
			
			inputConnector.tableOperations().create(args[2], false);
			outputConnector.tableOperations().create(args[6], false);

			// insert some test data
			insertData(inputConnector, args[2]);
		}
		

		
		
		// run the map reduce job to read the edge table and populate the node
		// table
		int res = ToolRunner.run(new Configuration(), new MinMaxReport(),
				args);

		
		/*
		 * Test
		 */
		if(test)
		{
			MockInstance inputMockInstance = new MockInstance(args[1]);
			MockInstance outputMockInstance = new MockInstance(args[5]);
			
			Connector inputConnector = inputMockInstance.getConnector(args[3], new PasswordToken(args[4]));
			Connector outputConnector = outputMockInstance.getConnector(args[7], new PasswordToken(args[8]));
			
			// print the inputtable
			printTable(inputConnector, args[2]);
			System.out.println("############################################");
			// print the results of mapreduce
			printTable(outputConnector, args[6]);
		}
		

		System.exit(res);

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
		 * args[9] = (boolean) allTime 
		 * args[10] = (long) minTimestamp
		 * args[11] = (long) maxTimestamp
		 */
		
		Configuration conf = new Configuration();
		conf.setStrings("outputTableName", args[6]);
		if(!args[9].equals("true"))
		{
			try
			{
			  DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy hh:mm:ss" );
			  Date d  = formatter.parse( args[10]);// day.month.year"
			  System.out.println( d.getTime() ); 
			  conf.setLong("minTimestamp", d.getTime());
			}
			catch ( ParseException e )
			{
				try
				{
				  DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				  Date d  = formatter.parse( args[10]);// day.month.year hour:minut:second
				  System.out.println( d.getTime() ); 
				  conf.setLong("minTimestamp", d.getTime());
				}
				catch ( ParseException ee ) {}
			}
			
			try
			{
			  DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy hh:mm:ss" );
			  Date d  = formatter.parse( args[11]);// day.month.year"
			  System.out.println( d.getTime() ); 
			  conf.setLong("maxTimestamp", d.getTime());
			}
			catch ( ParseException e )
			{
				try
				{
				  DateFormat formatter = new SimpleDateFormat( "dd.MM.yyyy" );
				  Date d  = formatter.parse( args[11]);// day.month.year hour:minut:second
				  System.out.println( d.getTime() ); 
				  conf.setLong("maxTimestamp", d.getTime());
				}
				catch ( ParseException ee ) {}
			}
			
		}
		
		
		Job job = Job.getInstance(conf);
		job.setJobName(MinMaxReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(MinMaxMapper.class);
		job.setReducerClass(MinMaxReducer.class);

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

	public static void insertData(Connector conn, String tableName)
			throws IOException, AccumuloException, AccumuloSecurityException,
			TableExistsException, TableNotFoundException {
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter wr = conn.createBatchWriter(tableName, config);

		// ######################################################################
		int consumptionId = 1;
		Text colFam = new Text("device");
		Text colQual = new Text("amount");
		Text colFam2 = new Text("residential");
		Text colQual2 = new Text("id");
		ColumnVisibility colVis = new ColumnVisibility();// "public");
		// long timestamp = System.currentTimeMillis();

		Value value = new Value("4".getBytes());

		//mutation with consumptionId as rowId
		Mutation mutation = new Mutation(new Text(String.valueOf(consumptionId) + "_wc"));
		mutation.put(colFam, colQual, colVis, 1, value);
		mutation.put(colFam2, colQual2, colVis, 1,new Value("1-2-10".getBytes()));
		wr.addMutation(mutation);
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 1) + "_wc"));
		mutation.put(colFam, colQual, colVis, 2,
				new Value("125".getBytes()));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-2-3".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 2) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 3,new Value("1-2-1".getBytes()));
		mutation.put(colFam, colQual, colVis, 3,
				new Value("111".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 3) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("1-2-0".getBytes()));
		mutation.put(colFam, colQual, colVis, 2,
				new Value("7".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 4) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 10,new Value("1-1-3".getBytes()));
		mutation.put(colFam, colQual, colVis, 10,
				new Value("42".getBytes()));
		wr.addMutation(mutation);

		mutation = new Mutation(new Text(String.valueOf(consumptionId + 5) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 2,new Value("2-2-3".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 2,
				new Value("2".getBytes()));
		wr.addMutation(mutation);
		
		mutation = new Mutation(new Text(String.valueOf(consumptionId + 6) + "_el"));
		mutation.put(colFam2, colQual2, colVis, 3,new Value("3-2-3".getBytes()));
		mutation.put(colFam, colQual, new ColumnVisibility(), 3,
				new Value("1".getBytes()));
		wr.addMutation(mutation);
		wr.close();
	}

	public static void printTable(Connector conn, String tableName)
			throws TableNotFoundException {

		System.out.println("\n Tabelle:" + tableName);
		Scanner scanner = conn.createScanner(tableName, new Authorizations());
		Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			Map.Entry<Key, Value> entry = iterator.next();
			Key key = entry.getKey();
			Value v = entry.getValue();
			System.out.println("Row: " + key.getRow() + " Fam: " + key.getColumnFamily() + " Qual: " + key.getColumnQualifier() +" Ts:" + key.getTimestamp() +  " Value: " + v);
		}
	}	
}
