package org.sensoriclife.reports.unusualRiseOfConsumption.firstJob;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.MockInstanceConfiguration;
import org.sensoriclife.world.ResidentialUnit;

public class UnusualRiseOfConsumptionReport extends Configured implements Tool {

	/**
	 * runs the whole process of the first map reduce job, including configuration, inserting of test data and execution of the job.
	 * @return Accumulo
	 * @throws Exception
	 */
	public static Accumulo runFirstJob() throws Exception {

		MockInstanceConfiguration mockConfig = MockInstanceConfiguration
				.getInstance();
		mockConfig.setMockInstanceName("mockInstance");
		mockConfig.setInputTableName("Consumption");
		mockConfig.setOutputTableName("UnusualRiseOfConsumption");
		mockConfig.setUserName("");
		mockConfig.setPassword("");

		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect();

		accumulo.createTable(mockConfig.getInputTableName(), false);
		accumulo.createTable(mockConfig.getOutputTableName(), false);
		// another output table ~ helpertable
		//--> needed for second map reduce job
		accumulo.createTable("HeatingConsumption", false);

		insertData(accumulo, mockConfig.getInputTableName());
		Iterator<Entry<Key, Value>> scanner = accumulo.scanAll(mockConfig
				.getInputTableName());

		while (scanner.hasNext()) {
			Entry<Key, Value> entry = scanner.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}

		ToolRunner.run(new Configuration(),
				new UnusualRiseOfConsumptionReport(),
				mockConfig.getConfigAsStringArray());

		Iterator<Entry<Key, Value>> scanner2 = accumulo.scanAll(mockConfig
				.getOutputTableName());

		while (scanner2.hasNext()) {
			Entry<Key, Value> entry = scanner2.next();
			System.out.println("Key: " + entry.getKey().toString() + " Value: "
					+ entry.getValue().toString());
		}

		return accumulo;
	}

	/**
	 * executes the accumulo map reduce job.
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		//time interval: weekly
		conf.setLong("minTimestamp", 1);
		conf.setLong("maxTimestamp", 2);

		Job job = Job.getInstance(conf);
		job.setJobName(UnusualRiseOfConsumptionReport.class.getName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(UnusualRiseOfConsumptionMapper.class);
		job.setReducerClass(UnusualRiseOfConsumptionReducer.class);

		AccumuloInputFormat.setMockInstance(job, args[3]);
		AccumuloInputFormat.setConnectorInfo(job, args[2], new PasswordToken(
				args[1]));
		AccumuloInputFormat.setInputTableName(job, args[0]);
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());

		AccumuloOutputFormat.setConnectorInfo(job, args[2], new PasswordToken(
				args[1]));
		AccumuloOutputFormat.setDefaultTableName(job, args[4]);
		AccumuloOutputFormat.setCreateTables(job, true);
		AccumuloOutputFormat.setMockInstance(job, args[3]);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ResidentialUnit.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);

		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * inserts some test data, adapted for this report.
	 * @param accumulo
	 * @param tableName
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public static void insertData(Accumulo accumulo, String tableName)
			throws MutationsRejectedException, TableNotFoundException {
		int consumptionId = 1;
		String colFam = "device";
		String colQual = "amount";
		String colFam2 = "residential";
		String colQual2 = "id";

		Mutation mutation = accumulo.putToNewMutation(
				String.valueOf(consumptionId) + "_wc", colFam, colQual, 1,
				new Value("90".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("1-2-10".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId)
				+ "_wc", colFam, colQual, 2, new Value("440".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("1-2-10".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 1)
				+ "_wc", colFam, colQual, 1, new Value("80".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("1-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 1)
				+ "_wc", colFam, colQual, 2, new Value("125".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("1-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId)
				+ "_wh", colFam, colQual, 1, new Value("35".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("1-2-10".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId)
				+ "_wh", colFam, colQual, 2, new Value("175".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("1-2-10".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId)
				+ "_el", colFam, colQual, 1, new Value("111".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("1-2-2".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId)
				+ "_el", colFam, colQual, 2, new Value("222".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("1-2-2".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 1)
				+ "_el", colFam, colQual, 1, new Value("111".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("1-2-1".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 1)
				+ "_el", colFam, colQual, 2, new Value("600".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("1-2-1".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 2)
				+ "_el", colFam, colQual, 1, new Value("42".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("1-1-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 2)
				+ "_el", colFam, colQual, 2, new Value("50".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("1-1-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 3)
				+ "_el", colFam, colQual, 1, new Value("333".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("2-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 3)
				+ "_el", colFam, colQual, 2, new Value("1500".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("2-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 4)
				+ "_el", colFam, colQual, 1, new Value("1".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("3-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 4)
				+ "_el", colFam, colQual, 2, new Value("14".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("3-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 5)
				+ "_el", colFam, colQual, 1, new Value("2000".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("3-2-8".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 5)
				+ "_el", colFam, colQual, 2, new Value("5000".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("3-2-8".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId)
				+ "_he", colFam, colQual, 1, new Value("180".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("1-2-1".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId)
				+ "_he", colFam, colQual, 2, new Value("220".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("1-2-1".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 1)
				+ "_he", colFam, colQual, 1, new Value("310".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("1-2-4".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 1)
				+ "_he", colFam, colQual, 2, new Value("800".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("1-2-4".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 2)
				+ "_he", colFam, colQual, 1, new Value("50".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 1,
				new Value("1-2-4".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 2)
				+ "_he", colFam, colQual, 2, new Value("80".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 2,
				new Value("1-2-4".getBytes()));
		accumulo.addMutation(tableName, mutation);

		mutation = accumulo.putToNewMutation(String.valueOf(consumptionId + 8)
				+ "_el", colFam, colQual, 0, new Value("14".getBytes()));
		mutation = accumulo.putToMutation(mutation, colFam2, colQual2, 0,
				new Value("3-2-3".getBytes()));
		accumulo.addMutation(tableName, mutation);
	}
}
