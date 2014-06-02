package org.sensoriclife.reports.dayWithMaxConsumption.secondJob;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.sensoriclife.Config;
import org.sensoriclife.world.Consumption;

/**
 * this class tests the second map reduce job for correctness with an example.
 * the second map reduce jobs complets the task "dayWithMaxConsumption".
 * 
 * @author marcel
 * @deprecated
 * 
 */
public class DayWithMaxConsumptionTest {

	/**
	 * with this class, map and reduce will be tested together.
	 */
	private MapReduceDriver<Key, Value, IntWritable, Consumption, Text, Mutation> mapReduceDriver;

	/**
	 * this method is called before the execution of the test.
	 */
	@Before
	public void setUp() {
		DaysWithMaxConsumptionMapper mapper = new DaysWithMaxConsumptionMapper();
		DaysWithMaxConsumptionReducer reducer = new DaysWithMaxConsumptionReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

		Config conf = Config.getInstance();
		// config for mockinstance
		conf.getProperties().setProperty("mockInstanceName", "mockInstance");
		conf.getProperties().setProperty("inputTableName",
				"DaysWithConsumption");
		conf.getProperties().setProperty("outputTableName",
				"DaysWithMaxConsumption");
		conf.getProperties().setProperty("username", "");
		conf.getProperties().setProperty("password", "");
	}

	/**
	 * this method tests the complete map reduce job.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testMapReduce() throws Exception {

		String colQual = "amount";
		List<Pair<Key, Value>> mapperInput = new ArrayList<Pair<Key, Value>>();

		mapperInput.add(new Pair<Key, Value>(new Key("1", "el", colQual, 1),
				new Value("0".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("1", "he", colQual, 1),
				new Value("0".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("1", "wc", colQual, 1),
				new Value("4".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("1", "wh", colQual, 1),
				new Value("12".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("10", "el", colQual, 10),
				new Value("42".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("10", "he", colQual, 10),
				new Value("0".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("10", "wc", colQual, 10),
				new Value("0".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("10", "wh", colQual, 10),
				new Value("0".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("2", "el", colQual, 2),
				new Value("112".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("2", "he", colQual, 2),
				new Value("20".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("2", "wc", colQual, 2),
				new Value("125".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("2", "wh", colQual, 2),
				new Value("13".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("3", "el", colQual, 3),
				new Value("112".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("3", "he", colQual, 3),
				new Value("18".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("3", "wc", colQual, 3),
				new Value("0".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key("3", "wh", colQual, 3),
				new Value("0".getBytes())));

		mapReduceDriver.withAll(mapperInput);

		List<Pair<Text, Mutation>> expectedReducerOutput = new ArrayList<Pair<Text, Mutation>>();

		Mutation m = new Mutation(String.valueOf(1));
		m.put("el", "DayOfYear", 2, String.valueOf(2));
		m.put("el", "MaxAmount", 2, String.valueOf(new Double(112)));
		
		m.put("el", "DayOfYear", 3, String.valueOf(3));
		m.put("el", "MaxAmount", 3, String.valueOf(new Double(112)));

		m.put("wc", "DayOfYear", 2, String.valueOf(2));
		m.put("wc", "MaxAmount", 2, String.valueOf(new Double(125)));

		m.put("wh", "DayOfYear", 2, String.valueOf(2));
		m.put("wh", "MaxAmount", 2, String.valueOf(new Double(13)));

		m.put("he", "DayOfYear", 2, String.valueOf(2));
		m.put("he", "MaxAmount", 2, String.valueOf(new Double(20)));

		expectedReducerOutput.add(new Pair<Text, Mutation>(new Text(Config
				.getProperty("outputTableName")), m));

		List<Pair<Text, Mutation>> realReducerOutput = new ArrayList<Pair<Text, Mutation>>();
		realReducerOutput = mapReduceDriver.run();

		int count = 0;
		boolean isCorrect = true;
		for (Pair<Text, Mutation> pair : realReducerOutput) {
			String table = String.valueOf(pair.getFirst());
			if (!Config.getProperty("outputTableName").equals(table)) {
				isCorrect = false;
				break;
			}

			Mutation mutation = (Mutation) pair.getSecond();
			Mutation currentExpectedMutation = expectedReducerOutput.get(count)
					.getSecond();

			if (mutation.equals(currentExpectedMutation) == false) {
				isCorrect = false;
				break;
			}
			count++;
		}
		if (isCorrect == true) {
			System.out.println("MapReduce correct");
		} else {
			System.out.println("MapReduce wrong");
		}

	}
}