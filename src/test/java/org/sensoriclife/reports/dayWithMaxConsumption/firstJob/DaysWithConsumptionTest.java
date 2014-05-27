package org.sensoriclife.reports.dayWithMaxConsumption.firstJob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.sensoriclife.Config;
import org.sensoriclife.world.ResidentialUnit;

/**
 * this class tests the first map reduce job for correctness with an example.
 * the second map reduce job needs the correct output for executing its own
 * calculation right.
 * 
 * @author marcel
 * @deprecated
 * 
 */
public class DaysWithConsumptionTest {

	/**
	 * the map function will be tested
	 */
	private MapDriver<Key, Value, IntWritable, ResidentialUnit> mapDriver;

	/**
	 * the reduce function will be tested
	 */
	private ReduceDriver<IntWritable, ResidentialUnit, Text, Mutation> reduceDriver;

	/**
	 * this method is called before the execution of the test.
	 */
	@Before
	public void setUp() {
		DaysWithConsumptionMapper mapper = new DaysWithConsumptionMapper();
		DaysWithConsumptionReducer reducer = new DaysWithConsumptionReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);

		Config conf = Config.getInstance();
		// config for mockinstance
		conf.getProperties().setProperty("mockInstanceName", "mockInstance");
		conf.getProperties().setProperty("inputTableName", "Consumption");
		conf.getProperties().setProperty("outputTableName",
				"DaysWithConsumption");
		conf.getProperties().setProperty("username", "");
		conf.getProperties().setProperty("password", "");

		// config for map reduce job
		// minTimestamp ~ 01.01.LastYear 0:00.00
		conf.getProperties().setProperty("minTimestamp", "1");
		// maxTimestamp ~ 31.12.LastYear 23:59.59
		conf.getProperties().setProperty("maxTimestamp", "10");
	}

	/**
	 * this method tests the map method with an example.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testMapper() throws Exception {

		int consumptionId = 1;
		String colFam = "device";
		String colQual = "amount";
		String colFam2 = "residential";
		String colQual2 = "id";

		List<Pair<Key, Value>> mapperInput = new ArrayList<Pair<Key, Value>>();

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId) + "_wc", colFam, colQual, 1),
				new Value("4".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId) + "_wc", colFam2, colQual2, 1),
				new Value("1-2-10".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 1) + "_wc", colFam, colQual, 2),
				new Value("125".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 1) + "_wc", colFam2, colQual2, 2),
				new Value("1-2-3".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId) + "_wh", colFam, colQual, 1),
				new Value("12".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId) + "_wh", colFam2, colQual2, 1),
				new Value("1-2-10".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 1) + "_wh", colFam, colQual, 2),
				new Value("13".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 1) + "_wh", colFam2, colQual2, 2),
				new Value("1-2-3".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId) + "_el", colFam, colQual, 3),
				new Value("111".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId) + "_el", colFam2, colQual2, 3),
				new Value("1-2-1".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 1) + "_el", colFam, colQual, 2),
				new Value("7".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 1) + "_el", colFam2, colQual2, 2),
				new Value("1-2-0".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 2) + "_el", colFam, colQual, 10),
				new Value("42".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 2) + "_el", colFam2, colQual2, 10),
				new Value("1-1-3".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 3) + "_el", colFam, colQual, 2),
				new Value("105".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 3) + "_el", colFam2, colQual2, 2),
				new Value("2-2-3".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 4) + "_el", colFam, colQual, 3),
				new Value("1".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 4) + "_el", colFam2, colQual2, 3),
				new Value("3-2-3".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId) + "_he", colFam, colQual, 3),
				new Value("18".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId) + "_he", colFam2, colQual2, 3),
				new Value("1-2-1".getBytes())));

		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 1) + "_he", colFam, colQual, 2),
				new Value("20".getBytes())));
		mapperInput.add(new Pair<Key, Value>(new Key(String
				.valueOf(consumptionId + 1) + "_he", colFam2, colQual2, 2),
				new Value("1-2-0".getBytes())));

		mapDriver.withAll(mapperInput);

		List<Pair<IntWritable, ResidentialUnit>> expectedMapperOutput = new ArrayList<Pair<IntWritable, ResidentialUnit>>();

		ResidentialUnit flat = new ResidentialUnit();
		flat.setDeviceAmount(4);
		flat.setTimeStamp(1);
		flat.setConsumptionID("1_wc");
		flat.setCounterType("wc");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(1), flat));

		flat = new ResidentialUnit();
		flat.setDeviceAmount(125);
		flat.setTimeStamp(2);
		flat.setConsumptionID("2_wc");
		flat.setCounterType("wc");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(2), flat));

		flat = new ResidentialUnit();
		flat.setDeviceAmount(12);
		flat.setTimeStamp(1);
		flat.setConsumptionID("1_wh");
		flat.setCounterType("wh");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(1), flat));

		flat = new ResidentialUnit();
		flat.setDeviceAmount(13);
		flat.setTimeStamp(2);
		flat.setConsumptionID("2_wh");
		flat.setCounterType("wh");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(2), flat));

		flat = new ResidentialUnit();
		flat.setDeviceAmount(111);
		flat.setTimeStamp(3);
		flat.setConsumptionID("1_el");
		flat.setCounterType("el");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(3), flat));

		flat = new ResidentialUnit();
		flat.setDeviceAmount(7);
		flat.setTimeStamp(2);
		flat.setConsumptionID("2_el");
		flat.setCounterType("el");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(2), flat));

		flat = new ResidentialUnit();
		flat.setDeviceAmount(42);
		flat.setTimeStamp(10);
		flat.setConsumptionID("3_el");
		flat.setCounterType("el");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(10), flat));

		flat = new ResidentialUnit();
		flat.setDeviceAmount(105);
		flat.setTimeStamp(2);
		flat.setConsumptionID("4_el");
		flat.setCounterType("el");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(2), flat));

		flat = new ResidentialUnit();
		flat.setDeviceAmount(1);
		flat.setTimeStamp(3);
		flat.setConsumptionID("5_el");
		flat.setCounterType("el");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(3), flat));

		flat = new ResidentialUnit();
		flat.setDeviceAmount(18);
		flat.setTimeStamp(3);
		flat.setConsumptionID("1_he");
		flat.setCounterType("he");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(3), flat));

		flat = new ResidentialUnit();
		flat.setDeviceAmount(20);
		flat.setTimeStamp(2);
		flat.setConsumptionID("2_he");
		flat.setCounterType("he");
		expectedMapperOutput.add(new Pair<IntWritable, ResidentialUnit>(
				new IntWritable(2), flat));

		List<Pair<IntWritable, ResidentialUnit>> realMapperOutput = new ArrayList<Pair<IntWritable, ResidentialUnit>>();
		realMapperOutput = mapDriver.run();

		boolean isCorrect = true;
		int count = 0;
		for (Pair<IntWritable, ResidentialUnit> pair : realMapperOutput) {
			long timestamp = Integer.parseInt(String.valueOf((IntWritable) pair
					.getFirst()));
			if (timestamp != Long.parseLong(String
					.valueOf((IntWritable) realMapperOutput.get(count)
							.getFirst()))) {
				isCorrect = false;
				break;
			}

			ResidentialUnit currentFlat = (ResidentialUnit) pair.getSecond();

			if (currentFlat.compareTo(expectedMapperOutput.get(count)
					.getSecond()) == -1) {
				isCorrect = false;
				break;
			}
			count++;
		}
		if (isCorrect == true) {
			System.out.println("Mapper correct");
		} else {
			System.out.println("Mapper wrong");
		}

	}

	/**
	 * this method tests the reducer with an example.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testReducer() throws IOException {

		List<Pair<IntWritable, List<ResidentialUnit>>> reducerInput = new ArrayList<Pair<IntWritable, List<ResidentialUnit>>>();
		List<ResidentialUnit> valuesForCertainKey = new ArrayList<ResidentialUnit>();
		// expected keys: 1,2,3,10

		// values for key "1"
		ResidentialUnit flat = new ResidentialUnit();
		flat.setDeviceAmount(4);
		flat.setTimeStamp(1);
		flat.setConsumptionID("1_wc");
		flat.setCounterType("wc");
		valuesForCertainKey.add(flat);

		flat = new ResidentialUnit();
		flat.setDeviceAmount(12);
		flat.setTimeStamp(1);
		flat.setConsumptionID("1_wh");
		flat.setCounterType("wh");
		valuesForCertainKey.add(flat);

		reducerInput.add(new Pair<IntWritable, List<ResidentialUnit>>(
				new IntWritable(1), valuesForCertainKey));

		// values for key "2"
		valuesForCertainKey = new ArrayList<ResidentialUnit>();

		flat = new ResidentialUnit();
		flat.setDeviceAmount(125);
		flat.setTimeStamp(2);
		flat.setConsumptionID("2_wc");
		flat.setCounterType("wc");
		valuesForCertainKey.add(flat);

		flat = new ResidentialUnit();
		flat.setDeviceAmount(13);
		flat.setTimeStamp(2);
		flat.setConsumptionID("2_wh");
		flat.setCounterType("wh");
		valuesForCertainKey.add(flat);

		flat = new ResidentialUnit();
		flat.setDeviceAmount(7);
		flat.setTimeStamp(2);
		flat.setConsumptionID("2_el");
		flat.setCounterType("el");
		valuesForCertainKey.add(flat);

		flat = new ResidentialUnit();
		flat.setDeviceAmount(105);
		flat.setTimeStamp(2);
		flat.setConsumptionID("4_el");
		flat.setCounterType("el");
		valuesForCertainKey.add(flat);

		flat = new ResidentialUnit();
		flat.setDeviceAmount(20);
		flat.setTimeStamp(2);
		flat.setConsumptionID("2_he");
		flat.setCounterType("he");
		valuesForCertainKey.add(flat);

		reducerInput.add(new Pair<IntWritable, List<ResidentialUnit>>(
				new IntWritable(2), valuesForCertainKey));

		// values for key "3"
		valuesForCertainKey = new ArrayList<ResidentialUnit>();

		flat = new ResidentialUnit();
		flat.setDeviceAmount(111);
		flat.setTimeStamp(3);
		flat.setConsumptionID("1_el");
		flat.setCounterType("el");
		valuesForCertainKey.add(flat);

		flat = new ResidentialUnit();
		flat.setDeviceAmount(1);
		flat.setTimeStamp(3);
		flat.setConsumptionID("5_el");
		flat.setCounterType("el");
		valuesForCertainKey.add(flat);

		flat = new ResidentialUnit();
		flat.setDeviceAmount(18);
		flat.setTimeStamp(3);
		flat.setConsumptionID("1_he");
		flat.setCounterType("he");
		valuesForCertainKey.add(flat);

		reducerInput.add(new Pair<IntWritable, List<ResidentialUnit>>(
				new IntWritable(3), valuesForCertainKey));

		// values for key "4"
		valuesForCertainKey = new ArrayList<ResidentialUnit>();

		flat = new ResidentialUnit();
		flat.setDeviceAmount(42);
		flat.setTimeStamp(10);
		flat.setConsumptionID("3_el");
		flat.setCounterType("el");
		valuesForCertainKey.add(flat);

		reducerInput.add(new Pair<IntWritable, List<ResidentialUnit>>(
				new IntWritable(10), valuesForCertainKey));

		reduceDriver.withAll(reducerInput);

		List<Pair<Text, Mutation>> expectedReducerOutput = new ArrayList<Pair<Text, Mutation>>();

		Mutation m = new Mutation(String.valueOf(1));
		m.put("el", "amount", 1, String.valueOf(new Double(0)));
		m.put("wc", "amount", 1, String.valueOf(new Double(4)));
		m.put("wh", "amount", 1, String.valueOf(new Double(12)));
		m.put("he", "amount", 1, String.valueOf(new Double(0)));

		expectedReducerOutput.add(new Pair<Text, Mutation>(new Text(Config
				.getProperty("outputTableName")), m));

		m = new Mutation(String.valueOf(2));
		m.put("el", "amount", 2, String.valueOf(new Double(112)));
		m.put("wc", "amount", 2, String.valueOf(new Double(125)));
		m.put("wh", "amount", 2, String.valueOf(new Double(13)));
		m.put("he", "amount", 2, String.valueOf(new Double(20)));

		expectedReducerOutput.add(new Pair<Text, Mutation>(new Text(Config
				.getProperty("outputTableName")), m));

		m = new Mutation(String.valueOf(3));
		m.put("el", "amount", 3, String.valueOf(new Double(112)));
		m.put("wc", "amount", 3, String.valueOf(new Double(0)));
		m.put("wh", "amount", 3, String.valueOf(new Double(0)));
		m.put("he", "amount", 3, String.valueOf(new Double(18)));

		expectedReducerOutput.add(new Pair<Text, Mutation>(new Text(Config
				.getProperty("outputTableName")), m));

		m = new Mutation(String.valueOf(10));
		m.put("el", "amount", 10, String.valueOf(new Double(42)));
		m.put("wc", "amount", 10, String.valueOf(new Double(0)));
		m.put("wh", "amount", 10, String.valueOf(new Double(0)));
		m.put("he", "amount", 10, String.valueOf(new Double(0)));

		expectedReducerOutput.add(new Pair<Text, Mutation>(new Text(Config
				.getProperty("outputTableName")), m));

		List<Pair<Text, Mutation>> realReducerOutput = new ArrayList<Pair<Text, Mutation>>();
		realReducerOutput = reduceDriver.run();

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
			System.out.println("Reducer correct");
		} else {
			System.out.println("Reducer wrong");
		}
	}
}
