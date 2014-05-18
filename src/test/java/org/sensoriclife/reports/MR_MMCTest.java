package org.sensoriclife.reports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.sensoriclife.minMaxConsumption.AccumuloMMCMapper;
import org.sensoriclife.minMaxConsumption.AccumuloMMCReducer;
import org.sensoriclife.reports.world.ResidentialUnit;
 
/**
 * 
 * @author yves, marcel
 *
 */
public class MR_MMCTest {
 
  private MapDriver<Key, Value, IntWritable, ResidentialUnit> mapDriver;
  private ReduceDriver<IntWritable, ResidentialUnit, Text, Mutation> reduceDriver;
  private MapReduceDriver<Key, Value, IntWritable, ResidentialUnit, Text, Mutation> mapReduceDriver;
 
  @Before
  public void setUp() {
    AccumuloMMCMapper mapper = new AccumuloMMCMapper();
    AccumuloMMCReducer reducer = new AccumuloMMCReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 
  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new Key("1-1-1-1-1"), new Value("1".getBytes()));
    ResidentialUnit flat = new ResidentialUnit();
	flat.getElecConsumption().setAmount(1);
	flat.getElecConsumption().setTimestamp(1);
	flat.setElectricMeterId("1_el");
    mapDriver.withOutput(new IntWritable(1), flat);
	mapDriver.runTest();	
  }
 

  @Test
  public void testReducer() throws IOException {
    List<ResidentialUnit> values = new ArrayList<ResidentialUnit>();
    ResidentialUnit flat = new ResidentialUnit();
   	flat.getElecConsumption().setAmount(1);
   	flat.getElecConsumption().setTimestamp(1);
   	flat.setElectricMeterId("1_el");
    values.add(flat);
    reduceDriver.withInput(new IntWritable(1), values);
   
    Mutation m1 = new Mutation();
	// write minimum
	m1.put("consumptionId",
			"min",
			new Value("1-1-1-1-1".getBytes())); 
	m1.put("amount",
			"minAmount",
			new Value(String.valueOf(1).getBytes()));
    
    reduceDriver.withOutput(new Text("minMaxConsumption"), m1);
    //reduceDriver.withOutput(new Text("minMaxConsumption"), m2);
    reduceDriver.runTest();
  }
}
