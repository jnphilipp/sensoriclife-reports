package org.sensoriclife.reports;

import org.sensoriclife.minMaxConsumption.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
 
public class MR_MMCTest {
 
  MapDriver<Text, Text, NullWritable, ResidentialUnit> mapDriver;
  ReduceDriver<NullWritable, ResidentialUnit, Text, DoubleWritable> reduceDriver;
  MapReduceDriver<Text, Text, NullWritable, ResidentialUnit, Text, DoubleWritable> mapReduceDriver;
 
  @Before
  public void setUp() {
    MMCMapper mapper = new MMCMapper();
    MMCReducer reducer = new MMCReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 
  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new Text("1-1-1-1-1"), new Text(
        "1;1;1"));
    //mapDriver.withOutput(new Text("6"), new IntWritable(1));
    
	mapDriver.runTest();
	
  }
 
  @Test
  public void testReducer() throws IOException {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    //reduceDriver.withInput(new Text("6"), values);
    //reduceDriver.withOutput(new Text("6"), new IntWritable(2));
    reduceDriver.runTest();
  }
}
