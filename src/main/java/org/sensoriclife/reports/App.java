package org.sensoriclife.reports;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.reports.minMaxConsumption.MinMaxReport;



/**
 *
 * @author jnphilipp, marcel
 * @version 0.0.1
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		

		/*
		 * args[0] = (String) reportName
		 * args[1-n] = reportArgs
		 */
		
		/*
		 * Testdata generally
		 * /*
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
		 */
		
		/*
		 * Testdata minMaxConsumption
		 *  
		 * args[9] = (boolean) allTime 
		 * args[10] = (long) minTimestamp
		 * args[11] = (long) maxTimestamp
		 * 
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		String[] testArgs = new String[12];
		testArgs[0] = "minMaxConsumption";
		testArgs[1] = "mockInstance";
		testArgs[2] = "InputTable";
		testArgs[3] = "";
		testArgs[4] = "";
		testArgs[5] = "mockInstance";
		testArgs[6] = "OutputTable";
		testArgs[7] = "";
		testArgs[8] = "";
		testArgs[9] = "true";
		testArgs[10] = "24.12.2007";
		testArgs[11] = "24.12.2007 12:15:12";

		
		Logger.getInstance();
		Logger.info("SensoricLife - reports");
		//Config c = Config.getInstance();
		//System.out.println("hallo: "+ c.getProperty("name"));
		
	
		switch(testArgs[0]){
			case "minMaxConsumption":
				MinMaxReport.runMinMax(testArgs);
				break;
		}
		/*int res = 0;
		try {
			res = ToolRunner.run(CachedConfiguration.getInstance(),
					new DaysWithConsumptionReport(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.exit(res);*/
	}
}