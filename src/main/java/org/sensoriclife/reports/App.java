package org.sensoriclife.reports;

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
		String[] args2 = new String[10];
		args2[0] = "minMaxConsumption";

		
		Logger.getInstance();
		Logger.info("SensoricLife - reports");
		//Config c = Config.getInstance();
		//System.out.println("hallo: "+ c.getProperty("name"));
		
		switch(args2[0]){
			case "minMaxConsumption":
				MinMaxReport.runMinMax(args2);
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