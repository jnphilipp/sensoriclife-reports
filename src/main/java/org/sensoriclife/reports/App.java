package org.sensoriclife.reports;

import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.dayWithMaxConsumption.firstJob.DaysWithConsumptionReport;
import org.sensoriclife.reports.dayWithMaxConsumption.secondJob.DayWithMaxConsumptionReport;
import org.sensoriclife.reports.unusualRiseOfConsumption.firstJob.UnusualRiseOfConsumptionReport;
import org.sensoriclife.reports.unusualRiseOfConsumption.secondob.UnusualRiseOfHeatingConsumptionReport;

/**
 * 
 * @author jnphilipp, marcel
 * @version 0.0.1
 * 
 */
public class App {
	public static void main(String[] args) throws Exception {

		String testArgs[] = new String[1];
		//testArgs[0] = "dayWithMaxConsumption";
		testArgs[0] = "unusualRiseOfConsumption";
		Config.getInstance();

		Logger.getInstance();
		Logger.info("SensoricLife - reports");

		Accumulo accumulo;

		switch (testArgs[0]) {
		case "dayWithMaxConsumption":
			accumulo = DaysWithConsumptionReport.runFirstJob();
			DayWithMaxConsumptionReport.runSecondJob(accumulo);
			break;

		case "unusualRiseOfConsumption":
			accumulo = UnusualRiseOfConsumptionReport.runFirstJob();
			UnusualRiseOfHeatingConsumptionReport.runSecondJob(accumulo);
			break;
		}
	}
}