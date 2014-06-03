package org.sensoriclife.reports;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.consumptionInDaytime.ConsumptionInDaytimeReport;
import org.sensoriclife.reports.dayWithMaxConsumption.firstJob.DaysWithConsumptionReport;
import org.sensoriclife.reports.dayWithMaxConsumption.secondJob.DaysWithMaxConsumptionReport;
import org.sensoriclife.reports.emptyResidentialUnitsConsumption.EmptyResidentialUnitsConsumptionReport;
import org.sensoriclife.reports.minMaxConsumption.MinMaxConsumptionReport;
import org.sensoriclife.reports.unusualRiseOfConsumption.firstJob.UnusualRiseOfConsumptionReport;
import org.sensoriclife.reports.unusualRiseOfConsumption.secondob.UnusualRiseOfHeatingConsumptionReport;
import org.sensoriclife.reports.yearConsumption.YearConsumptionReport;
import org.sensoriclife.reports.yearInvoiceResidentialUnit.YearInvoiceReport;

/**
 * 
 * @author jnphilipp, marcel
 * @version 0.0.1
 * 
 */
public class App {
	public static boolean test = false;

	public static void main(String[] args) throws Exception {
		
		if(test){
			args = new String[2];
			args[0] = "-report";
			args[1] = "1";
		}
		
		Logger.getInstance();
		Logger.info("SensoricLife - reports");

		String confFile = "", report = "";
		if ( args.length != 0 ) {
			List<String> l = Arrays.asList(args);
			Iterator<String> it = l.iterator();
			while ( it.hasNext() ) {
				switch ( it.next() ) {
					case "-conf":
						confFile = it.next();
						break;
					case "-report":
						report = it.next();
				}
			}
		}

		Config.getInstance();
		if ( confFile.isEmpty() )
			Config.load();
		else
			Config.load(confFile);

		Accumulo accumulo;
		switch ( report ) {
			case "1":
			case "minMaxConsumption":
				MinMaxConsumptionReport.test=test;
				MinMaxConsumptionReport.runMinMaxConsumption();
				break;
			case "2":
			case "yearConsumption":
				YearConsumptionReport.test=test;
				YearConsumptionReport.runYearConsumption();
				break;
			case "3":
			case "yearInvoice":
				YearInvoiceReport.test=test;
				YearInvoiceReport.runYearInvoice();
				break;
			case "4":
			case "emptyResidentialUnitsConsumption":
				EmptyResidentialUnitsConsumptionReport.test=test;
				EmptyResidentialUnitsConsumptionReport.runEmptyResidentialUnitsConsumption();
				break;
			case "5":
			case "consumptionInDaytime":
				ConsumptionInDaytimeReport.test=test;
				ConsumptionInDaytimeReport.runReport();
				break;
			case "6":
			case "dayWithMaxConsumption":
				accumulo = DaysWithConsumptionReport.runFirstJob();
				DaysWithMaxConsumptionReport.runSecondJob(accumulo);
				break;
			case "7":
			case "unusualRiseOfConsumption":
				accumulo = UnusualRiseOfConsumptionReport.runFirstJob();
				UnusualRiseOfHeatingConsumptionReport.runSecondJob(accumulo);
				break;
			default:
				Logger.info("Wrong report.");
		}
	}
}