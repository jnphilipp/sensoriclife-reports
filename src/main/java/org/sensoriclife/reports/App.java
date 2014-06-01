package org.sensoriclife.reports;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.reports.emptyResidentialUnitsConsumption.EmptyResidentialUnitsConsumptionReport;
import org.sensoriclife.reports.minMaxConsumption.MinMaxConsumptionReport;
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
		Logger.getInstance();
		Logger.info("SensoricLife - reports");
		
		String confFile = "", report = "";
		if(test)
		{
			args = new String[2];
			//args[0] = "-report" ; args[1] = "minMaxConsumption";
			//args[0] = "-report" ; args[1] = "yearConsumption";
			args[0] = "-report" ; args[1] = "yearInvoice";
			//args[0] = "-report" ; args[1] = "emptyResidentialUnitsConsumption";
		}
		
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
			default:
				Logger.info("Wrong report.");
		}
	}
}