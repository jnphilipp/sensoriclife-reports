package org.sensoriclife.reports;


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
	public static void main(String[] args) throws Exception {
		
		String testArgs[] = new String[1];
		//testArgs[0] = "minMaxConsumption";
		//testArgs[0] = "yearConsumption";
		//testArgs[0] = "yearInvoice";
		testArgs[0] = "emptyResidentialUnitsConsumption";
		Config.getInstance();
		
		Logger.getInstance();
		Logger.info("SensoricLife - reports");
		
		switch(testArgs[0]){
			case "minMaxConsumption":
				MinMaxConsumptionReport.runMinMaxConsumption();
				break;
			
			case "yearConsumption":
				YearConsumptionReport.runYearConsumption();
				break;
				
			case "yearInvoice":
				YearInvoiceReport.runYearInvoice();
				break;
			
			case "emptyResidentialUnitsConsumption":
				EmptyResidentialUnitsConsumptionReport.runEmptyResidentialUnitsConsumption();
				break;
		}
	}
}