package org.sensoriclife.reports;


import org.sensoriclife.Logger;
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
		 * args[9] = (long) minTimestamp
		 * args[10] = (long) maxTimestamp
		 * args[11] = (boolean) compute min max consumption for residentialunit
		 * args[12] = (boolean) compute min max consumption for builiding
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		String[] testArgs = new String[13];
		testArgs[0] = "minMaxConsumption";
		testArgs[1] = "mockInstance";
		testArgs[2] = "InputTable";
		testArgs[3] = "";
		testArgs[4] = "";
		testArgs[5] = "mockInstance";
		testArgs[6] = "OutputTable";
		testArgs[7] = "";
		testArgs[8] = "";
		testArgs[9] = "";//"24.12.2007";
		testArgs[10] = "";//"24.12.2007 12:15:12";
		testArgs[11] = "true"; //residentialUnit
		testArgs[12] = "true";  //buildings
		
		/*
		 * Testdata yearConsumption
		 *  
		 * args[9] = (Times) timestamp
		 * 
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		/*String[] testArgs = new String[10];
		testArgs[0] = "yearConsumption";
		testArgs[1] = "mockInstance";
		testArgs[2] = "InputTable";
		testArgs[3] = "";
		testArgs[4] = "";
		testArgs[5] = "mockInstance";
		testArgs[6] = "OutputTable";
		testArgs[7] = "";
		testArgs[8] = "";
		testArgs[9] = "";//"24.12.2007";
		*/
		
		/*
		 * Testdata yearInvoice
		 *  
		 * args[9] = (Times) timestamp
		 * args[10] = price
		 * 
		 * Times: dd.MM.yyyy or
		 * 		  dd.MM.yyyy kk:mm:ss
		 */
		
		/*String[] testArgs = new String[11];
		testArgs[0] = "yearInvoice";
		testArgs[1] = "mockInstance";
		testArgs[2] = "InputTable";
		testArgs[3] = "";
		testArgs[4] = "";
		testArgs[5] = "mockInstance";
		testArgs[6] = "OutputTable";
		testArgs[7] = "";
		testArgs[8] = "";
		testArgs[9] = "";//"24.12.2007";
		testArgs[10] = "wu;12.5;el;2"; 	*/	
		
		Logger.getInstance();
		Logger.info("SensoricLife - reports");
		
		switch(testArgs[0]){
			case "minMaxConsumption":
				MinMaxConsumptionReport.runMinMaxConsumption(testArgs);
				break;
			
			case "yearConsumption":
				YearConsumptionReport.runYearConsumption(testArgs);
				break;
				
			case "yearInvoice":
				YearInvoiceReport.runYearInvoice(testArgs);
				break;
		}
	}
}