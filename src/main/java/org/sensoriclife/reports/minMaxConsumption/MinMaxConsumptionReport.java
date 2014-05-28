package org.sensoriclife.reports.minMaxConsumption;

import java.util.GregorianCalendar;
import org.sensoriclife.Config;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.helper.ConsumptionGeneralizeReport;
import org.sensoriclife.reports.helper.ConvertMinMaxTimeStampReport;
import org.sensoriclife.util.Helpers;

public class MinMaxConsumptionReport {
	public static void runMinMaxConsumption() throws Exception{
		
		String[] args = new String[16];
		args[0] = "Report: MinMaxConsumption"; //Name :)
		args[1] = Config.getProperty("accumulo.name");//inputInstanceName
		args[2] = Config.getProperty("accumulo.zooServers");//zoo servers
		args[3] = Config.getProperty("accumulo.user");//inputUserName
		args[4] = Config.getProperty("accumulo.password");//inputPassword
		args[5] = Config.getProperty("accumulo.table_name");//inputPassword
		  
		args[6] = Config.getProperty("reports.min_max_consumption.output_table.name");//outputInstanceName
		args[7] = Config.getProperty("reports.min_max_consumption.output_table.zooServers");//outputTableName
		args[8] = Config.getProperty("reports.min_max_consumption.output_table.user");//outputUserName
		args[9] = Config.getProperty("reports.min_max_consumption.output_table.password");//outputPassword
		   
		args[10] = Config.getProperty("accumulo.minMaxConsumption.dateRange.min");//(time) minTimestamp
		args[11] = Config.getProperty("accumulo.minMaxConsumption.dateRange.max");//(long) maxTimestamp
		args[12] = Config.getProperty("accumulo.minMaxConsumption.dataRange.onlyYear");//(boolean) onlyYear -> is false, when the compute inside the year
		args[13] = Config.getProperty("accumulo.minMaxConsumption.outPutTable.pastLimitation");//(time) pastLimitation
		 
		args[14] = Config.getProperty("accumulo.minMaxConsumption.run.residentialUnit");//(boolean) compute min max consumption for residentialunit
		args[15] = Config.getProperty("accumulo.minMaxConsumption.run.builiding");//(boolean) compute min max consumption for builiding

		GregorianCalendar now = new GregorianCalendar(); 
		long reportTimestamp = now.getTimeInMillis();

		String[] filterArgs = new String[14];
		for(int i = 0; (i < args.length) && (i < 13);i++)
			filterArgs[i] = args[i];
		filterArgs[13] = new Long(reportTimestamp).toString();
	
		//first report: merge residentialID,consumptionID and Consumption -> filter timestamp
		ConvertMinMaxTimeStampReport.runConvert(filterArgs);
		
		//second report: consumption for a residentialUnits
		String[] summeryArgs = new String[7];
		summeryArgs[0] = args[0];
		summeryArgs[1] = args[5];
		summeryArgs[2] = args[6];
		summeryArgs[3] = args[7];
		summeryArgs[4] = args[8];
		summeryArgs[5] = "";
		summeryArgs[6] = new Long(reportTimestamp).toString();
		
		if(args[13].equals("true"))
		{
			summeryArgs[5] = "6";
			ConsumptionGeneralizeReport.runYearConsumptionGeneralize(summeryArgs);
			
			summeryArgs[5] = "5";
			MinMaxReport.runMinMax(summeryArgs);
		}
			
		if(args[14].equals("true"))
		{
			ConsumptionGeneralizeToBuildingReport.runConsumptionGeneralize(summeryArgs);
			
			summeryArgs[5] = "4";
			MinMaxReport.runMinMax(summeryArgs);
		}
		
		Accumulo.getInstance().connect(args[1], args[2], args[3], args[4]);
		Accumulo.getInstance().addMutation(args[6], "MinMaxConsumption", "report", "version", Helpers.toByteArray(reportTimestamp));
		Accumulo.getInstance().flushBashWriter(args[6]);
		Accumulo.getInstance().disconnect();
	}
}