package org.sensoriclife.reports.dayWithMaxConsumption;

import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.dayWithMaxConsumption.firstJob.DaysWithConsumptionReport;
import org.sensoriclife.reports.dayWithMaxConsumption.secondJob.DaysWithMaxConsumptionReport;

public class DayWithMaxConsumptionMain {
	public static void main1(String[] args) throws Exception {
		Accumulo accumulo = DaysWithConsumptionReport.runFirstJob();
		DaysWithMaxConsumptionReport.runSecondJob(accumulo);
	}
}
