package org.sensoriclife.reports.dayWithMaxConsumption;

import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.dayWithMaxConsumption.firstJob.DaysWithConsumptionReport;
import org.sensoriclife.reports.dayWithMaxConsumption.secondJob.DayWithMaxConsumptionReport;

public class DayWithMaxConsumptionMain {
	public static void main(String[] args) throws Exception {
		Accumulo accumulo = DaysWithConsumptionReport.runFirstJob();
		DayWithMaxConsumptionReport.runSecondJob(accumulo);
	}
}
