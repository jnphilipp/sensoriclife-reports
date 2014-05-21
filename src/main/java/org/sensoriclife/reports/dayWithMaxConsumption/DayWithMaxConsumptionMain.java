package org.sensoriclife.reports.dayWithMaxConsumption;

import org.apache.accumulo.core.client.mock.MockInstance;
import org.sensoriclife.reports.dayWithMaxConsumption.firstJob.DaysWithConsumptionReport;
import org.sensoriclife.reports.dayWithMaxConsumption.secondJob.DaysWithMaxConsumptionReport;

public class DayWithMaxConsumptionMain {
	public static void main(String[] args) throws Exception {
		MockInstance mockInstance = DaysWithConsumptionReport.runFirstJob();
		DaysWithMaxConsumptionReport.runSecondJob(mockInstance);
	}
}
