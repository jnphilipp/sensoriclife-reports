package org.sensoriclife.reports.unusualRiseOfConsumption;

import org.sensoriclife.db.Accumulo;
import org.sensoriclife.reports.unusualRiseOfConsumption.firstJob.UnusualRiseOfConsumptionReport;
import org.sensoriclife.reports.unusualRiseOfConsumption.secondob.UnusualRiseOfHeatingConsumptionReport;

public class UnusualRiseOfConsumptionMain {
	public static void main(String[] args) throws Exception {
		Accumulo accumulo = UnusualRiseOfConsumptionReport.runFirstJob();
		UnusualRiseOfHeatingConsumptionReport.runSecondJob(accumulo);
	}
}
