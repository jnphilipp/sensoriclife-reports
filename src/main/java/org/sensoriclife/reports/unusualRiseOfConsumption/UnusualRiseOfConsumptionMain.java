package org.sensoriclife.reports.unusualRiseOfConsumption;

import org.apache.accumulo.core.client.mock.MockInstance;
import org.sensoriclife.reports.unusualRiseOfConsumption.firstJob.UnusualRiseOfConsumptionReport;
import org.sensoriclife.reports.unusualRiseOfConsumption.secondob.UnusualRiseOfHeatingConsumptionReport;

public class UnusualRiseOfConsumptionMain {
	public static void main(String[] args) throws Exception {
		MockInstance mockInstance = UnusualRiseOfConsumptionReport.runFirstJob();
		UnusualRiseOfHeatingConsumptionReport.runSecondJob(mockInstance);
	}
}
