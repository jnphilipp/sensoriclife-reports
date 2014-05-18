package org.sensoriclife.reports;

import org.sensoriclife.Config;
import org.sensoriclife.Logger;

/**
 *
 * @author jnphilipp, marcel
 * @version 0.0.1
 */
public class App {
	public static void main(String[] args) {
		Logger.getInstance();
		Logger.info("SensoricLife - reports");
		Config.getInstance();

		/*int res = 0;
		try {
			res = ToolRunner.run(CachedConfiguration.getInstance(),
					new MinMaxReport(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.exit(res);*/
	}
}