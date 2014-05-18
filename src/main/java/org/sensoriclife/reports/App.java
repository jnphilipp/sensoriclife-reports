package org.sensoriclife.reports;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.sensoriclife.Logger;
import org.sensoriclife.reports.dayWithMaxConsumption.firstJob.DaysWithConsumptionReport;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp, marcel
 * @version 0.0.1
 */
public class App {
	
	/**
	 * default database configuration file
	 */
	private static final String DEFAULT_CONFIGURATION_FILE = Helpers.getUserDir() + "/config/config.properties";
	
	/**
	 * properties
	 */
	private static Properties properties;
	
	public static void main(String[] args) {
		Logger.getInstance();
		Logger.info("SensoricLife - reports");
		String configFile = App.DEFAULT_CONFIGURATION_FILE;
		App.loadConfig(configFile);
		
		
		
		
		
		
		int res = 0;
		try {
			res = ToolRunner.run(CachedConfiguration.getInstance(),
					new DaysWithConsumptionReport(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.exit(res);

	}
	
	/**
	 * @param key key
	 * @return values of the given key
	 */
	public static String getProperty(String key) {
		switch ( key ) {
			case "realtime":
				return App.properties.getProperty(key, "true");
			default:
				return App.properties.getProperty(key, "");
		}
	}

	public static void loadConfig(String configFile) {
		if ( !new File(configFile).exists() ) {
			System.err.println("The configuration file does not exists.");
			Logger.error(App.class, "The configuration file does not exists.");
			System.exit(1);
		}

		try {
			App.properties = new Properties();
			App.properties.load(new FileInputStream(configFile));
		}
		catch ( IOException e ) {
			Logger.error(App.class, "Error while reading config file.", e.toString());
			System.exit(1);
		}
	}
}