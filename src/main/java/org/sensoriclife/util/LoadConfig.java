package org.sensoriclife.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LoadConfig {
	
	private Properties prop;
	private InputStream input;
	private String filePath;
	
	public LoadConfig(String filePath) {
		super();
		prop = new Properties();
		input = null;
		this.filePath = filePath;
		
		load();
	}
	
	private void load()
	{
		try {
			 
			input = new FileInputStream(this.filePath);
	 
			// load a properties file
			prop.load(input);
	  
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public String getProerty(String key)
	{
		return prop.getProperty(key);
	}
}

