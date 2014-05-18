package org.sensoriclife.util;

public class MockInstanceConfiguration {
	
	private String mockInstanceName;
	private String inputTableName;
	private String outputTableName;
	private String userName;
	private String password;
	private static MockInstanceConfiguration config;
	
	private MockInstanceConfiguration() {
	}
	
	public static MockInstanceConfiguration getInstance(){
		if(config == null){
			config = new MockInstanceConfiguration();
		}
		return config;
	}
	
	public String getMockInstanceName() {
		return mockInstanceName;
	}
	
	public void setMockInstanceName(String mockInstanceName) {
		this.mockInstanceName = mockInstanceName;
	}
	
	public String getInputTableName() {
		return inputTableName;
	}
	
	public void setInputTableName(String inputTableName) {
		this.inputTableName = inputTableName;
	}
	
	public String getOutputTableName() {
		return outputTableName;
	}
	
	public void setOutputTableName(String outputTableName) {
		this.outputTableName = outputTableName;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public String getUserName() {
		return userName;
	}
	
	public void setUserName(String userName) {
		this.userName = userName;
	}
	
	public String[] getConfigAsStringArray(){
		String[] configArray = new String[5];
		configArray[0] = getInputTableName();
		configArray[1] = getPassword();
		configArray[2] = getUserName();
		configArray[3] = getMockInstanceName();
		configArray[4] = getOutputTableName();
		return configArray;
	}
}
