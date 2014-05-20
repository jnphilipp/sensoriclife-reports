package org.sensoriclife.util;

public class MinMaxConfiguration {
	
	public MinMaxConfiguration() {
		super();
		
	}

	private String reportName;
	
	private String inputInstanceName;
	private String inputTableName;
	private String inputUserName;
	private String inputPassword;
	
	private String outputInstanceName;
	private String outputTableName;
	private String outputUserName;
	private String outputPassword;
			
	private String allTime; 
	private String minTimestamp;
	private String maxTimestamp;
	
	private static MinMaxConfiguration config;
	
	
	public static MinMaxConfiguration getInstance(){
		if(config == null){
			config = new MinMaxConfiguration();
		}
		return config;
	}
	
	public String getReportName() {
		return reportName;
	}
	
	public void setReportName(String reportName) {
		this.reportName = reportName;
	}

	public String getInputInstanceName() {
		return inputInstanceName;
	}

	public void setInputInstanceName(String inputInstanceName) {
		this.inputInstanceName = inputInstanceName;
	}

	public String getInputTableName() {
		return inputTableName;
	}

	public void setInputTableName(String inputTableName) {
		this.inputTableName = inputTableName;
	}

	public String getInputUserName() {
		return inputUserName;
	}

	public void setInputUserName(String inputUserName) {
		this.inputUserName = inputUserName;
	}

	public String getInputPassword() {
		return inputPassword;
	}

	public void setInputPassword(String inputPassword) {
		this.inputPassword = inputPassword;
	}

	public String getOutputInstanceName() {
		return outputInstanceName;
	}

	public void setOutputInstanceName(String outputInstanceName) {
		this.outputInstanceName = outputInstanceName;
	}

	public String getOutputTableName() {
		return outputTableName;
	}

	public void setOutputTableName(String outputTableName) {
		this.outputTableName = outputTableName;
	}

	public String getOutputUserName() {
		return outputUserName;
	}

	public void setOutputUserName(String outputUserName) {
		this.outputUserName = outputUserName;
	}

	public String getOutputPassword() {
		return outputPassword;
	}

	public void setOutputPassword(String outputPassword) {
		this.outputPassword = outputPassword;
	}

	public String getAllTime() {
		return allTime;
	}

	public void setAllTime(String allTime) {
		this.allTime = allTime;
	}

	public String getMinTimestamp() {
		return minTimestamp;
	}

	public void setMinTimestamp(String minTimestamp) {
		this.minTimestamp = minTimestamp;
	}

	public String getMaxTimestamp() {
		return maxTimestamp;
	}

	public void setMaxTimestamp(String maxTimestamp) {
		this.maxTimestamp = maxTimestamp;
	}
	
	public String[] getConfigAsStringArray(){
		String[] configArray = new String[5];
		configArray[0] = getReportName();
		configArray[1] = getInputInstanceName();
		configArray[2] = getInputTableName();
		configArray[3] = getInputUserName();
		configArray[4] = getInputPassword();

		configArray[5] = getOutputInstanceName();
		configArray[6] = getOutputTableName();
		configArray[7] = getOutputUserName();
		configArray[8] = getOutputPassword();
		
		configArray[9] = getAllTime();
		configArray[10] = getMinTimestamp();
		configArray[11] = getMaxTimestamp();
		return configArray;
	}
}
