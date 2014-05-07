package org.sensoriclife.util;
import com.google.gson.Gson;
import com.google.gson.JsonObject;


public class ConvertJsonValue {

	private Gson gson;
	JsonObject json;
	
	
	public ConvertJsonValue(String jsonValue) {
		super();
		this.gson = new Gson();
		json = gson.fromJson(jsonValue, JsonObject.class);
	}
	
	public int getInt(String key)
	{
		return json.get(key).getAsInt();
	}
	
	public String getString(String key)
	{
		return json.get(key).getAsString();
	}
}
