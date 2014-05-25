package org.sensoriclife.reports;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


public class datetest {
	public static void main(String[] args) {
		long timestamp = System.currentTimeMillis();
		System.out.println(timestamp);
		//long timestamp = 1401273184964;
		Timestamp ts = new Timestamp(timestamp);
		GregorianCalendar cal = (GregorianCalendar) Calendar.getInstance();
		cal.setTime(ts);

		//Date date = calendar.getTime();
		//calendar.setTime(date);
		String outputDate = String.valueOf(cal.get(Calendar.DAY_OF_MONTH)) + "." + String.valueOf(cal.get(Calendar.MONTH) + 1 + ".");
		
		System.out.println(outputDate);
	}
}
