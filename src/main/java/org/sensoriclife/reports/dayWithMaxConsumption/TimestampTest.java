package org.sensoriclife.reports.dayWithMaxConsumption;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class TimestampTest {
	public static void main(String[] args) {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		GregorianCalendar cal = (GregorianCalendar) Calendar.getInstance();
		cal.setTime(timestamp);
		System.out.println(cal.get(Calendar.DAY_OF_YEAR));
	}
}
