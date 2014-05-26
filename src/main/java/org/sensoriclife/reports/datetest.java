package org.sensoriclife.reports;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


public class datetest {
	public static void main(String[] args) {
		//minTs
		 Calendar cal = Calendar.getInstance();
		 cal.set(cal.get(Calendar.YEAR) - 1, 0, 1, 0, 0, 0);
		 cal.getTimeInMillis();
		 //maxTs
		 cal.set(cal.get(Calendar.YEAR), 11, 31, 23, 59, 59);
		 cal.getTimeInMillis();

		 
	}
}
