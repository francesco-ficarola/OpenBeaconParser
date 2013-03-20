package it.uniroma1.dis.wsngroup.core;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTime {
	
	/**
	 * @author Francesco Ficarola
	 *
	 */
	
	public DateTime() {	}
	
	public String[] getDateTime() {
		String [] dateTime = new String[2];
		Calendar cal = Calendar.getInstance();
		Date date = cal.getTime();

		// Doc.: http://download.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html
		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat timeFormatter = new SimpleDateFormat("HH.mm.ss");
		
		dateTime[0] = dateFormatter.format(date);
		dateTime[1] = timeFormatter.format(date);
		
		return dateTime;
	}
}
