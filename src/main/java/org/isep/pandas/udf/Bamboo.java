package org.isep.pandas.udf;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

public class Bamboo {
	
	private static final String FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	/**
	 * Util function to test if a date format is good or not
	 * It set the to a time/zone without summer/winter time
	 * Returns null if the format is wrong
	 * @param dtt
	 * @return
	 */
	public static final DateTime dateParse(String dtt){
		DateTimeZone.setDefault(DateTimeZone.forID("Etc/GMT-4"));
		DateTime datetime;
		try{
			datetime = DateTime.parse(dtt, DateTimeFormat.forPattern(FORMAT));
		} catch(IllegalArgumentException e){
			datetime = null;
		}
		return datetime;
		
	}
}
