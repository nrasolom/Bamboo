package org.isep.pandas.udf;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

public class Bamboo {
	
	private static final String FORMAT = "yyyy-MM-dd HH:mm:ss";
	
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
