package org.isep.pandas.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;


public class Season extends UDF{

	private final int START_SPRING = new DateTime(2000,  3, 20, 0,0,0).getDayOfYear();
	private final int START_SUMMER = new DateTime(2000,  6, 21, 0,0,0).getDayOfYear();
	private final int START_AUTUMN = new DateTime(2000,  9, 23, 0,0,0).getDayOfYear();
	private final int START_WINTER = new DateTime(2000, 12, 21, 0,0,0).getDayOfYear();
	
	/**
	 * Take Text param with a date time format and return a season <br/>
	 * WINTER: December January February <br/>
	 * SPRING: March April May <br/>
	 * SUMMER: Jun July August <br/>
	 * AUTUMN: September October November<br/>
	 * In case of bad format it returns "NOT_FOUND".
	 * @param dateTime
	 * @return
	 */
	public Text evaluate(Text dateTime){
		DateTime dtt = Bamboo.dateParse(dateTime.toString());
		if (dtt == null ) return new Text("NOT_FOUND");
		//int season = (dtt.getMonthOfYear() == 12) ? 0 : dtt.getMonthOfYear();
		int day = dtt.getDayOfYear();

		if (day > START_WINTER){
			return new Text("WINTER");
		} else if (day >= START_AUTUMN){ 
			return new Text("AUTUMN");
		} else if (day >= START_SUMMER){
			return new Text("SUMMER");
		} else if (day >= START_SPRING){
			return new Text("SPRING");
		}else if (day < START_SPRING){
			return new Text("WINTER");
		} else {
			return new Text("NOT_FOUND");
		}
		
	}
}
