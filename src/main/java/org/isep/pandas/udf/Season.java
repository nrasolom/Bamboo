package org.isep.pandas.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;

public class Season extends UDF{

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
		int season = (dtt.getMonthOfYear() == 12) ? 0 : dtt.getMonthOfYear();
		season += 1;
		if (season <= 3 ){
			return new Text("WINTER");
		} else if (season <= 6){ 
			return new Text("SPRING");
		} else if (season <= 9){
			return new Text("SUMMER");
		} else if (season <= 12){
			return new Text("AUTUMN");
		} else {
			return new Text("NOT_FOUND");
		}
		
	}
}
