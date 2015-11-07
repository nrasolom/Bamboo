package org.isep.pandas.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;


public class Week extends UDF{
	
	/**
	 * Take a datetime format like: 2011-12-31 01:05:00 <br/>
	 * Return {YEAR}_week-{week number} <br/>
	 * Usage: SELECT getweek(dttm_utc) as week, SUM(value) FROM ... GROUP BY getweek(dttm_utc);
	 * @param dateTime
	 * @return
	 */
	public Text evaluate(Text dateTime){
		DateTime dtt = Bamboo.dateParse(dateTime.toString());
		if (dtt == null ) return new Text("NULL");
		String w = dtt.getWeekyear() + " / week:" + dtt.getWeekOfWeekyear();
		return new Text(w);
	}
}
