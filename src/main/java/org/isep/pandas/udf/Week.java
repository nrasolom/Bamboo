package org.isep.pandas.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;


public class Week extends UDF{
	
	private final String FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	public Text evaluate(Text dtt){
		DateTime datetime;
		try{
			datetime = DateTime.parse(
					dtt.toString(), 
					DateTimeFormat.forPattern(this.FORMAT)
			);
		} catch(IllegalArgumentException e){
			return new Text("NULL");
		}
		
		datetime.getWeekOfWeekyear();
		String w = datetime.getWeekyear() + 
				 "_week-" + 
				 datetime.getWeekOfWeekyear();
		
		return new Text(w);
		
	}
}
