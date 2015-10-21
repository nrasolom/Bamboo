package org.isep.pandas.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class Day extends UDF {
	private final String FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	public Text evaluate(Text dateTime){
		DateTime dtt;
		try{
			dtt = DateTime.parse(
					dateTime.toString(), 
					DateTimeFormat.forPattern(this.FORMAT)
				);
		}catch(IllegalArgumentException e){
			return new Text("NULL");
		}
		
		String d = dtt.getYear() + 
				 "_day-" + 
				 dtt.getDayOfYear();
		
		return new Text(d);
	}
}
