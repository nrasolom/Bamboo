package org.isep.pandas.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class Day extends UDF {

	public Text evaluate(Text dateTime){
		DateTime dtt = Bamboo.dateParse(dateTime.toString());
		if (dtt == null ) return new Text("NULL");
		//String d = dtt.getYear() + "_day-" + dtt.getDayOfYear();
		String d = dtt.toString("yyyy-MM-dd");
		return new Text(d);
	}
}
