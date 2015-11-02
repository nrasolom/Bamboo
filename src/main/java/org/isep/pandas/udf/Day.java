package org.isep.pandas.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;

public class Day extends UDF {

	/**
	 * Take Text pam with Datetime format and
	 * return Text with a Date Format.
	 * @param dateTime
	 * @return
	 */
	public Text evaluate(Text dateTime){
		DateTime dtt = Bamboo.dateParse(dateTime.toString());
		if (dtt == null ) return new Text("NULL");
		return new Text(dtt.toString("yyyy-MM-dd"));
	}
}
