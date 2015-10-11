package org.isep.pandas.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class FileName extends UDF{
	public Text evaluate(Text input){
		if (input == null) return new Text("");
		final String path = input.toString();
		final int index = path.lastIndexOf("/");
		final int offset = path.lastIndexOf(".");
		return new Text(path.substring(index, offset));
	}
}
