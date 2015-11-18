package org.isep.pandas.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.FloatWritable;


public class ArraySum extends UDF {
	
	/**
	 * Take a column name with a Hive Array(float) type and <br/>
	 * return the sum of the column for the current row.
	 * @param col
	 * @return
	 */
	public FloatWritable evaluate(ArrayList<FloatWritable> col){
		float res = 0;
		for (FloatWritable c : col){ res += c.get(); }
		return new FloatWritable(res);
	}

}
