package org.isep.pandas.udf;

import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.FloatWritable;

public class ArrayAvg extends UDF{

	/**
	 * Take Column name with a Hive Array(Float) Type <br/>
	 * and compute the average of the column for the current row.
	 * @param col
	 * @return
	 */
	public FloatWritable evaluate(ArrayList<FloatWritable> col){
		float res = 0; float acc = 0;
		for (FloatWritable c : col){ acc += c.get(); }
		res = acc / col.size();
		return new FloatWritable(res);
	}
	
}
