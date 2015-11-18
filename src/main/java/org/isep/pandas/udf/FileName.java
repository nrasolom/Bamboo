package org.isep.pandas.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class FileName extends UDF{
	
	/**
	 * Simple UDF function to get the file name of HIVE row value. <br/>
	 * It takes the INPUT__FILE__NAME and return the filename without the hdfs address and without the extension. <br/>
	 * Ex: it turns hdfs://ns1234567.ip-127-0-0-1.eu:8020/apps/hive/warehouse/panda_db.db/all_records/281.csv <br/> into 281. 
	 * Usefull for the join between all_records and all_sites to get the industry of each sites. <br/>
	 * @param input - INPUT__FILE__NAME from an HIVE query.
	 * @return filename - filename without extension (.csv, .txt, .tsv)
	 */
	public Text evaluate(Text input){
		if (input == null) return new Text("");
		final String path = input.toString();
		final int index = path.lastIndexOf("/");
		final int offset = path.lastIndexOf(".");
		return new Text(path.substring(index + 1, offset));
	}
}
