package org.isep.pandas.udf;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class FileNameTest {

	public FileName udf = new FileName();
	@Test
	public void test() {
		//fail("Not yet implemented");
	}

	@Test
	public void fileTest(){
		final Text inputFileName = new Text("hdfs://ns3099426.ip-91-121-196.eu:8020"
				+ "/apps/hive/warehouse/project.db/enernoc/474.csv");
		Text result = this.udf.evaluate(inputFileName);
		Assert.assertEquals("474", result.toString());
	}
}
