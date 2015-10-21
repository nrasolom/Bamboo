package org.isep.pandas.udf;

import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class DayTest {

	public Day d = new Day();
	
	@Test
	public void test() {
		//fail("Not yet implemented");
	}
	
	@Test
	public void regular(){
		assertEquals(new Text("2015_day-293"),d.evaluate(new Text("2015-10-20 17:21:00")));
	}

	@Test
	public void firstDay(){
		assertEquals(new Text("2014_day-1"),d.evaluate(new Text("2014-01-01 00:00:00")));
	}
	
	@Test
	public void lastDay(){
		assertEquals(new Text("2015_day-365"),d.evaluate(new Text("2015-12-31 23:59:59")));
	}

	@Test
	public void bissextile(){
		assertEquals(new Text("2016_day-60"),d.evaluate(new Text("2016-02-29 23:59:59")));
	}
	
	@Test
	public void nonBissextile(){
		assertNotEquals(new Text("2015_day-60"),d.evaluate(new Text("2015-02-29 23:59:59")));
	}
}
