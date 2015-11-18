package org.isep.pandas.udf;

import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class DayTest {

	public Day d = new Day();
	
	@Test
	public void test() {
		//fail("Not yet implemented");
	}
	
	@Test
	public void regular(){
		assertEquals(new Text("2015-10-20"),d.evaluate(new Text("2015-10-20 17:21:00")));
	}
	
	@Test
	public void badFormat(){
		assertEquals(new Text("NULL"),d.evaluate(new Text("ALL THE PANNDAS IN THE CLUB??!!")));
	}

	@Test
	public void firstDay(){
		assertEquals(new Text("2014-01-01"),d.evaluate(new Text("2014-01-01 00:00:00")));
	}
	
	@Test
	public void lastDay(){
		assertEquals(new Text("2015-12-31"),d.evaluate(new Text("2015-12-31 23:59:59")));
	}
	
	@Test
	public void realyWeidCase(){
		// HiveQL queries reveal that time/zone metter
		// Europe/Paris pass from 2h to 3h on 12/03/25
		assertEquals(new Text("2012-03-25"),d.evaluate(new Text("2012-03-25 02:55:00")));
	}

}
