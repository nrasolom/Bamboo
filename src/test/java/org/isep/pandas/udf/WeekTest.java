package org.isep.pandas.udf;

import static org.junit.Assert.*;
import org.joda.time.DateTime;
import org.junit.Test;
import org.apache.hadoop.io.Text;

public class WeekTest {
	
	public Week w = new Week();

	@Test
	public void regularTest(){
		// regular case -> 
		Text weekno = w.evaluate(new Text("2012-01-04 17:30:12"));
		assertEquals("2012 / week:1", weekno.toString());
	}
	
	@Test
	public void multiYearTest(){
		// limit case -> GROUP BY works fine for different years
		Text weekno = w.evaluate(new Text("2015-02-07 12:51:12"));
		assertEquals("2015 / week:6", weekno.toString());
	}
	
	@Test
	public void badFormatTest(){
		// limit case -> bad date format
		Text weekno1 = w.evaluate(new Text("26-09-2012/17-30-12"));
		assertEquals("NULL", weekno1.toString());
		
		Text weekno2 = w.evaluate(new Text("I'M BEST PANDA IN THE WORLD"));
		assertEquals("NULL", weekno2.toString());
	}
	
	@Test
	public void lastDayTest(){
		// limit case -> last day of the week 
		Text weekno = w.evaluate(new Text("2012-01-07 21:47:08"));
		assertEquals("2012 / week:1", weekno.toString());
	}
	
	@Test
	public void veryVeryLast(){
		// limit case -> the very very last day of the year
		Text weekno = w.evaluate(new Text("2011-12-31 01:05:00"));
		assertEquals("2011 / week:52", weekno.toString());
	}
	
	@Test
	public void veryVeryFirst(){
		// limit case -> the very very first day of the year
		Text weekno1 = w.evaluate(new Text("2014-01-01 00:00:00"));
		assertEquals("2014 / week:1", weekno1.toString());
				
		// In the definition of the FIRST week it has to contain 
		// at least 4 days or it belongs to the last year and return 52
		Text weekno2 = w.evaluate(new Text("2012-01-01 00:05:00"));
		assertEquals("2011 / week:52", weekno2.toString());
		
		// In the definition of the LAST week it has to contain 
		// at least 4 days or it belongs to the next year and return 1 instead of 52
		Text weekno3 = w.evaluate(new Text("2014-12-31 07:35:00"));
		assertEquals("2015 / week:1", weekno3.toString());
	}
	
	@Test
	public void test() {
		//DateTime d = new DateTime(2012,01,01, 0, 0, 0);
		//System.out.println(d.toString()); //2012-01-01T00:00:00.000+01:00
		//System.out.println("Number of weeks: "+d.getWeekOfWeekyear()); // 52
		//System.out.println("Last week belongs to: " +d.getWeekyear()); // 2011
	}

}
