package org.isep.pandas.udf;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.junit.Before;
import org.junit.Test;

public class ArrayAggTest {

	public double[] measures = {58.0094,0.8476,17.8057,0.0,29.1381,2.1326,24.4977,104.8736,13.9492,23.7422,68.1132,57.6915,21.7602,60.9895,17.093,20.9558,253.3193,11.2068,8.4734,274.8273,2.8236,17.9272,2.1106,28.3085,7.3269,3.4221,3.2456,18.1388,9.2559,4.3118,5.9179,3.9051,2.6304,6.3633,4.7271,136.7605,9.2303,10.4499,4.4388,1.8709,5.2445,5.1538,25.4666,11.8662,4.7311,27.0053,18.1351,12.2727,13.3675,10.6253,7.3969,16.9444,29.1731,15.0239,28.8834,24.8405,14.9718,7.4548,5.2848,35.636,2.193,15.7658,21.2946,15.4872};
	public ArrayList<FloatWritable> column1 = new ArrayList<FloatWritable>(); 
	public ArrayList<FloatWritable> column2 = new ArrayList<FloatWritable>();
	public ArrayList<FloatWritable> column3 = new ArrayList<FloatWritable>();
	public ArraySum sum = new ArraySum();
	public ArrayAvg avg = new ArrayAvg();
	
	@Before
	public void init(){
		for(double m : measures){
			this.column1.add(new FloatWritable((float)m));
		}
	}
	
	@Test
	public void test() {
		//fail("Not yet implemented");
	}
	
	@Test
	public void ArraySum(){
		assertEquals(1726.80960214, sum.evaluate(column1).get(), 0.1);
	}
	
	@Test
	public void ArrayAvg(){
		assertNotEquals(1726.80960214,avg.evaluate(column1).get(),0.1);
		assertEquals(26.981395, avg.evaluate(column1).get(), 0.1);
	}
	
	

}
