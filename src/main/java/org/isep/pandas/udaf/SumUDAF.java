package org.isep.pandas.udaf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.FloatWritable;
import org.isep.pandas.udf.ArraySum;

@SuppressWarnings("deprecation")
public class SumUDAF extends UDAF{
	
	public static class UDAFSumState {
	    private long mCount;
	    private float mSum;
	    private int mArray;
	    
	}
	
	public static class Sum implements UDAFEvaluator{
		
		UDAFSumState state;
		ArraySum innerSum = new ArraySum();
		
		public Sum() {
	      super();
	      state = new UDAFSumState();
	      init();
		}
		
		public void init() {
			// TODO Auto-generated method stub
			state.mArray = 0;
			state.mSum = 0;
			state.mCount = 0;
		}
		
		public boolean iterate(ArrayList<FloatWritable> a) {
	      if (a != null) {
	        state.mSum += this.innerSum.evaluate(a).get();
	        state.mCount += a.size();
	        state.mArray++;
	      }
	      return true;
	    }
		
		public UDAFSumState terminatePartial() {
	      return state.mCount == 0 ? null : state;
	    }
		
		public boolean merge(UDAFSumState uss) {
	      if (uss != null) {
	        state.mSum += uss.mSum;
	        state.mCount += uss.mCount;
	        state.mArray += uss.mArray;
	      }
	      return true;
	    }
		
		public FloatWritable terminate() {
		      return new FloatWritable(state.mSum);
	    }
		
	  }

	  private SumUDAF() {
	    // prevent instantiation
	  }
		
	
}
