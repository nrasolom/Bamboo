package org.isep.pandas.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.FloatWritable;

@SuppressWarnings("deprecation")
public class RatioUDAF extends UDAF{
	
	public static class UDAFRatioState {
	    private long mCount;
	    private float mRatio;
	}
	
	public static class Ratio implements UDAFEvaluator{

		UDAFRatioState state;
		public Ratio(){
			super();
			state = new UDAFRatioState();
			init();
		}
		
		public void init() {
			// TODO Auto-generated method stub
			this.state.mCount = 0;
			this.state.mRatio = 0;
		}
		
		public void iterate(FloatWritable value, FloatWritable area){
			if (value != null) {
				this.state.mCount ++;
				this.state.mRatio += (value.get() / area.get());
			}	
		}
		
		public UDAFRatioState terminatePartial() {
			return state.mCount == 0 ? null : state;
		}
		
		public boolean merge(UDAFRatioState uss) {
	      if (uss != null) {
	        state.mRatio += uss.mRatio;
	        state.mCount += uss.mCount;
	      }
	      return true;
	    }
		
		public FloatWritable terminate() {
		      return new FloatWritable(state.mRatio);
	    }
		
	}
}
