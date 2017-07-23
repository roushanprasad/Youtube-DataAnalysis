package com.core;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingsReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
		
		int i=0;
		float sum = 0.0f;
		for (FloatWritable value : values) {
			sum += value.get();
			i++;
		}
		sum = sum/i;
		context.write(key, new FloatWritable(sum));
	}
	
	

}
