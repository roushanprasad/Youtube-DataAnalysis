package com.core;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String line = value.toString();
		String[] arr = line.split("\t");
		
		if(arr.length > 6){
		Float temp = Float.parseFloat(arr[6]); 
		
		Text keyOutput = new Text(arr[0]);
		System.out.println("-----Key From Map: "+keyOutput);
		System.out.println("-----Value from MAp: "+temp);
		
		context.write(keyOutput, new FloatWritable(temp));
		}
		
	}
}
