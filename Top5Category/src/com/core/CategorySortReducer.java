package com.core;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CategorySortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
	private ArrayList<Text> ar;
	private IntWritable valueOutput;
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		System.out.println("CategorySortReducer.reduce(): Starts");
		System.out.println("Key is: "+key);
		ar = new ArrayList<Text>();
		
		valueOutput = key;
		for (Text value : values) {
			System.out.println("Iterable Value is: "+value);
			ar.add(value);
		}
		
		for (Text temp : ar) {
			System.out.println("-----------Key is: "+temp);
			System.out.println("-----------Value is: "+valueOutput);
			context.write(temp, valueOutput);
		}
		
		
		
	}

}
