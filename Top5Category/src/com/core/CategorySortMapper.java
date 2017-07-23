package com.core;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CategorySortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("Input Line is: "+value);
		
		String line = value.toString();
		String[] arr = line.split("\t");
		
		Integer temp = Integer.parseInt(arr[1]);
		String category = arr[0];
		
		System.out.println("Key from Map is: "+temp);
		System.out.println("Value from Map is: "+category);
		
		context.write(new IntWritable(temp), new Text(category));
	}

}
