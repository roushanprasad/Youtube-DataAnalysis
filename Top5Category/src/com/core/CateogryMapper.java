package com.core;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CateogryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text category = new Text();
	private final static IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException{
		
		String line = value.toString();
		String[] cols = line.split("\t");
		
		if(cols.length > 4){
			category.set(cols[3]);
		}
		context.write(category, one);
	}
}
