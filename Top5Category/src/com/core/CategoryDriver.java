package com.core;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CategoryDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CategoryDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(CategoryDriver.class);
		job.setJobName("Top 5 Category on Youtube");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(CateogryMapper.class);
		job.setReducerClass(CategoryReducer.class);
		
		job.waitForCompletion(true);
		
		//----------- Job2 Config------------------
		Job job2 = Job.getInstance(getConf());
		job2.setJarByClass(CategoryDriver.class);
		job2.setJobName("Sort - Top 5 Category on Youtube");
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		Path inputPath2 = new Path(args[1]);
		Path outputPath2 = new Path(args[2]);
		
		FileInputFormat.addInputPath(job2, inputPath2);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		
		job2.setMapperClass(CategorySortMapper.class);
		job2.setReducerClass(CategorySortReducer.class);
		
		return job2.waitForCompletion(true)?0:1;
		
	}

}
