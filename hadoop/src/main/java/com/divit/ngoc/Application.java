package com.divit.ngoc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.divit.ngoc.mapper.TopFiveCategoriesMapper;
import com.divit.ngoc.reducer.TopFiveCategoriesReducer;

public class Application {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "categories");
		job.setJarByClass(Application.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(TopFiveCategoriesMapper.class);
		job.setReducerClass(TopFiveCategoriesReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		Path out = new Path(args[1]);
		out.getFileSystem(conf).deleteOnExit(out);
		job.waitForCompletion(true);

		if (job.isSuccessful()) {
			System.out.println("Job was successful");
		} else {
			System.out.println("Job was not successful");
		}

	}
}
