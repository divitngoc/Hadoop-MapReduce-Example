package com.divit.ngoc.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopFiveCategoriesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text category = new Text();
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String str[] = line.split("\t");
		if (str.length > 5) {
			category.set(str[3]);
		}
		context.write(category, one);
	}
}
