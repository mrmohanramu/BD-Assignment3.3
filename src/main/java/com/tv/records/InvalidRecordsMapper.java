package com.tv.records;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvalidRecordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	IntWritable unitsSoldForOnida;
	@Override
	public void setup(Context context) {
		unitsSoldForOnida = new IntWritable();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		if (!lineArray[0].equals("NA") && !lineArray[1].equals("NA")) {
			context.write(new Text(lineArray[0]), new IntWritable(1));
		}
		if(lineArray[0].equals("Onida") && !lineArray[0].equals("NA")){
			context.write(new Text(lineArray[3]), new IntWritable(1));
		}
	}
}
