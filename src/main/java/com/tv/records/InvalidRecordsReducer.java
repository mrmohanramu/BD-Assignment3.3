package com.tv.records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvalidRecordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable unitsSold;
	IntWritable unitsSoldForOnida;

	@Override
	public void setup(Context context) {
		unitsSold = new IntWritable();
		unitsSoldForOnida = new IntWritable();
	}
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int i = 0;
		String str ="Units sold by Onida in ";
		for(IntWritable value : values){
			i = i + value.get();
		}
		if((key.toString()).equals("Uttar Pradesh")){
			unitsSoldForOnida = new IntWritable(i);
			str = str+key.toString();
			System.out.println(str);
			context.write(new Text(str), unitsSoldForOnida);	
		}else{
			unitsSold = new IntWritable(i);
			context.write(key, unitsSold);
		}
		
	}
}
