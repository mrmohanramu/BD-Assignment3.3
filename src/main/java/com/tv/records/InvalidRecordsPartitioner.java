package com.tv.records;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvalidRecordsPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text company, Text product, int arg2) {
		if (company.equals("Samsung")){
			return 0;
		}else{
			return 1;
		}
		
	}
}