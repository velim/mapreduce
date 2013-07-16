package org.velim;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TwitterCount {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		// Interface Mapper<K1,V1,K2,V2>
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// map(K1 key, V1 value, OutputCollector<K2,V2> output, Reporter
		// reporter)
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			List<String> tags = TwitterParser.getHashTags(line);
			for (String tag : tags) {
				word.set(tag);
				output.collect(word, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		private int cnt;
		
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			cnt = Integer.parseInt(job.get("cnt"));
		}
		
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			System.out.println(key +" : "+ sum);
			if (sum >= cnt)
				output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(TwitterCount.class);
		conf.setJobName("wordcountW");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		//conf.setNumMapTasks(10);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.set("cnt", args[2]);

		JobClient.runJob(conf);

	}
}
