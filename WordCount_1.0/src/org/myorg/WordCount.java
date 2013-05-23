package org.myorg; 

import java.io.IOException; 
import java.util.*; 

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
 
	
	public class WordCount { 
	
	     public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  		 //Interface Mapper<K1,V1,K2,V2>	    	 
	      private final static IntWritable one = new IntWritable(1); 
	      private Text word = new Text(); 
	      
	      // map(K1 key, V1 value, OutputCollector<K2,V2> output, Reporter reporter)
	      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
	        String line = value.toString(); 
	        StringTokenizer tokenizer = new StringTokenizer(line); 
	        while (tokenizer.hasMoreTokens()) {
	          word.set(tokenizer.nextToken());
	          System.out.println(word);
	          output.collect(word, one); 
	        } 
	      } 
	    } 
	
	     public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> { 
	       public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
	        int sum = 0; 
	        while (values.hasNext()) {
	          sum += values.next().get();
	          System.out.println(key + "/" +sum);
	        } 
	        output.collect(key, new IntWritable(sum)); 
	      } 
	    } 
	
	     public static void main(String[] args) throws Exception { 
	      JobConf conf = new JobConf(WordCount.class); 
	      conf.setJobName("wordcountW"); 
	
	      conf.setOutputKeyClass(Text.class); 
	      conf.setOutputValueClass(IntWritable.class); 
	
	      conf.setMapperClass(Map.class); 
	      conf.setCombinerClass(Reduce.class); 
	      conf.setReducerClass(Reduce.class); 
	
	      conf.setInputFormat(TextInputFormat.class); 
	      conf.setOutputFormat(TextOutputFormat.class); 
	
	      FileInputFormat.setInputPaths(conf, new Path(args[0])); 
	      FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
	
	      JobClient.runJob(conf); 
	    } 
	} 
