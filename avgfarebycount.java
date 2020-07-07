package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4b {

	final String gtid = "asong49";

	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>{

	IntWritable intkey = new IntWritable();
	Text textval = new Text();
	IntWritable one = new IntWritable(1);
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	String line = value.toString();
	String[] fields = line.split("\t");
	int passenger = Integer.parseInt(fields[2]);
	double v = Double.parseDouble(fields[3]);
	String h = String.valueOf(one) + "," + String.valueOf(v);
	intkey.set(passenger);
	textval.set(h);
	context.write(intkey, textval);

}
}

	public static class IntSumReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
	Text val = new Text();
	FloatWritable result = new FloatWritable(); 


	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	int total = 0;
	double sum = 0;
	double avg = 0;
	for (Text value : values) {
        String line = value.toString();
	String[] field = line.split(",");
	total = total + Integer.parseInt(field[0]);
	sum = sum + Double.parseDouble(field[1]);
        avg = sum/total;

}
	String z = String.format("%.2f", avg);
	val.set(z);
	context.write(key, val);
}
}

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q4b");
    job.setJarByClass(Q4b.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
